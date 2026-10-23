"""Pure DataFrame transformations used by Lakeflow and local tests."""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_run_metadata(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("source_file", F.col("_metadata.file_path"))
        .withColumn("exported_at", F.col("_metadata.file_modification_time"))
        .withColumn("path_run_id", F.regexp_extract("source_file", r"run_id=([^/]+)", 1))
        .withColumn("ingested_at", F.current_timestamp())
    )


def unwrap_extended_json(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("_id", F.col("_id.`$oid`"))
        .withColumn(
            "attempt_timestamp", F.expr("try_cast(attempt_timestamp.`$date` AS TIMESTAMP)")
        )
        .withColumn(
            "updated_at",
            F.when(F.col("updated_at.`$date`").isNull(), F.col("attempt_timestamp"))
            .otherwise(F.expr("try_cast(updated_at.`$date` AS TIMESTAMP)")),
        )
    )


def reconcile_exports(manifests: DataFrame, events: DataFrame) -> DataFrame:
    m = manifests.groupBy("run_id").agg(
        F.count("*").alias("manifest_rows"),
        F.max("finished_at").alias("finished_at"),
        F.max("file_count").alias("file_count"),
        F.max("record_count").alias("record_count"),
        F.max("path_run_id").alias("manifest_path_run_id"),
    )
    b = events.groupBy("path_run_id").agg(
        F.count("*").alias("loaded_records"),
        F.countDistinct("source_file").alias("loaded_files"),
        F.min("exported_at").alias("first_exported_at"),
    )
    return (
        m.join(b, m.run_id == b.path_run_id, "full_outer")
        .withColumn("run_id", F.coalesce(m.run_id, b.path_run_id))
        .drop(b.path_run_id)
        .withColumn(
            "status",
            F.when(F.col("manifest_rows").isNull(), "NO_MANIFEST")
            .when(F.col("manifest_rows") != 1, "DUPLICATE_MANIFEST")
            .when(F.col("run_id") != F.col("manifest_path_run_id"), "RUN_ID_MISMATCH")
            .when(
                F.col("file_count").isNull() | F.col("record_count").isNull()
                | (F.col("file_count") < 0) | (F.col("record_count") < 0),
                "INVALID_MANIFEST",
            )
            .when(
                (F.col("file_count") == 0) & (F.col("record_count") == 0)
                & F.col("loaded_records").isNull(), "OK",
            )
            .when(F.col("loaded_records").isNull(), "NOT_LOADED")
            .when(F.col("loaded_files") < F.col("file_count"), "MISSING_FILES")
            .when(F.col("loaded_files") > F.col("file_count"), "EXTRA_FILES")
            .when(F.col("loaded_records") < F.col("record_count"), "MISSING_RECORDS")
            .when(F.col("loaded_records") > F.col("record_count"), "EXTRA_RECORDS")
            .otherwise("OK"),
        )
        .withColumn("checked_at", F.current_timestamp())
    )


def quarantine_reasons(df: DataFrame, rules: dict) -> DataFrame:
    """Null predicates are failures, matching the clean-stream routing."""
    reasons = F.array(*[
        F.when(~F.coalesce(F.expr(rule), F.lit(False)), F.lit(name))
        for name, rule in rules.items()
    ])
    return df.withColumn("quality_errors", F.filter(reasons, lambda value: value.isNotNull()))


def cdc_conflicts(events: DataFrame) -> DataFrame:
    """The producer must supply one business payload per document/update time."""
    from transformations.schema import EVENT_SCHEMA

    business_columns = [field.name for field in EVENT_SCHEMA if not field.name.startswith("_rescued")]
    payload = F.to_json(F.struct(*[F.col(name) for name in business_columns]))
    return events.groupBy("_id", "updated_at").agg(
        F.countDistinct(F.sha2(payload, 256)).alias("payload_versions")
    ).where("payload_versions > 1")
