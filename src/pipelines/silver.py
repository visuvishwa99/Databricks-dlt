"""Quality gates, quarantine, SCD type 1 current state, and school dimension."""

import sys

from pyspark import pipelines as dp
from pyspark.sql import functions as F  # noqa: F821

sys.path.insert(0, spark.conf.get("bundle.sourcePath"))  # noqa: F821

from transformations.parse import quarantine_reasons, unwrap_extended_json
from transformations.rules import DROP_ALL, DROP_RULES, FAIL_RULES, WARN_RULES


def parsed():
    return unwrap_extended_json(spark.readStream.table("bronze_student_events"))  # noqa: F821


@dp.table(name="silver_student_events_clean")
@dp.expect_all_or_fail(FAIL_RULES)
@dp.expect_all_or_drop(DROP_RULES)
@dp.expect_all(WARN_RULES)
def silver_student_events_clean():
    # Unparseable JSON has no reliable document ID and belongs in quarantine.
    # Keep the fail expectation for parseable records with a missing ID.
    return parsed().where("_corrupt_record IS NULL")


@dp.table(name="silver_quarantine_events")
def silver_quarantine_events():
    return quarantine_reasons(parsed(), DROP_RULES).where(f"NOT coalesce(({DROP_ALL}), false)").withColumn(
        "quarantined_at", F.current_timestamp()
    )


dp.create_streaming_table("silver_student_events")
dp.create_auto_cdc_flow(
    target="silver_student_events",
    source="silver_student_events_clean",
    keys=["_id"],
    sequence_by=F.struct("updated_at", "exported_at", "source_file"),
    stored_as_scd_type=1,
)


@dp.materialized_view(name="silver_school_dim")
@dp.expect_or_fail("consistent_school_metadata", "metadata_versions = 1")
@dp.expect_or_fail("complete_school", "school_id IS NOT NULL AND school_tz IS NOT NULL")
def silver_school_dim():
    if spark.conf.get("school_table") == "__sample__":  # noqa: F821
        return spark.sql(  # noqa: F821
            "SELECT 'demo-school-1' AS school_id, 'Demo School' AS school_name, "
            "'demo-district-1' AS district_id, 'America/Phoenix' AS school_tz, "
            "1 AS metadata_versions"
        )
    return (
        spark.read.table(spark.conf.get("school_table"))  # noqa: F821
        .select("school_id", "school_name", "district_id", "school_tz")
        .groupBy("school_id")
        .agg(
            F.min("school_name").alias("school_name"),
            F.min("district_id").alias("district_id"),
            F.min("school_tz").alias("school_tz"),
            F.size(F.collect_set(F.struct("school_name", "district_id", "school_tz"))).alias(
                "metadata_versions"
            ),
        )
    )
