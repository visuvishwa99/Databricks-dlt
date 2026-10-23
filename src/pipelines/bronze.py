"""Auto Loader inputs. S3 prefix must be granted through Unity Catalog."""

import sys

import dlt
from pyspark.sql import functions as F

sys.path.insert(0, spark.conf.get("bundle.sourcePath"))  # noqa: F821

from transformations.parse import add_run_metadata
from transformations.rules import MANIFEST_RULES
from transformations.schema import EVENT_SCHEMA, MANIFEST_SCHEMA

ROOT = spark.conf.get("export_root").rstrip("/") + "/"  # noqa: F821
USE_FILE_EVENTS = spark.conf.get("use_managed_file_events", "true").lower() == "true"  # noqa: F821


def auto_loader(glob, schema):
    reader = (
        spark.readStream.format("cloudFiles")  # noqa: F821
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.allowOverwrites", "false")
        .option("cloudFiles.maxFilesPerTrigger", spark.conf.get("max_files_per_trigger", "1000"))  # noqa: F821
        .option("mode", "FAILFAST")
        .option("pathGlobFilter", glob)
        .schema(schema)
    )
    if USE_FILE_EVENTS:
        reader = reader.option("cloudFiles.useManagedFileEvents", "true")
    return reader.load(ROOT)


@dlt.table(
    name="bronze_student_events",
    table_properties={"pipelines.reset.allowed": "false"},
)
def bronze_student_events():
    return add_run_metadata(auto_loader("part-*.json", EVENT_SCHEMA)).withColumn(
        "run_id", F.col("path_run_id")
    )


@dlt.table(
    name="bronze_export_manifests",
    table_properties={"pipelines.reset.allowed": "false"},
)
@dlt.expect_all_or_fail(MANIFEST_RULES)
def bronze_export_manifests():
    return add_run_metadata(auto_loader("manifest.json", MANIFEST_SCHEMA))
