"""Explicit schemas for newline-delimited MongoDB Extended JSON exports."""

from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

OID = StructType([StructField("$oid", StringType())])
DATE = StructType([StructField("$date", StringType())])

EVENT_SCHEMA = StructType(
    [
        StructField("_id", OID),
        StructField("schema_version", IntegerType()),
        StructField("product_name", StringType()),
        StructField("change_type", StringType()),
        StructField("student_id", StringType()),
        StructField("unit_id", StringType()),
        StructField("strand_id", StringType()),
        StructField("level_id", StringType()),
        StructField("activity_id", StringType()),
        StructField("session_id", StringType()),
        StructField("school_id", StringType()),
        StructField("grade_level", StringType()),
        StructField("is_completed", BooleanType()),
        StructField("attempt_timestamp", DATE),
        StructField("updated_at", DATE),
        StructField("_rescued_data", StringType()),
        StructField("_corrupt_record", StringType()),
    ]
)

MANIFEST_SCHEMA = StructType(
    [
        StructField("run_id", StringType()),
        StructField("started_at", TimestampType()),
        StructField("finished_at", TimestampType()),
        StructField("file_count", IntegerType()),
        StructField("record_count", LongType()),
        StructField("max_event_ts", TimestampType()),
        StructField("schema_version", IntegerType()),
        StructField("_rescued_data", StringType()),
    ]
)
