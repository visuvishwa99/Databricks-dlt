"""Business aggregates kept independent of pipeline decorators for testing."""

from pyspark.sql import functions as F


def daily_completions(events, schools):
    return (
        events.where("is_completed = true").join(schools, "school_id", "left")
        .withColumn("school_day", F.to_date(F.from_utc_timestamp("attempt_timestamp", F.col("school_tz"))))
        .groupBy("product_name", "school_id", "school_name", "school_tz",
                 "grade_level", "school_day", "change_type")
        .agg(F.count("*").alias("completions"), F.countDistinct("student_id").alias("students"))
    )


def student_progress(events):
    latest_order = F.struct("attempt_timestamp", "updated_at", "_id")
    return events.where("is_completed = true").groupBy("product_name", "student_id").agg(
        F.max_by("school_id", latest_order).alias("school_id"),
        F.max_by("unit_id", latest_order).alias("latest_unit_id"),
        F.max_by("level_id", latest_order).alias("latest_level_id"),
        F.max("attempt_timestamp").alias("last_completed_at"),
        F.count("*").alias("total_completions"),
    )
