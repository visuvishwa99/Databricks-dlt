"""Materialized views over current document state."""
import sys

from pyspark import pipelines as dp

sys.path.insert(0, spark.conf.get("bundle.sourcePath"))  # noqa: F821

from transformations.business import daily_completions, student_progress


@dp.materialized_view(name="gold_daily_completions")
@dp.expect_or_fail("school_timezone_found", "school_tz IS NOT NULL")
def gold_daily_completions():
    return daily_completions(spark.read.table("silver_student_events"), spark.read.table("silver_school_dim"))  # noqa: F821


@dp.materialized_view(name="gold_student_progress")
def gold_student_progress():
    return student_progress(spark.read.table("silver_student_events"))  # noqa: F821
