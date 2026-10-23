"""Materialized views over current document state."""
import sys

import dlt

sys.path.insert(0, spark.conf.get("bundle.sourcePath"))  # noqa: F821

from transformations.business import daily_completions, student_progress


@dlt.table(name="gold_daily_completions")
@dlt.expect_or_fail("school_timezone_found", "school_tz IS NOT NULL")
def gold_daily_completions():
    return daily_completions(dlt.read("silver_student_events"), dlt.read("silver_school_dim"))


@dlt.table(name="gold_student_progress")
def gold_student_progress():
    return student_progress(dlt.read("silver_student_events"))
