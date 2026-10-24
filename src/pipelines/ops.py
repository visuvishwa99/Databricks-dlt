"""Run-level export completeness and freshness evidence."""

import sys

from pyspark import pipelines as dp

sys.path.insert(0, spark.conf.get("bundle.sourcePath"))  # noqa: F821

from transformations.parse import cdc_conflicts, reconcile_exports


@dp.materialized_view(name="ops_export_reconciliation")
def ops_export_reconciliation():
    return reconcile_exports(
        spark.read.table("bronze_export_manifests"),  # noqa: F821
        spark.read.table("bronze_student_events"),  # noqa: F821
    )


@dp.materialized_view(name="ops_cdc_conflicts")
@dp.expect_or_fail("unambiguous_source_revision", "payload_versions = 1")
def ops_cdc_conflicts():
    return cdc_conflicts(spark.read.table("silver_student_events_clean"))  # noqa: F821
