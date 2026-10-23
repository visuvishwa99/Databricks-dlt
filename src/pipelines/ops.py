"""Run-level export completeness and freshness evidence."""

import sys

import dlt

sys.path.insert(0, spark.conf.get("bundle.sourcePath"))  # noqa: F821

from transformations.parse import cdc_conflicts, reconcile_exports


@dlt.table(name="ops_export_reconciliation")
def ops_export_reconciliation():
    return reconcile_exports(
        dlt.read("bronze_export_manifests"),
        dlt.read("bronze_student_events"),
    )


@dlt.table(name="ops_cdc_conflicts")
@dlt.expect_or_fail("unambiguous_source_revision", "payload_versions = 1")
def ops_cdc_conflicts():
    return cdc_conflicts(dlt.read("silver_student_events_clean"))
