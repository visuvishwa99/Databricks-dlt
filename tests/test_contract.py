"""Fast checks that run without Databricks or a Spark installation."""

import json
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "sample_exports/run_date=2026-10-22/run_id=sample-001"


def test_sample_export_contract():
    manifest = json.loads((SAMPLE / "manifest.json").read_text())
    part_files = list(SAMPLE.glob("part-*.json"))
    rows = [json.loads(line) for path in part_files for line in path.read_text().splitlines()]
    assert manifest["run_id"] == SAMPLE.name.split("=", 1)[1]
    assert manifest["file_count"] == len(part_files)
    assert manifest["record_count"] == len(rows)
    assert all(row["schema_version"] == 1 and row["_id"]["$oid"] for row in rows)


def test_rules_are_shared_with_quarantine():
    from sys import path

    path.insert(0, str(ROOT / "src"))
    try:
        from transformations.rules import DROP_ALL, DROP_RULES, FAIL_RULES

        assert {"valid_update_time", "valid_product", "no_rescued_fields"} <= DROP_RULES.keys()
        assert all(f"({rule})" in DROP_ALL for rule in DROP_RULES.values())
        assert "schema_version = 1" in FAIL_RULES.values()
    finally:
        path.remove(str(ROOT / "src"))


def test_bundle_contains_required_resources():
    yaml = pytest.importorskip("yaml")
    bundle = yaml.safe_load((ROOT / "databricks.yml").read_text())
    assert "student_events" in bundle["resources"]["pipelines"]
    tasks = bundle["targets"]["dev"]["resources"]["jobs"]["student_events_refresh"]["tasks"]
    assert {task["task_key"] for task in tasks} == {
        "refresh_pipeline", "check_export_health", "write_back", "reconcile_sql_server"
    }
    demo_tasks = bundle["targets"]["demo"]["resources"]["jobs"]["student_events_refresh"]["tasks"]
    assert [task["task_key"] for task in demo_tasks] == ["refresh_pipeline"]
    assert "workspace" not in bundle or "host" not in bundle["workspace"]
    assert "${workspace.current_user.userName}" in bundle["targets"]["prod"]["workspace"]["root_path"]
