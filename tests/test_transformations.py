"""Spark DataFrame checks; skipped when local PySpark is unavailable."""

import sys
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from transformations.parse import quarantine_reasons, reconcile_exports, unwrap_extended_json  # noqa: E402
from transformations.rules import DROP_RULES  # noqa: E402
from transformations.schema import EVENT_SCHEMA  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[1]").appName("lexia-tests")
               .config("spark.sql.session.timeZone", "UTC")
               .config("spark.sql.shuffle.partitions", "2").getOrCreate())
    yield session
    session.stop()


def test_unwrap_extended_json(spark):
    sample = (
        Path(__file__).resolve().parents[1]
        / "sample_exports/run_date=2026-10-22/run_id=sample-001/part-00000.json"
    )
    parsed = unwrap_extended_json(
        spark.read.schema(EVENT_SCHEMA).json(sample.as_uri().replace("%3D", "="))
    ).first()
    assert parsed._id == "507f1f77bcf86cd799439011"
    assert parsed.updated_at > parsed.attempt_timestamp


@pytest.mark.parametrize(
    ("record_count", "file_count", "expected"),
    [(2, 1, "OK"), (3, 1, "MISSING_RECORDS"), (2, 2, "MISSING_FILES"),
     (1, 1, "EXTRA_RECORDS"), (2, 0, "EXTRA_FILES"), (-1, 1, "INVALID_MANIFEST")],
)
def test_reconciliation(spark, record_count, file_count, expected):
    manifest = spark.sql(
        "SELECT 'r1' AS run_id, 'r1' AS path_run_id, "
        f"CAST({record_count} AS BIGINT) AS record_count, {file_count} AS file_count, "
        "TIMESTAMP '2026-10-22 14:07:00' AS finished_at"
    )
    events = spark.sql(
        "SELECT 'r1' AS path_run_id, 'f1' AS source_file, "
        "TIMESTAMP '2026-10-22 14:00:00' AS exported_at "
        "UNION ALL SELECT 'r1', 'f1', TIMESTAMP '2026-10-22 14:00:00'"
    )
    status = reconcile_exports(manifest, events).first().status
    assert status == expected


def test_malformed_dates_do_not_bypass_quarantine(spark):
    from transformations.parse import quarantine_reasons
    from transformations.rules import DROP_RULES
    raw = spark.sql("""
        SELECT '{"_id":{"$oid":"a"},"attempt_timestamp":{"$date":"bad"},"updated_at":{"$date":"bad"}}' value
        UNION ALL
        SELECT '{"_id":{"$oid":"b"},"attempt_timestamp":{"$date":"2026-01-01T00:00:00Z"},"updated_at":{"$date":"bad"}}'
    """)
    from pyspark.sql import functions as F
    parsed = unwrap_extended_json(raw.select(F.from_json("value", EVENT_SCHEMA).alias("e")).select("e.*"))
    results = quarantine_reasons(parsed, DROP_RULES).collect()
    assert all(row.updated_at is None and "valid_update_time" in row.quality_errors for row in results)
    assert "valid_time" in results[0].quality_errors


def test_empty_and_missing_exports(spark):
    manifest = spark.sql("SELECT 'r1' run_id, 'r1' path_run_id, 0 record_count, 0 file_count, current_timestamp() finished_at")
    events = spark.sql("SELECT 'r1' path_run_id, 'f1' source_file, current_timestamp() exported_at WHERE false")
    assert reconcile_exports(manifest, events).first().status == "OK"
    assert reconcile_exports(manifest.union(manifest), events).first().status == "DUPLICATE_MANIFEST"
    from pyspark.sql import functions as F
    assert reconcile_exports(manifest.withColumn("record_count", F.lit(1)), events).first().status == "NOT_LOADED"
    populated = spark.sql("SELECT 'r2' path_run_id, 'f1' source_file, current_timestamp() exported_at")
    assert reconcile_exports(manifest.limit(0), populated).first().status == "NO_MANIFEST"
    assert reconcile_exports(manifest.withColumn("path_run_id", F.lit("wrong")), events).first().status == "RUN_ID_MISMATCH"


def test_conflicting_source_revisions(spark):
    from pyspark.sql import functions as F

    from transformations.parse import cdc_conflicts
    sample = Path(__file__).resolve().parents[1] / "sample_exports/run_date=2026-10-22/run_id=sample-001/part-00000.json"
    events = unwrap_extended_json(spark.read.schema(EVENT_SCHEMA).json(sample.as_uri().replace("%3D", "=")))
    assert cdc_conflicts(events.union(events)).count() == 0
    assert cdc_conflicts(events.union(events.withColumn("student_id", F.lit("conflict")))).count() == 2


def test_local_school_day_and_latest_progress(spark):
    from transformations.business import daily_completions, student_progress
    events = spark.sql("""
        SELECT 'a' _id, 'alpha' product_name, 's1' student_id, 'school' school_id,
          '01' grade_level, 'UnitCompleted' change_type, true is_completed,
          'u1' unit_id, 'l1' level_id, TIMESTAMP '2026-03-08 04:30:00' attempt_timestamp,
          TIMESTAMP '2026-03-08 04:31:00' updated_at
        UNION ALL SELECT 'b','alpha','s1','school','01','UnitCompleted',true,'u2','l2',
          TIMESTAMP '2026-03-08 07:30:00',TIMESTAMP '2026-03-08 07:31:00'
    """)
    schools = spark.sql("SELECT 'school' school_id, 'School' school_name, 'America/New_York' school_tz")
    days = daily_completions(events, schools).select("school_day").collect()
    assert {str(row.school_day) for row in days} == {"2026-03-07", "2026-03-08"}
    progress = student_progress(events).first()
    assert progress.total_completions == 2 and progress.latest_unit_id == "u2"


def test_malformed_json_and_null_change_type_are_quarantined(spark, tmp_path):
    sample = Path(__file__).resolve().parents[1] / "sample_exports/run_date=2026-10-22/run_id=sample-001/part-00000.json"
    import json

    event = json.loads(sample.read_text(encoding="utf-8").splitlines()[0])
    event["change_type"] = None
    path = tmp_path / "part-00000.json"
    path.write_text(json.dumps(event) + "\n{" + "\n", encoding="utf-8")
    parsed = unwrap_extended_json(
        spark.read.option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(EVENT_SCHEMA).json(str(path))
    )
    rows = quarantine_reasons(parsed, DROP_RULES).select("quality_errors", "_corrupt_record").collect()
    assert len(rows) == 2
    assert any("valid_change_type" in row.quality_errors and row._corrupt_record is None for row in rows)
    assert any("no_corrupt_record" in row.quality_errors and row._corrupt_record for row in rows)
