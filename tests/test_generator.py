import json
from datetime import UTC, datetime

import pytest
from generate_export import generate


def test_generator_counts_and_immutable_run(tmp_path):
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    folder = generate(tmp_path, "run-1", 5, 2, timestamp, quarantine_every=2)
    manifest = json.loads((folder / "manifest.json").read_text())
    rows = [json.loads(line) for file in folder.glob("part-*.json") for line in file.read_text().splitlines()]
    assert manifest["file_count"] == 3 and manifest["record_count"] == len(rows) == 5
    assert sum(row["student_id"] is None for row in rows) == 3
    with pytest.raises(FileExistsError):
        generate(tmp_path, "run-1", 1, 2, timestamp)


def test_generator_empty_heartbeat(tmp_path):
    folder = generate(tmp_path, "empty", 0, 10, datetime(2026, 1, 1, tzinfo=UTC))
    assert not list(folder.glob("part-*.json"))
    assert json.loads((folder / "manifest.json").read_text())["file_count"] == 0


def test_generator_controls_distinct_student_count(tmp_path):
    folder = generate(tmp_path, "cardinality", 8, 3, datetime(2026, 1, 1, tzinfo=UTC),
                      student_cardinality=3)
    rows = [json.loads(line) for file in folder.glob("part-*.json") for line in file.read_text().splitlines()]
    assert len({row["student_id"] for row in rows}) == 3
