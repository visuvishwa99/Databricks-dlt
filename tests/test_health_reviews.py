import json

import pytest
from health_reviews import unreviewed_quarantine


def review_file(tmp_path):
    path = tmp_path / "review.json"
    path.write_text(json.dumps({"version": 1, "source": "demo", "quarantine": [
        {"run_id": "bad-export", "count": 2, "fingerprint": "a" * 64,
         "reviewed_by": "operator", "reviewed_at": "2026-10-23", "reason": "Intentional malformed drill"}
    ]}))
    return path


def test_exact_review_removes_only_known_findings(tmp_path):
    rows = [("bad-export", "2", "a" * 64), ("new-export", "1", "b" * 64)]
    pending, findings = unreviewed_quarantine(rows, review_file(tmp_path), "demo")
    assert pending == 1
    assert len(findings) == 2


@pytest.mark.parametrize("row", [("bad-export", "3", "a" * 64), ("bad-export", "2", "b" * 64)])
def test_changed_data_invalidates_review(tmp_path, row):
    assert unreviewed_quarantine([row], review_file(tmp_path), "demo")[0] == int(row[1])


def test_review_cannot_apply_to_another_source(tmp_path):
    with pytest.raises(ValueError, match="source"):
        unreviewed_quarantine([], review_file(tmp_path), "prod")


def test_no_review_counts_all_rows():
    assert unreviewed_quarantine([("old", "2", "a" * 64)], None, "demo")[0] == 2
