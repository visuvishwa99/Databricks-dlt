"""Explicit quarantine reviews; changed contents must be reviewed again."""

import json
import re
from pathlib import Path


def unreviewed_quarantine(rows, review_path, source):
    reviews = []
    if review_path:
        document = json.loads(Path(review_path).read_text())
        if document.get("source") != source or document.get("version") != 1:
            raise ValueError("Review file source/version does not match this pipeline")
        reviews = document.get("quarantine", [])
    accepted = set()
    for review in reviews:
        if (not review.get("reviewed_by") or not review.get("reason") or not review.get("reviewed_at")
                or not isinstance(review.get("count"), int) or review["count"] < 1
                or not re.fullmatch(r"[0-9a-f]{64}", review.get("fingerprint", ""))):
            raise ValueError("Quarantine review requires reviewer, time, reason, count and fingerprint")
        accepted.add((review["run_id"], review["count"], review["fingerprint"]))
    findings = [{"run_id": run, "count": int(count), "fingerprint": fingerprint}
                for run, count, fingerprint in rows]
    pending = sum(r["count"] for r in findings
                  if (r["run_id"], r["count"], r["fingerprint"]) not in accepted)
    return pending, findings
