"""Generate immutable synthetic exports for isolated load and replay exercises."""

import argparse
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path


def generate(root, run_id, rows, rows_per_file, event_time, revision=0, quarantine_every=0):
    if rows < 0 or rows_per_file <= 0 or revision < 0 or quarantine_every < 0:
        raise ValueError("Counts and revision must be nonnegative; rows_per_file must be positive")
    if not run_id or any(c not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for c in run_id):
        raise ValueError("run_id must contain only letters, digits, hyphen or underscore")
    now = datetime.now(UTC)
    updated = event_time + timedelta(seconds=revision)
    if event_time.tzinfo is None or updated > now + timedelta(minutes=5):
        raise ValueError("Use an aware event timestamp; revision cannot put updates in the future")
    destination = Path(root) / f"run_date={now.date()}" / f"run_id={run_id}"
    destination.mkdir(parents=True, exist_ok=False)
    file_count = 0
    for start in range(0, rows, rows_per_file):
        with (destination / f"part-{file_count:05d}.json").open("x", encoding="utf-8") as output:
            for i in range(start, min(start + rows_per_file, rows)):
                record = {
                    "_id": {"$oid": f"{i:024x}"}, "schema_version": 1,
                    "product_name": "alpha", "change_type": "UnitCompleted",
                    "student_id": None if quarantine_every and i % quarantine_every == 0 else f"synthetic-{i % 10000}",
                    "school_id": "demo-school-1", "grade_level": "03", "unit_id": f"unit-{revision}",
                    "level_id": "level-1", "is_completed": True,
                    "attempt_timestamp": {"$date": event_time.isoformat()},
                    "updated_at": {"$date": updated.isoformat()},
                }
                output.write(json.dumps(record) + "\n")
        file_count += 1
    manifest = {"run_id": run_id, "schema_version": 1, "started_at": now.isoformat(),
                "finished_at": datetime.now(UTC).isoformat(), "file_count": file_count,
                "record_count": rows, "max_event_ts": event_time.isoformat() if rows else None}
    with (destination / "manifest.json").open("x", encoding="utf-8") as marker:
        json.dump(manifest, marker)
    return destination


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", default=".generated")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--rows-per-file", type=int, default=10000)
    parser.add_argument("--event-time", default=(datetime.now(UTC) - timedelta(hours=1)).isoformat())
    parser.add_argument("--revision", type=int, default=0)
    parser.add_argument("--quarantine-every", type=int, default=0)
    args = parser.parse_args()
    print(generate(args.output, args.run_id, args.rows, args.rows_per_file,
                   datetime.fromisoformat(args.event_time), args.revision, args.quarantine_every))


if __name__ == "__main__":
    main()
