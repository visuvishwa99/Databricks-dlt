"""Run isolated 100k/1M input benchmarks; never publish into the demo serving database."""

import argparse
import json
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path

from run_local_postgres_e2e import statement
from workspace_cli import cli


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--target", choices=["benchmark100k", "benchmark1m"], required=True)
    p.add_argument("--profile", default="LEXIA")
    p.add_argument("--warehouse-id", required=True)
    args = p.parse_args()
    root = Path(".generated") / args.target
    manifests = list(root.rglob("manifest.json"))
    if len(manifests) != 1:
        raise ValueError("Generate exactly one export under .generated/<target> first")
    manifest = manifests[0]
    parts = sorted(manifest.parent.glob("part-*.json"))
    prefix = f"dbfs:/Volumes/workspace/default/lexia_sample_exports/{args.target}/"
    subprocess.run(["databricks", "fs", "mkdir", prefix + manifest.parent.relative_to(root).as_posix(),
                    "--profile", args.profile], check=True)

    def upload(path):
        destination = prefix + path.relative_to(root).as_posix()
        result = subprocess.run(["databricks", "fs", "cp", str(path), destination,
                                 "--profile", args.profile], capture_output=True, text=True)
        if result.returncode:
            raise RuntimeError(f"Upload failed: {path.name}; immutable paths are not overwritten")

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(upload, parts))
    upload(manifest)
    print(f"Uploaded {len(parts)} parts, then manifest", flush=True)
    subprocess.run(["databricks", "bundle", "deploy", "-t", args.target, "-p", args.profile], check=True)
    bundle = cli(args.profile, "bundle", "summary", "-t", args.target)
    job_id = bundle["resources"]["jobs"]["student_events_refresh"]["id"]
    start = time.monotonic()
    run = cli(args.profile, "jobs", "run-now", str(job_id), "--no-wait")
    while time.monotonic()-start < 3600:
        result = cli(args.profile, "jobs", "get-run", str(run["run_id"]))
        if result["state"]["life_cycle_state"] in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            break
        time.sleep(10)
    if result["state"].get("result_state") != "SUCCESS":
        raise RuntimeError(f"Benchmark did not succeed: {result['state']}")
    elapsed = time.monotonic()-start
    prefix = f"workspace.dbx_{args.target}"
    counts, statement_id = statement(args.profile, args.warehouse_id, f"""
      SELECT (SELECT count(*) FROM {prefix}.bronze_student_events),
        (SELECT count(*) FROM {prefix}.silver_student_events),
        (SELECT count(*) FROM {prefix}.gold_student_progress),
        (SELECT count(*) FROM {prefix}.silver_quarantine_events),
        (SELECT count(*) FROM {prefix}.ops_export_reconciliation WHERE status <> 'OK' OR status IS NULL),
        (SELECT count(*) FROM {prefix}.ops_cdc_conflicts)
    """)
    bronze, silver, gold, quarantine, failed, conflicts = map(int, counts[0])
    expected = json.loads(manifest.read_text())["record_count"]
    if (bronze, silver, gold, quarantine, failed, conflicts) != (expected, expected, 10000, 0, 0, 0):
        raise RuntimeError(f"Benchmark counts failed: {counts}")
    report = {"result": "PASS", "target": args.target, "checked_at": datetime.now(UTC).isoformat(),
              "input_rows": expected, "input_bytes": sum(p.stat().st_size for p in parts),
              "bronze_rows": bronze, "current_rows": silver, "gold_students": gold,
              "quarantine": quarantine, "failed_exports": failed, "conflicts": conflicts,
              "run_id": run["run_id"], "statement_id": statement_id,
              "wall_seconds_including_startup": round(elapsed, 2),
              "scope": "Single cold pipeline run; excludes PostgreSQL transport, p95, cost and recovery claims"}
    Path(f"docs/{args.target}-evidence.json").write_text(json.dumps(report, indent=2)+"\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
