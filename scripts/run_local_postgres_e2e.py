"""Outbound-only demo: refresh Databricks, fetch bounded Gold snapshot, publish locally."""

import argparse
import json
import sys
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

import psycopg
from workspace_cli import cli, secret

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src/jobs"))
from common import PROGRESS_COLUMNS, table_name, validate_run_id  # noqa: E402
from postgres_common import connection_options, publish, reconcile  # noqa: E402


def statement(profile, warehouse, query):
    response = cli(profile, "api", "post", "/api/2.0/sql/statements", "--json", json.dumps({
        "warehouse_id": warehouse, "statement": query, "wait_timeout": "10s",
        "disposition": "INLINE", "format": "JSON_ARRAY", "on_wait_timeout": "CONTINUE",
    }))
    deadline = time.monotonic() + 600
    while response["status"]["state"] in ("PENDING", "RUNNING"):
        if time.monotonic() >= deadline:
            raise RuntimeError("SQL statement timeout; publication has not started")
        time.sleep(5)
        response = cli(profile, "api", "get", f"/api/2.0/sql/statements/{response['statement_id']}")
    if response["status"]["state"] != "SUCCEEDED" or response.get("manifest", {}).get("truncated"):
        raise RuntimeError("SQL snapshot failed or was truncated; publication blocked")
    rows = []
    chunk = response.get("result", {})
    while True:
        rows.extend(chunk.get("data_array", []))
        link = chunk.get("next_chunk_internal_link")
        if not link:
            break
        if not link.startswith(f"/api/2.0/sql/statements/{response['statement_id']}/result/chunks/"):
            raise RuntimeError("Unexpected SQL chunk location")
        chunk = cli(profile, "api", "get", link)
    if len(rows) != response["manifest"]["total_row_count"]:
        raise RuntimeError("Incomplete SQL result")
    return rows, response["statement_id"]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--profile", default="LEXIA")
    p.add_argument("--warehouse-id", required=True)
    p.add_argument("--job-id", type=int, required=True)
    p.add_argument("--catalog", default="workspace")
    p.add_argument("--schema", default="default")
    p.add_argument("--max-rows", type=int, default=100000)
    p.add_argument("--max-quarantine-rows", type=int, default=0)
    p.add_argument("--secret-scope", default="sis_postgres")
    p.add_argument("--output", default=".generated/postgres-e2e.json")
    args = p.parse_args()
    if args.max_rows <= 0 or args.max_quarantine_rows < 0:
        raise ValueError("Invalid row bounds")
    namespace = table_name(args.catalog, args.schema, "gold_student_progress")
    run = cli(args.profile, "jobs", "run-now", str(args.job_id), "--no-wait")
    deadline = time.monotonic() + 1800
    while True:
        status = cli(args.profile, "jobs", "get-run", str(run["run_id"]))
        state = status["state"]
        if state["life_cycle_state"] in ("TERMINATED", "SKIPPED", "INTERNAL_ERROR"):
            if state.get("result_state") != "SUCCESS":
                raise RuntimeError(f"Pipeline refresh failed: {state}")
            break
        if time.monotonic() > deadline:
            raise RuntimeError("Pipeline timeout; publication blocked")
        time.sleep(10)
    print(f"Pipeline refresh succeeded: {run['run_id']}", flush=True)
    prefix = f"`{args.catalog}`.`{args.schema}`"
    health, _ = statement(args.profile, args.warehouse_id, f"""
      SELECT (SELECT count(*) FROM {prefix}.ops_export_reconciliation WHERE status <> 'OK' OR status IS NULL),
             (SELECT count(*) FROM {prefix}.ops_cdc_conflicts),
             (SELECT count(*) FROM {prefix}.silver_quarantine_events)
    """)
    failed, conflicts, quarantine = map(int, health[0])
    if failed or conflicts or quarantine > args.max_quarantine_rows:
        raise RuntimeError("Source quality gate failed")
    snapshot_at = datetime.now(UTC)
    rows, statement_id = statement(args.profile, args.warehouse_id,
                                  f"SELECT {', '.join(PROGRESS_COLUMNS)} FROM {namespace} LIMIT {args.max_rows+1}")
    if not rows or len(rows) > args.max_rows:
        raise RuntimeError("Empty or oversized snapshot blocked")
    keys = [(row[0], row[1]) for row in rows]
    if len(set(keys)) != len(keys):
        raise RuntimeError("Duplicate Gold business keys")
    options = connection_options({k: secret(args.profile, args.secret_scope, k)
                                  for k in ("host", "port", "database", "user", "password")}, local=True)
    run_id = validate_run_id(f"dbx_{run['run_id']}")
    attempt = str(uuid.uuid4())
    with psycopg.connect(**options) as c:
        with c.cursor() as cursor:
            cursor.executemany(
                f"INSERT INTO stg.student_progress (run_id,attempt_id,{','.join(PROGRESS_COLUMNS)}) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                [(run_id, attempt, *row[:5], row[5], int(row[6])) for row in rows],
            )
    publish(options, run_id, attempt, len(rows), snapshot_at)
    verified = reconcile(options, run_id)
    # Exercise retry-after-commit behavior without modifying the published snapshot.
    publish(options, run_id, attempt, len(rows), snapshot_at)
    replay_verified = reconcile(options, run_id)
    report = {"result": "PASS", "mode": "local outbound demo runner", "database": "dbx_db",
              "checked_at": datetime.now(UTC).isoformat(), "pipeline_run_id": run["run_id"],
              "statement_id": statement_id, "publish_run_id": run_id, "published_rows": verified,
              "replay_verified_rows": replay_verified, "failed_exports": failed,
              "conflicting_revisions": conflicts, "quarantine_rows": quarantine,
              "limitations": ["Synthetic demo; fixed-sample freshness gate not applied",
                              "Local runner must remain online; no cloud-to-laptop route verified"]}
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
