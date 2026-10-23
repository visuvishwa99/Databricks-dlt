"""Read-only post-deployment checks using an existing SQL warehouse."""

import argparse
import json
import re
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--warehouse-id", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--profile")
    parser.add_argument("--expected-current-rows", type=int)
    parser.add_argument("--max-quarantine-rows", type=int, default=0)
    parser.add_argument("--output")
    args = parser.parse_args()
    if not all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", v) for v in (args.catalog, args.schema)):
        raise ValueError("Invalid catalog/schema identifier")
    prefix = f"{args.catalog}.{args.schema}"
    queries = {
        "bronze_rows": f"SELECT count(*) FROM {prefix}.bronze_student_events",
        "current_rows": f"SELECT count(*) FROM {prefix}.silver_student_events",
        "daily_rows": f"SELECT count(*) FROM {prefix}.gold_daily_completions",
        "quarantine_rows": f"SELECT count(*) FROM {prefix}.silver_quarantine_events",
        "failed_exports": f"SELECT count(*) FROM {prefix}.ops_export_reconciliation WHERE status <> 'OK' OR status IS NULL",
        "conflicting_revisions": f"SELECT count(*) FROM {prefix}.ops_cdc_conflicts",
    }
    sql = " UNION ALL ".join(f"SELECT '{key}' metric, ({query}) value" for key, query in queries.items())

    def cli(*arguments):
        command = ["databricks", *arguments, "--output", "json"]
        if args.profile:
            command += ["--profile", args.profile]
        return json.loads(subprocess.check_output(command, text=True))

    response = cli("api", "post", "/api/2.0/sql/statements", "--json", json.dumps({
        "warehouse_id": args.warehouse_id, "statement": sql, "wait_timeout": "10s",
    }))
    deadline = time.monotonic() + 180
    while response["status"]["state"] in ("PENDING", "RUNNING") and time.monotonic() < deadline:
        time.sleep(5)
        response = cli("api", "get", f"/api/2.0/sql/statements/{response['statement_id']}")
    if response["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(f"Smoke query did not succeed: {response['status']['state']}; statement {response['statement_id']}")
    metrics = {key: int(value) for key, value in response["result"]["data_array"]}
    if metrics["failed_exports"] or metrics["conflicting_revisions"]:
        raise RuntimeError(f"Data contract smoke failure: {metrics}")
    if metrics["quarantine_rows"] > args.max_quarantine_rows:
        raise RuntimeError(f"Quarantine smoke threshold exceeded: {metrics}")
    if args.expected_current_rows is not None and metrics["current_rows"] != args.expected_current_rows:
        raise RuntimeError(f"Unexpected current document count: {metrics}")
    report = {"checked_at": datetime.now(UTC).isoformat(), "namespace": prefix,
              "statement_id": response["statement_id"], "metrics": metrics, "result": "PASS"}
    text = json.dumps(report, indent=2)
    if args.output:
        Path(args.output).write_text(text + "\n", encoding="utf-8")
    print(text)


if __name__ == "__main__":
    main()
