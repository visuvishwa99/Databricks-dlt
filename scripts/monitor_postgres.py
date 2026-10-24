"""Independent local no-run monitor and HTML dashboard; nonzero exit signals alarm."""

import argparse
import html
import json
import sys
from datetime import UTC, datetime
from pathlib import Path

import psycopg
from workspace_cli import secret

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src/jobs"))
from postgres_common import connection_options  # noqa: E402


def health(latest, pending, now, max_age_minutes):
    if latest is None:
        return "ALERT: no reconciled publication"
    if latest > now:
        return "ALERT: future publication timestamp"
    if pending:
        return "ALERT: publication awaiting reconciliation"
    if (now-latest).total_seconds() > max_age_minutes*60:
        return "ALERT: no recent reconciled publication"
    return "OK"


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--profile", default="LEXIA")
    p.add_argument("--secret-scope", default="sis_postgres")
    p.add_argument("--max-age-minutes", type=int, default=60)
    p.add_argument("--output", default=".generated/monitor.html")
    args = p.parse_args()
    if args.max_age_minutes <= 0:
        raise ValueError("Monitor age must be positive")
    now = datetime.now(UTC)
    try:
        options = connection_options({k: secret(args.profile, args.secret_scope, k)
                                      for k in ("host", "port", "database", "user", "password")}, local=True)
        with psycopg.connect(**options) as c:
            latest, pending = c.execute("SELECT max(applied_at) FILTER (WHERE reconciled_at IS NOT NULL), count(*) FILTER (WHERE reconciled_at IS NULL) FROM serving.publish_audit").fetchone()
            rows = c.execute("SELECT count(*) FROM serving.student_progress").fetchone()[0]
            status = health(latest, pending, now, args.max_age_minutes)
            data = {"status": status, "checked_at": now.isoformat(), "last_reconciled_publish": str(latest),
                    "unreconciled_runs": pending, "serving_rows": rows, "max_age_minutes": args.max_age_minutes}
    except Exception:
        # Never include driver errors/connection strings in alerts.
        data = {"status": "ALERT: monitor could not verify database", "checked_at": now.isoformat()}
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text("<!doctype html><html lang='en'><meta charset='utf-8'><title>PostgreSQL publication health</title>"
                      "<style>body{font:18px system-ui;max-width:850px;margin:60px auto;background:#101827;color:#edf2fa}"
                      "td,th{padding:14px;text-align:left;border-bottom:1px solid #42526a}</style>"
                      "<h1>PostgreSQL publication health</h1><p>Snapshot dashboard; rerun the monitor to refresh.</p><table>"
                      + "".join(f"<tr><th>{html.escape(k)}</th><td>{html.escape(str(v))}</td></tr>" for k,v in data.items())
                      + "</table><p>This monitor must run independently of the publisher to detect missed runs.</p></html>")
    output.with_suffix(".json").write_text(json.dumps(data, indent=2)+"\n")
    print(json.dumps(data))
    raise SystemExit(0 if data["status"] == "OK" else 2)


if __name__ == "__main__":
    main()
