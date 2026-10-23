"""Fail when a completed export is missing or no recent manifest arrived."""

import argparse
import json

from pyspark.sql import SparkSession, functions as F
from common import table_name


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--freshness-minutes", type=int, default=60)
    parser.add_argument("--max-quarantine-fraction", type=float, default=0.01)
    args = parser.parse_args()

    spark = SparkSession.builder.getOrCreate()
    if args.freshness_minutes <= 0 or not 0 <= args.max_quarantine_fraction <= 1:
        raise ValueError("Invalid health thresholds")
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    table = table_name(args.catalog, args.schema, "ops_export_reconciliation")
    reconciliation = spark.read.table(table)
    # A grace window may suppress paging, but must never permit incomplete publication.
    bad = reconciliation.where("status != 'OK' OR status IS NULL").select("run_id", "status").limit(10).collect()
    if bad:
        raise RuntimeError(f"Export reconciliation failures: {[(r.run_id, r.status) for r in bad]}")
    if reconciliation.where("finished_at > current_timestamp() + INTERVAL 5 MINUTES").limit(1).count():
        raise RuntimeError("Future-dated producer manifest")
    latest = reconciliation.where("status = 'OK'").agg(F.max("finished_at").alias("latest")).first().latest
    if latest is None:
        raise RuntimeError("No export manifest has been ingested")
    freshness_ok = spark.createDataFrame([(latest,)], "latest timestamp").select(
        (
            F.col("latest")
            >= F.current_timestamp() - F.expr(f"INTERVAL {args.freshness_minutes} MINUTES")
        ).alias("ok")
    ).first().ok
    if not freshness_ok:
        raise RuntimeError(f"No manifest in the past {args.freshness_minutes} minutes")
    recent_runs = reconciliation.where(
        F.col("finished_at") >= F.current_timestamp() - F.expr(f"INTERVAL {args.freshness_minutes} MINUTES")
    ).select("run_id")
    bronze = spark.read.table(table_name(args.catalog, args.schema, "bronze_student_events"))
    quarantine = spark.read.table(table_name(args.catalog, args.schema, "silver_quarantine_events"))
    total = bronze.join(recent_runs, "run_id", "left_semi").count()
    rejected = quarantine.join(recent_runs, "run_id", "left_semi").count()
    if rejected / max(total, 1) > args.max_quarantine_fraction:
        raise RuntimeError(f"Quarantine threshold exceeded: rejected={rejected}, total={total}")
    print(json.dumps({"event": "export_health_passed", "rows": total, "rejected": rejected}))


if __name__ == "__main__":
    main()
