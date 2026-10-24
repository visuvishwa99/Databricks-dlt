"""Reconcile or clean PostgreSQL using a native driver and scope-specific identity."""

import argparse
import json

from common import validate_run_id
from postgres_common import cleanup, connection_options, reconcile
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--action", choices=["reconcile", "cleanup"], required=True)
    p.add_argument("--secret-scope", required=True)
    p.add_argument("--run-id")
    p.add_argument("--retention-days", type=int, default=7)
    args = p.parse_args()
    secrets = DBUtils(SparkSession.builder.getOrCreate()).secrets
    options = connection_options({k: secrets.get(args.secret_scope, k)
                                  for k in ("host", "port", "database", "user", "password", "sslrootcert")})
    if args.action == "reconcile":
        count = reconcile(options, validate_run_id(args.run_id or ""))
    else:
        count = cleanup(options, args.retention_days)
    print(json.dumps({"event": args.action, "rows": count, "run_id": args.run_id}))


if __name__ == "__main__":
    main()
