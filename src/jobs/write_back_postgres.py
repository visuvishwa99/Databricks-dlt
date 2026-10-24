"""Stage a gold snapshot and invoke the idempotent PostgreSQL merge procedure.

Run on a dedicated Databricks cluster with the PostgreSQL JDBC driver.
The scope requires host, port, database, user, password and sslrootcert keys.
"""

import argparse
import json
import uuid
from datetime import UTC, datetime

from common import PROGRESS_COLUMNS, table_name, validate_run_id
from postgres_common import connection_options, jdbc_options, publish
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--secret-scope", default="sis_postgres")
    parser.add_argument("--jdbc-partitions", type=int, default=4)
    parser.add_argument("--allow-deletes", action="store_true")
    parser.add_argument("--max-delete-fraction", type=float, default=0.05)
    args = parser.parse_args()
    validate_run_id(args.run_id)
    if not 1 <= args.jdbc_partitions <= 8 or not 0 <= args.max_delete_fraction <= 1:
        raise ValueError("Invalid JDBC partition count or deletion threshold")

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    secrets = DBUtils(spark).secrets
    native = connection_options({key: secrets.get(args.secret_scope, key)
                                 for key in ("host", "port", "database", "user", "password", "sslrootcert")})
    options = jdbc_options(native)
    source = table_name(args.catalog, args.schema, "gold_student_progress")
    snapshot_at = datetime.now(UTC)
    attempt_id = str(uuid.uuid4())
    snapshot = spark.read.table(source).select(*PROGRESS_COLUMNS).persist()
    try:
        expected_rows = snapshot.count()
        if expected_rows == 0:
            raise RuntimeError("Empty gold snapshot blocked; investigate before publishing")
        if snapshot.groupBy("product_name", "student_id").count().where("count > 1").limit(1).count():
            raise RuntimeError("Duplicate gold business keys")
        (
            snapshot.withColumn("run_id", F.lit(args.run_id))
            .withColumn("attempt_id", F.lit(attempt_id))
            .coalesce(args.jdbc_partitions).write.format("jdbc")
            .options(**options).option("dbtable", "stg.student_progress")
            .option("batchsize", "1000").mode("append").save()
        )
    finally:
        snapshot.unpersist()

    publish(native, args.run_id, attempt_id, expected_rows, snapshot_at,
            allow_deletes=args.allow_deletes, fraction=args.max_delete_fraction)
    print(json.dumps({"event": "writeback_committed", "run_id": args.run_id,
                      "attempt_id": attempt_id, "rows": expected_rows}))


if __name__ == "__main__":
    main()
