# LEGACY SQL SERVER REFERENCE: not used by the active PostgreSQL bundle or CI.
"""Stage a gold snapshot and invoke the idempotent SQL Server merge procedure.

Run on a dedicated Databricks cluster with the Microsoft SQL Server JDBC driver.
Secrets ``sis/jdbc_url``, ``sis/user`` and ``sis/password`` must exist.
"""

import argparse
import json
import uuid
from datetime import UTC, datetime

from common import PROGRESS_COLUMNS, jdbc_options, table_name, validate_run_id
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--secret-scope", default="sis")
    parser.add_argument("--jdbc-partitions", type=int, default=4)
    parser.add_argument("--allow-deletes", action="store_true")
    parser.add_argument("--max-delete-fraction", type=float, default=0.05)
    args = parser.parse_args()
    validate_run_id(args.run_id)
    if not 1 <= args.jdbc_partitions <= 8 or not 0 <= args.max_delete_fraction <= 1:
        raise ValueError("Invalid JDBC partition count or deletion threshold")

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    options = jdbc_options(spark, args.secret_scope)
    source = table_name(args.catalog, args.schema, "gold_student_progress")
    snapshot_at = datetime.now(UTC).replace(tzinfo=None).isoformat()
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
            .options(**options).option("dbtable", "stg.student_progress_v2")
            .option("batchsize", "1000").mode("append").save()
        )
    finally:
        snapshot.unpersist()

    # The stored procedure owns the target transaction and audit row.
    jvm = spark.sparkContext._gateway.jvm
    connection = jvm.java.sql.DriverManager.getConnection(
        options["url"], options["user"], options["password"]
    )
    try:
        statement = connection.prepareCall("{call dbo.usp_publish_student_progress_v2(?,?,?,?,?,?)}")
        try:
            statement.setString(1, args.run_id)
            statement.setString(2, attempt_id)
            statement.setLong(3, expected_rows)
            statement.setString(4, snapshot_at)
            statement.setBoolean(5, args.allow_deletes)
            statement.setDouble(6, args.max_delete_fraction)
            statement.setQueryTimeout(300)
            statement.execute()
        finally:
            statement.close()
    finally:
        connection.close()
    print(json.dumps({"event": "writeback_committed", "run_id": args.run_id,
                      "attempt_id": attempt_id, "rows": expected_rows}))


if __name__ == "__main__":
    main()
