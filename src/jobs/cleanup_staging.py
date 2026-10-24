# LEGACY SQL SERVER REFERENCE: not used by the active PostgreSQL bundle or CI.
"""Remove old unaudited SQL staging attempts with a maintenance identity."""

import argparse
import json

from common import jdbc_options
from pyspark.sql import SparkSession


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--secret-scope", required=True)
    parser.add_argument("--retention-days", type=int, default=7)
    args = parser.parse_args()
    if not 7 <= args.retention_days <= 365:
        raise ValueError("Staging retention must be between 7 and 365 days")

    spark = SparkSession.builder.getOrCreate()
    options = jdbc_options(spark, args.secret_scope)
    connection = spark.sparkContext._gateway.jvm.java.sql.DriverManager.getConnection(
        options["url"], options["user"], options["password"]
    )
    try:
        statement = connection.prepareCall("{call dbo.usp_cleanup_student_progress_staging_v2(?)}")
        try:
            statement.setInt(1, args.retention_days)
            statement.setQueryTimeout(300)
            result = statement.executeQuery()
            try:
                deleted_rows = int(result.getLong(1)) if result.next() else 0
            finally:
                result.close()
        finally:
            statement.close()
    finally:
        connection.close()
    print(json.dumps({"event": "staging_cleanup_completed", "deleted_rows": deleted_rows}))


if __name__ == "__main__":
    main()
