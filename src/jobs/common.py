"""Shared serving contract and runtime configuration; no credentials in logs."""

import re

PROGRESS_COLUMNS = [
    "product_name", "student_id", "school_id", "latest_unit_id",
    "latest_level_id", "last_completed_at", "total_completions",
]


def table_name(catalog, schema, table):
    for value in (catalog, schema, table):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
            raise ValueError("Catalog, schema and table must be simple SQL identifiers")
    return f"`{catalog}`.`{schema}`.`{table}`"


def validate_run_id(value):
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", value):
        raise ValueError("Invalid run ID")
    return value


def jdbc_options(spark, scope):
    from pyspark.dbutils import DBUtils

    secrets = DBUtils(spark).secrets
    url = secrets.get(scope, "jdbc_url")
    fields = dict(
        part.strip().lower().split("=", 1)
        for part in url.split(";")[1:] if "=" in part
    )
    if fields.get("encrypt") != "true" or fields.get("trustservercertificate") != "false":
        raise ValueError("JDBC URL must require encrypt=true;trustServerCertificate=false")
    return {
        "url": url, "user": secrets.get(scope, "user"),
        "password": secrets.get(scope, "password"),
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }
