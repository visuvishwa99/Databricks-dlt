# Databricks notebook source
# ruff: noqa: F821
"""Independent cloud no-run check. Failure triggers the job's configured email."""

import re

dbutils.widgets.text("catalog", "workspace")  # noqa: F821
dbutils.widgets.text("schema", "default")  # noqa: F821
dbutils.widgets.text("publisher_job_id", "")  # noqa: F821
dbutils.widgets.text("max_age_minutes", "60")  # noqa: F821
catalog = dbutils.widgets.get("catalog")  # noqa: F821
schema = dbutils.widgets.get("schema")  # noqa: F821
job_id = dbutils.widgets.get("publisher_job_id")  # noqa: F821
minutes = int(dbutils.widgets.get("max_age_minutes"))  # noqa: F821
if not all(re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", v) for v in (catalog, schema)):
    raise ValueError("Invalid namespace")
if not job_id.isdigit() or minutes <= 0:
    raise ValueError("Invalid monitor settings")
status = spark.sql(f"""
  SELECT count(*)=1 AND max(reconciled_at) BETWEEN
    current_timestamp()-INTERVAL {minutes} MINUTES AND current_timestamp() AS healthy
  FROM `{catalog}`.`{schema}`.ops_publish_heartbeat WHERE job_id='{job_id}'
""").first()  # noqa: F821
if not status.healthy:
    raise RuntimeError("No recent successful PostgreSQL publication; check local runner and database")
print("Publication heartbeat is healthy")
