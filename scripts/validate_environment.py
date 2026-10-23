"""Fail before deployment when environment wiring is incomplete."""

import argparse
import json
import os
import re
import subprocess
from urllib.parse import urlparse


def validate(env, target):
    required = ["DATABRICKS_HOST", "DATABRICKS_CLIENT_ID", "BUNDLE_VAR_catalog",
                "BUNDLE_VAR_schema", "BUNDLE_VAR_export_root", "BUNDLE_VAR_school_table",
                "BUNDLE_VAR_writeback_cluster_id", "BUNDLE_VAR_alert_email"]
    if target == "prod":
        required.append("BUNDLE_VAR_run_as_service_principal")
    for key in required:
        if not env.get(key) or "REPLACE" in env[key]:
            raise ValueError(f"Missing environment setting: {key}")
    host = urlparse(env["DATABRICKS_HOST"])
    if host.scheme != "https" or not host.hostname or host.path not in ("", "/"):
        raise ValueError("DATABRICKS_HOST must be an HTTPS workspace origin")
    for key in ("BUNDLE_VAR_catalog", "BUNDLE_VAR_schema"):
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", env[key]):
            raise ValueError(f"Invalid identifier: {key}")
    if not env["BUNDLE_VAR_export_root"].startswith("s3://"):
        raise ValueError("Full environments require an S3 export prefix")
    if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", env["BUNDLE_VAR_alert_email"]):
        raise ValueError("Configure a real operations alert email")
    if target == "prod" and env.get("GITHUB_REF", "refs/heads/main") != "refs/heads/main":
        raise ValueError("Production deployment must use main")
    if target == "prod" and env["BUNDLE_VAR_catalog"] == "workspace":
        raise ValueError("Production requires a dedicated catalog")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=["dev", "prod"], required=True)
    parser.add_argument("--live", action="store_true")
    args = parser.parse_args()
    validate(os.environ, args.target)
    if args.live:
        def cli(*arguments):
            output = subprocess.check_output(["databricks", *arguments, "--output", "json"], text=True)
            return json.loads(output)
        cli("schemas", "get", f"{os.environ['BUNDLE_VAR_catalog']}.{os.environ['BUNDLE_VAR_schema']}")
        cli("tables", "get", os.environ["BUNDLE_VAR_school_table"])
        cluster = cli("clusters", "get", os.environ["BUNDLE_VAR_writeback_cluster_id"])
        if cluster.get("data_security_mode") != "SINGLE_USER":
            raise ValueError("JDBC publisher requires a dedicated SINGLE_USER cluster")
        version = re.match(r"(\d+)\.(\d+)", cluster.get("spark_version", ""))
        if not version or tuple(map(int, version.groups())) < (15, 4):
            raise ValueError("JDBC publisher requires DBR 15.4 or later with Python 3.11+")
    print(f"Environment preflight passed: {args.target}")


if __name__ == "__main__":
    main()
