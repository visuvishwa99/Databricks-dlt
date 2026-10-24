"""Provision dbx_db objects and least-privilege logins; passwords stay in secrets."""

import argparse
import secrets
from pathlib import Path

import psycopg
from psycopg import sql
from workspace_cli import cli, put_secret, secret


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="LEXIA")
    args = parser.parse_args()
    scopes = {s["name"] for s in cli(args.profile, "secrets", "list-scopes")}
    credentials = {}
    for scope, role in (("sis_postgres", "dbx_publisher"), ("sis_postgres_maintenance", "dbx_maintenance")):
        if scope not in scopes:
            cli(args.profile, "secrets", "create-scope", scope)
        keys = {s["key"] for s in cli(args.profile, "secrets", "list-secrets", scope)}
        if "password" not in keys:
            put_secret(args.profile, scope, "password", secrets.token_urlsafe(36))
        credentials[role] = secret(args.profile, scope, "password")
        for key, value in {"user": role, "host": "localhost", "port": "5432", "database": "dbx_db"}.items():
            if key not in keys:
                put_secret(args.profile, scope, key, value)
    with psycopg.connect(host="localhost", port=5432, dbname="dbx_db", user="postgres",
                         password=secret(args.profile, "postgres_local_admin", "password"),
                         connect_timeout=10) as c:
        for role in ("dbx_owner", *credentials):
            if not c.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (role,)).fetchone():
                c.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(role)))
        c.execute("GRANT CREATE ON DATABASE dbx_db TO dbx_owner")
        c.execute("SET ROLE dbx_owner")
        # Migration has its own BEGIN/COMMIT; strip them to retain one provisioning transaction.
        migration = (Path(__file__).resolve().parents[1] / "sql/postgres_setup.sql").read_text()
        c.execute(migration.replace("BEGIN;\nCREATE SCHEMA", "CREATE SCHEMA").removesuffix("COMMIT;\n"))
        c.execute("RESET ROLE")
        for role, password in credentials.items():
            c.execute(sql.SQL("ALTER ROLE {} LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD {}")
                      .format(sql.Identifier(role), sql.Literal(password)))
        c.execute("GRANT CONNECT ON DATABASE dbx_db TO dbx_publisher, dbx_maintenance")
        c.execute("GRANT USAGE ON SCHEMA stg, serving TO dbx_publisher")
        c.execute("GRANT INSERT, SELECT ON stg.student_progress TO dbx_publisher")
        c.execute("GRANT SELECT ON serving.student_progress, serving.publish_audit TO dbx_publisher")
        c.execute("GRANT EXECUTE ON FUNCTION serving.publish_progress(text,text,bigint,timestamptz,boolean,double precision), serving.reconcile_progress(text) TO dbx_publisher")
        c.execute("GRANT USAGE ON SCHEMA serving TO dbx_maintenance")
        c.execute("GRANT EXECUTE ON FUNCTION serving.cleanup_staging(integer) TO dbx_maintenance")
    print("Provisioned dbx_db: tables, functions, publisher and maintenance roles; secrets stored.")


if __name__ == "__main__":
    main()
