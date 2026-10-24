"""PostgreSQL contract; native driver transactions avoid private Spark APIs."""

import re

import psycopg


def connection_options(values, *, local=False):
    host = values["host"]
    if not re.fullmatch(r"[A-Za-z0-9.-]+", host):
        raise ValueError("Invalid PostgreSQL host")
    port = int(values.get("port", "5432"))
    database = values["database"]
    if not 1 <= port <= 65535 or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", database):
        raise ValueError("Invalid PostgreSQL port/database")
    if local and host not in ("localhost", "127.0.0.1"):
        raise ValueError("Local mode only supports loopback")
    options = {"host": host, "port": port, "dbname": database, "user": values["user"],
               "password": values["password"], "connect_timeout": 10,
               "options": "-c timezone=UTC -c statement_timeout=300000 -c lock_timeout=30000",
               "sslmode": "disable" if local else "verify-full"}
    if not local:
        options["sslrootcert"] = values["sslrootcert"]
    return options


def jdbc_options(options):
    if options["sslmode"] != "verify-full":
        raise ValueError("Cloud JDBC requires verified TLS")
    return {
        "url": f"jdbc:postgresql://{options['host']}:{options['port']}/{options['dbname']}",
        "user": options["user"], "password": options["password"],
        "driver": "org.postgresql.Driver", "sslmode": "verify-full",
        "sslrootcert": options["sslrootcert"],
        "options": "-c timezone=UTC",
    }


def publish(options, run_id, attempt_id, count, snapshot_at, *, allow_deletes=False, fraction=0.05):
    with psycopg.connect(**options) as connection:
        connection.execute("SELECT serving.publish_progress(%s,%s,%s,%s,%s,%s)",
                           (run_id, attempt_id, count, snapshot_at, allow_deletes, fraction))


def reconcile(options, run_id):
    with psycopg.connect(**options) as connection:
        return connection.execute("SELECT serving.reconcile_progress(%s)", (run_id,)).fetchone()[0]


def cleanup(options, days):
    with psycopg.connect(**options) as connection:
        return connection.execute("SELECT serving.cleanup_staging(%s)", (days,)).fetchone()[0]
