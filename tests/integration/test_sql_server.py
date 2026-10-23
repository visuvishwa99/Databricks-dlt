"""Real SQL Server failure tests. Only run against an explicitly enabled disposable DB."""

import os
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(os.getenv("RUN_SQLSERVER_TESTS") != "1", reason="SQL Server integration disabled")


@pytest.fixture
def database():
    pymssql = pytest.importorskip("pymssql")
    options = {"server": os.environ["SQLSERVER_HOST"], "user": "sa",
               "password": os.environ["SQLSERVER_PASSWORD"], "login_timeout": 5, "autocommit": True}
    for attempt in range(30):
        try:
            admin = pymssql.connect(**options)
            break
        except pymssql.OperationalError:
            if attempt == 29:
                raise
            time.sleep(2)
    name = "lexia_ci_" + uuid.uuid4().hex
    admin.cursor().execute(f"CREATE DATABASE [{name}]")
    connection = pymssql.connect(database=name, **options)
    try:
        script = (Path(__file__).resolve().parents[2] / "sql/sql_server_setup.sql").read_text(encoding="utf-8-sig")
        for batch in re.split(r"(?im)^GO\s*$", script):
            if batch.strip():
                connection.cursor().execute(batch)
        yield connection
    finally:
        connection.close()
        admin.cursor().execute(f"ALTER DATABASE [{name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
        admin.cursor().execute(f"DROP DATABASE [{name}]")
        admin.close()


def stage(db, run="r1", attempt="a1", student="s1", completions=1):
    db.cursor().execute(
        "INSERT stg.student_progress_v2 (run_id,attempt_id,product_name,student_id,total_completions) "
        "VALUES (%s,%s,'alpha',%s,%s)", (run, attempt, student, completions),
    )


def publish(db, run="r1", attempt="a1", count=1, when=None, deletes=False, fraction=0.05):
    cursor = db.cursor()
    cursor.execute("EXEC dbo.usp_publish_student_progress_v2 %s,%s,%s,%s,%s,%s",
                   (run, attempt, count, when or datetime.now(UTC).replace(tzinfo=None), int(deletes), fraction))
    while cursor.nextset():
        pass


def scalar(db, sql):
    cursor = db.cursor()
    cursor.execute(sql)
    return cursor.fetchone()[0]


def test_commit_and_replay_are_idempotent(database):
    stage(database)
    publish(database)
    publish(database, attempt="client_retry_with_no_rows")
    assert scalar(database, "SELECT COUNT(*) FROM dbo.student_progress") == 1
    assert scalar(database, "SELECT COUNT(*) FROM dbo.student_progress_write_audit_v2") == 1


def test_identical_executor_retries_are_deduplicated(database):
    stage(database)
    stage(database)
    publish(database)
    assert scalar(database, "SELECT staged_rows FROM dbo.student_progress_write_audit_v2") == 1


@pytest.mark.parametrize("failure", ["partial", "conflicting", "empty"])
def test_bad_staging_rolls_back(database, failure):
    import pymssql
    if failure != "empty":
        stage(database)
    if failure == "conflicting":
        stage(database, completions=9)
    with pytest.raises(pymssql.DatabaseError):
        publish(database, count=0 if failure == "empty" else 2)
    assert scalar(database, "SELECT COUNT(*) FROM dbo.student_progress") == 0
    assert scalar(database, "SELECT COUNT(*) FROM dbo.student_progress_write_audit_v2") == 0


def test_stale_snapshot_is_rejected(database):
    import pymssql
    now = datetime.now(UTC).replace(tzinfo=None)
    stage(database)
    publish(database, when=now)
    stage(database, run="r2", attempt="a2", completions=99)
    with pytest.raises(pymssql.DatabaseError):
        publish(database, run="r2", attempt="a2", when=now - timedelta(minutes=1))
    assert scalar(database, "SELECT total_completions FROM dbo.student_progress") == 1


def test_delete_requires_opt_in_and_threshold(database):
    import pymssql
    now = datetime.now(UTC).replace(tzinfo=None)
    stage(database, student="s1")
    stage(database, student="s2")
    publish(database, count=2, when=now - timedelta(seconds=10))
    stage(database, run="r2", attempt="a2", student="s1")
    with pytest.raises(pymssql.DatabaseError):
        publish(database, run="r2", attempt="a2", when=now)
    with pytest.raises(pymssql.DatabaseError):
        publish(database, run="r2", attempt="a2", when=now, deletes=True)
    assert scalar(database, "SELECT COUNT(*) FROM dbo.student_progress") == 2
    publish(database, run="r2", attempt="a2", when=now, deletes=True, fraction=0.5)
    assert scalar(database, "SELECT COUNT(*) FROM dbo.student_progress") == 1


def test_newer_snapshot_wins_concurrent_publishers(database):
    import pymssql
    now = datetime.now(UTC).replace(tzinfo=None)
    stage(database, run="older", attempt="old", completions=1)
    stage(database, run="newer", attempt="new", completions=2)
    name = scalar(database, "SELECT DB_NAME()")

    def apply(run, attempt, timestamp):
        connection = pymssql.connect(server=os.environ["SQLSERVER_HOST"], user="sa",
                                     password=os.environ["SQLSERVER_PASSWORD"], database=name,
                                     autocommit=True)
        try:
            publish(connection, run=run, attempt=attempt, when=timestamp)
        except pymssql.DatabaseError as error:
            # Only the old writer may lose the race to a committed newer snapshot.
            if run != "older" or "newer snapshot" not in str(error):
                raise
        finally:
            connection.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(apply, "older", "old", now - timedelta(seconds=10)),
                   pool.submit(apply, "newer", "new", now)]
        for future in futures:
            future.result()
    assert scalar(database, "SELECT total_completions FROM dbo.student_progress") == 2
