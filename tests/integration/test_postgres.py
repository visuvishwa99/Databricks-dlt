"""PostgreSQL transaction tests in an isolated disposable database, never dbx_db."""

import os
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

pytestmark = pytest.mark.skipif(os.getenv("RUN_POSTGRES_TESTS") != "1", reason="PostgreSQL tests disabled")


@pytest.fixture
def db():
    psycopg = pytest.importorskip("psycopg")
    from psycopg import sql
    options = {"host": os.getenv("PGHOST", "localhost"), "port": os.getenv("PGPORT", "5432"),
               "user": os.getenv("PGUSER", "postgres"), "password": os.environ["PGPASSWORD"]}
    name = "dbx_test_" + uuid.uuid4().hex
    with psycopg.connect(**options, dbname="postgres", autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
        try:
            with psycopg.connect(**options, dbname=name, autocommit=True) as c:
                c.execute((Path(__file__).resolve().parents[2] / "sql/postgres_setup.sql").read_text())
                yield c
        finally:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))


def stage(c, run="r1", student="s1", school="School", attempt="a1"):
    c.execute("INSERT INTO stg.student_progress(run_id,attempt_id,product_name,student_id,school_id,total_completions) VALUES(%s,%s,'alpha',%s,%s,1)",
              (run, attempt, student, school))


def publish(c, run="r1", expected=1, when=None, deletes=False, fraction=0.05):
    c.execute("SELECT serving.publish_progress(%s,'a1',%s,%s,%s,%s)",
              (run, expected, when or datetime.now(UTC), deletes, fraction))


def test_publish_replay_and_reconcile(db):
    stage(db)
    stage(db)  # identical JDBC retries collapse
    publish(db)
    publish(db)
    assert db.execute("SELECT count(*) FROM serving.publish_audit").fetchone()[0] == 1
    assert db.execute("SELECT serving.reconcile_progress('r1')").fetchone()[0] == 1


@pytest.mark.parametrize("school", ["school", "School ", None])
def test_exact_changes(db, school):
    stage(db)
    publish(db)
    before = db.execute("SELECT updated_at FROM serving.student_progress").fetchone()[0]
    stage(db, run="r2", school=school)
    publish(db, run="r2")
    assert db.execute("SELECT school_id FROM serving.student_progress").fetchone()[0] == school
    assert db.execute("SELECT updated_at FROM serving.student_progress").fetchone()[0] > before


def test_unchanged_rows_not_updated(db):
    stage(db)
    publish(db)
    before = db.execute("SELECT updated_at FROM serving.student_progress").fetchone()[0]
    stage(db, run="r2")
    publish(db, run="r2")
    assert db.execute("SELECT updated_at FROM serving.student_progress").fetchone()[0] == before


@pytest.mark.parametrize("kind", ["empty", "partial", "conflicting", "future", "stale", "delete", "fraction"])
def test_guards_rollback(db, kind):
    import psycopg
    stage(db)
    baseline = datetime.now(UTC) - timedelta(seconds=10)
    publish(db, when=baseline)
    stage(db, run="r2", student="s2")
    expected, when, deletes, fraction = 1, datetime.now(UTC), False, 0.05
    if kind == "empty":
        expected = 0
    elif kind == "partial":
        expected = 2
    elif kind == "conflicting":
        stage(db, run="r2", student="s2", school="Other")
        expected = 2
    elif kind == "future":
        when += timedelta(hours=1)
    elif kind == "stale":
        when = baseline
    elif kind == "fraction":
        deletes = True
    with pytest.raises(psycopg.Error):
        publish(db, "r2", expected, when, deletes, fraction)
    assert db.execute("SELECT student_id FROM serving.student_progress").fetchone()[0] == "s1"
    assert db.execute("SELECT count(*) FROM serving.publish_audit").fetchone()[0] == 1


def test_bounded_deletion(db):
    stage(db)
    stage(db, student="s2")
    publish(db, expected=2)
    stage(db, run="r2")
    publish(db, "r2", deletes=True, fraction=0.5)
    assert db.execute("SELECT count(*) FROM serving.student_progress").fetchone()[0] == 1


def test_retention_preserves_latest_unverified_and_audit(db):
    for run in ("r1", "r2", "r3"):
        stage(db, run=run)
        publish(db, run)
        if run != "r2":
            db.execute("SELECT serving.reconcile_progress(%s)", (run,))
    stage(db, run="orphan")
    db.execute("UPDATE stg.student_progress SET staged_at=now()-interval '8 days'")
    assert db.execute("SELECT serving.cleanup_staging(7)").fetchone()[0] == 2
    assert db.execute("SELECT run_id FROM stg.student_progress ORDER BY run_id").fetchall() == [("r2",), ("r3",)]
    assert db.execute("SELECT count(*) FROM serving.publish_audit").fetchone()[0] == 3
    publish(db, "r1")  # replay still safe after old staging is removed


def test_reconciliation_detects_mutation(db):
    import psycopg
    stage(db)
    publish(db)
    db.execute("UPDATE serving.student_progress SET total_completions=9")
    with pytest.raises(psycopg.Error, match="differs"):
        db.execute("SELECT serving.reconcile_progress('r1')")
    assert db.execute("SELECT reconciled_at FROM serving.publish_audit").fetchone()[0] is None


def test_competing_same_run_is_idempotent(db):
    import psycopg
    stage(db)
    when = datetime.now(UTC)

    def worker():
        with psycopg.connect(host=os.getenv("PGHOST", "localhost"), port=os.getenv("PGPORT", "5432"),
                             user=os.getenv("PGUSER", "postgres"), password=os.environ["PGPASSWORD"],
                             dbname=db.info.dbname) as c:
            publish(c, when=when)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(worker) for _ in range(2)]
        for future in futures:
            future.result(timeout=15)
    assert db.execute("SELECT count(*) FROM serving.publish_audit").fetchone()[0] == 1


def test_caller_temp_table_is_never_trusted(db):
    import psycopg
    stage(db)
    db.execute("CREATE TEMP TABLE progress_candidate (dummy integer)")
    with pytest.raises(psycopg.errors.DuplicateTable):
        publish(db)
    assert db.execute("SELECT count(*) FROM serving.publish_audit").fetchone()[0] == 0
