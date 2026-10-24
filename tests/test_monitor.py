from datetime import UTC, datetime, timedelta

import pytest

pytest.importorskip("psycopg")
from monitor_postgres import health


def test_monitor_detects_no_run_stale_pending_and_future():
    now = datetime.now(UTC)
    assert health(None, 0, now, 60).startswith("ALERT")
    assert health(now-timedelta(hours=2), 0, now, 60).startswith("ALERT")
    assert health(now, 1, now, 60).startswith("ALERT")
    assert health(now+timedelta(seconds=1), 0, now, 60).startswith("ALERT")
    assert health(now-timedelta(minutes=1), 0, now, 60) == "OK"
