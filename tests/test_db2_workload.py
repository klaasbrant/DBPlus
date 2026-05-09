"""Integration tests for the DB2 WorkloadIntrospector implementation.

These tests require a live DB2 LUW connection, configured via the
DATABASE_URL environment variable. The connecting user must have SQLADM
or DBADM to read the MON_GET_* functions.

Run with (bash):
    DATABASE_URL="DB2://user:pwd@host:port/db" pytest tests/test_db2_workload.py

``deadlocks_recent`` requires a locking event monitor. If no LOCK_EVENT
table exists we expect a clean DBError with setup instructions, not a
crash.
"""
from __future__ import annotations

import pytest

from dbplus import Database
from dbplus.drivers.workload import (
    DeadlockEvent,
    LockInfo,
    PackageCacheStmt,
    WorkloadIntrospector,
)
from dbplus.errors import DBError
from tests.conftest import DATABASE_URL


def _detect_db2_workload() -> bool:
    try:
        db = Database(DATABASE_URL)
    except Exception:
        return False
    try:
        if db.get_driver().get_name() != "DB2":
            return False
        try:
            db.query(
                "SELECT 1 FROM TABLE(MON_GET_PKG_CACHE_STMT(NULL, NULL, NULL, -2)) "
                "FETCH FIRST 1 ROW ONLY"
            ).all()
            return True
        except Exception:
            return False
    finally:
        db.close()


pytestmark = pytest.mark.skipif(
    not _detect_db2_workload(),
    reason=(
        "DB2 workload tests require DATABASE_URL pointing to DB2 LUW with "
        "SQLADM/DBADM privilege to read MON_GET_PKG_CACHE_STMT"
    ),
)


@pytest.fixture(scope="module")
def db2():
    database = Database(DATABASE_URL)
    yield database
    database.close()


class TestWorkloadProtocol:
    def test_driver_is_workload_introspector(self, db2):
        assert isinstance(db2.get_driver(), WorkloadIntrospector)

    def test_database_proxies_workload_methods(self, db2):
        assert callable(db2.workload_snapshot)
        assert callable(db2.locks_current)
        assert callable(db2.deadlocks_recent)


class TestWorkloadSnapshot:
    def test_returns_list_of_dataclasses(self, db2):
        rows = db2.workload_snapshot(top=5)
        assert isinstance(rows, list)
        for r in rows:
            assert isinstance(r, PackageCacheStmt)

    def test_respects_top_limit(self, db2):
        rows = db2.workload_snapshot(top=3)
        assert len(rows) <= 3

    def test_minutes_filter_narrows_or_equals(self, db2):
        unfiltered = db2.workload_snapshot(top=100)
        recent = db2.workload_snapshot(minutes=1, top=100)
        assert len(recent) <= len(unfiltered)

    def test_rejects_non_positive_top(self, db2):
        with pytest.raises(DBError):
            db2.workload_snapshot(top=0)


class TestLocksCurrent:
    def test_returns_list_of_lockinfo(self, db2):
        locks = db2.locks_current()
        assert isinstance(locks, list)
        for l in locks:
            assert isinstance(l, LockInfo)

    def test_each_lock_has_name_and_mode(self, db2):
        locks = db2.locks_current()
        # Every lock that exists has a name; mode may be NULL briefly
        # during transition but the field should still be present.
        for l in locks:
            assert hasattr(l, "lock_name")
            assert hasattr(l, "lock_mode")


class TestDeadlocksRecent:
    def test_returns_list_or_raises_setup_error(self, db2):
        try:
            events = db2.deadlocks_recent()
        except DBError as exc:
            # No event monitor configured — that's the expected path on a
            # fresh database, and the error should tell the DBA what to do.
            assert "LOCK_EVENT" in str(exc)
            return
        assert isinstance(events, list)
        for e in events:
            assert isinstance(e, DeadlockEvent)

    def test_since_filter_narrows_or_equals(self, db2):
        try:
            all_events = db2.deadlocks_recent()
        except DBError:
            pytest.skip("No LOCK_EVENT table configured")
        filtered = db2.deadlocks_recent(since="2999-01-01T00:00:00")
        assert len(filtered) <= len(all_events)
