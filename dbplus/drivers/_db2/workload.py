"""DB2 WorkloadIntrospector implementation.

:class:`DB2WorkloadMixin` surfaces three live views of a running DB2 LUW
instance:

- ``workload_snapshot`` — top statements from the package cache via
  ``MON_GET_PKG_CACHE_STMT``
- ``locks_current``     — currently-held locks via ``MON_GET_LOCKS``,
  joined to ``SYSCAT.TABLES`` so object names are resolved where possible
- ``deadlocks_recent``  — rows from the ``LOCK_EVENT`` table written by a
  ``CREATE EVENT MONITOR FOR LOCKING`` (if one exists). When no such
  monitor is configured, this raises :class:`DBError` with a pointer to
  the DB2 doc rather than silently returning an empty list.

The MON_GET_* functions require SQLADM or DBADM; the caller is assumed to
have been granted one. The mixin never writes, reorgs, or resets counters
— pure SELECT from monitor views and one system catalog.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from dbplus.drivers._db2 import get_queries
from dbplus.drivers.workload import (
    DeadlockEvent,
    LockInfo,
    PackageCacheStmt,
)
from dbplus.errors import DBError
from dbplus.helpers import _validate_identifier
from dbplus.Statement import Statement


def _iso(ts: Any) -> Optional[str]:
    if ts is None:
        return None
    iso = getattr(ts, "isoformat", None)
    return iso() if callable(iso) else str(ts)


def _int(v: Any) -> Optional[int]:
    return int(v) if v is not None else None


def _hex(v: Any) -> Optional[str]:
    """Render a DB2 VARCHAR FOR BIT DATA value as a hex string.

    ``LOCK_ATTRIBUTES`` is an 8-byte bit field; showing it as hex keeps it
    readable in JSON and lossless to consumers who want to decode it.
    """
    if v is None:
        return None
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).hex().upper()
    return str(v)


class DB2WorkloadMixin:
    """Mixin that implements :class:`WorkloadIntrospector` for the DB2 driver."""

    def _run_query(
        self, query_name: str, **params: Any
    ) -> List[Dict[str, Any]]:
        q = getattr(get_queries(), query_name)
        stmt = Statement(self)
        stmt.execute(q.sql, **params)
        return list(stmt.iterate())

    def workload_snapshot(
        self, minutes: Optional[int] = None, top: int = 25
    ) -> List[PackageCacheStmt]:
        if top is not None and top <= 0:
            raise DBError(f"top must be positive, got {top}")
        rows = self._run_query("workload_package_cache", minutes=minutes)
        limited = rows[:top] if top is not None else rows
        return [
            PackageCacheStmt(
                stmt_text=r["stmt_text"],
                num_executions=_int(r["num_executions"]),
                total_cpu_time_us=_int(r["total_cpu_time_us"]),
                total_exec_time_ms=_int(r["total_exec_time_ms"]),
                rows_read=_int(r["rows_read"]),
                rows_returned=_int(r["rows_returned"]),
                total_wait_time_ms=_int(r["total_wait_time_ms"]),
                last_updated=_iso(r["last_updated"]),
            )
            for r in limited
        ]

    def locks_current(self) -> List[LockInfo]:
        rows = self._run_query("locks_current")
        return [
            LockInfo(
                application_handle=_int(r["application_handle"]),
                lock_name=r["lock_name"],
                lock_object_type=r["lock_object_type"],
                lock_mode=r["lock_mode"],
                lock_status=r["lock_status"],
                lock_attributes=_hex(r["lock_attributes"]),
                tbsp_id=_int(r["tbsp_id"]),
                tab_file_id=_int(r["tab_file_id"]),
                tabschema=r["tabschema"],
                tabname=r["tabname"],
            )
            for r in rows
        ]

    def deadlocks_recent(
        self, since: Optional[str] = None
    ) -> List[DeadlockEvent]:
        schema = self._find_lock_event_schema()
        if schema is None:
            raise DBError(
                "No LOCK_EVENT table found. Create a locking event monitor "
                "first, e.g.: "
                "CREATE EVENT MONITOR dlmon FOR LOCKING WRITE TO TABLE; "
                "SET EVENT MONITOR dlmon STATE = 1;"
            )
        _validate_identifier(schema)
        sql = (
            "SELECT "
            "    EVENT_ID AS event_id, "
            "    EVENT_TYPE AS event_type, "
            "    EVENT_TIMESTAMP AS event_timestamp, "
            "    DL_CONNS AS dl_conns, "
            "    ROLLED_BACK_PARTICIPANT_NO AS rolled_back_participant_no "
            f"FROM {schema}.LOCK_EVENT "
            "WHERE EVENT_TYPE = 'DEADLOCK' "
            "  AND (CAST(:since AS TIMESTAMP) IS NULL "
            "       OR EVENT_TIMESTAMP >= CAST(:since AS TIMESTAMP)) "
            "ORDER BY EVENT_TIMESTAMP DESC"
        )
        stmt = Statement(self)
        stmt.execute(sql, since=since)
        rows = list(stmt.iterate())
        return [
            DeadlockEvent(
                event_id=_int(r["event_id"]),
                event_type=r["event_type"],
                event_timestamp=_iso(r["event_timestamp"]),
                dl_conns=_int(r["dl_conns"]),
                rolled_back_participant_no=_int(
                    r["rolled_back_participant_no"]
                ),
            )
            for r in rows
        ]

    def _find_lock_event_schema(self) -> Optional[str]:
        rows = self._run_query("find_lock_event_table")
        return rows[0]["schema"] if rows else None
