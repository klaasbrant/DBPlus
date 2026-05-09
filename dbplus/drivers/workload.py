"""Driver-agnostic workload/monitor contract.

A driver implements :class:`WorkloadIntrospector` if it can expose live
workload metrics — a snapshot of the package cache (``workload_snapshot``),
the current lock picture (``locks_current``), and recent deadlock events
(``deadlocks_recent``). The shape is driver-agnostic: each method returns
a list of plain dataclasses so the MCP server can serialise them to JSON
without knowing the underlying catalog.

DB2 LUW is the first implementation (see
:mod:`dbplus.drivers._db2.workload`). Postgres/MySQL/Oracle analogues can
implement the same protocol in Phase 2; the MCP server does not change.

Callers gate on ``isinstance(driver, WorkloadIntrospector)`` at wire-up
time. Drivers that don't implement it simply don't register the
workload tools.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Protocol, runtime_checkable


@dataclass
class PackageCacheStmt:
    """One row from the DBMS's package / statement cache.

    Times are stored in the units the native view reports them in — see the
    field suffix. Drivers that don't surface a given metric set it to
    ``None`` rather than zero so consumers can distinguish ``missing`` from
    ``truly zero``.
    """

    stmt_text: Optional[str]
    num_executions: Optional[int]
    total_cpu_time_us: Optional[int]
    total_exec_time_ms: Optional[int]
    rows_read: Optional[int]
    rows_returned: Optional[int]
    total_wait_time_ms: Optional[int]
    last_updated: Optional[str]  # ISO-8601 timestamp


@dataclass
class LockInfo:
    """One current lock held or requested on the database."""

    application_handle: Optional[int]
    lock_name: Optional[str]
    lock_object_type: Optional[str]
    lock_mode: Optional[str]
    lock_status: Optional[str]
    lock_attributes: Optional[str]
    tbsp_id: Optional[int]
    tab_file_id: Optional[int]
    tabschema: Optional[str]
    tabname: Optional[str]


@dataclass
class DeadlockEvent:
    """One row from the DBMS's deadlock / locking event monitor."""

    event_id: Optional[int]
    event_type: Optional[str]
    event_timestamp: Optional[str]  # ISO-8601
    dl_conns: Optional[int]
    rolled_back_participant_no: Optional[int]


@runtime_checkable
class WorkloadIntrospector(Protocol):
    """Structural protocol: a driver with live workload/monitor methods."""

    def workload_snapshot(
        self, minutes: Optional[int] = None, top: int = 25
    ) -> List[PackageCacheStmt]:
        """Return the top ``top`` statements from the package cache.

        If ``minutes`` is given, only statements whose metrics were
        updated within the last ``minutes`` minutes are returned.
        Implementations order by a reasonable "hotness" metric (DB2
        uses ``TOTAL_CPU_TIME DESC``).
        """
        ...

    def locks_current(self) -> List[LockInfo]:
        """Return every currently-held lock on the database."""
        ...

    def deadlocks_recent(
        self, since: Optional[str] = None
    ) -> List[DeadlockEvent]:
        """Return deadlock events from the driver's event monitor.

        ``since`` is an ISO-8601 timestamp; only events at or after this
        moment are returned. Drivers that require an event monitor to be
        pre-configured raise :class:`~dbplus.errors.DBError` with setup
        instructions when no monitor is present.
        """
        ...
