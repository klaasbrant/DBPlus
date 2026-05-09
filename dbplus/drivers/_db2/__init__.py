"""Internal DB2 driver mixins and shared query library.

This package is intentionally underscore-prefixed: callers should not import
from it directly. The public entry point remains
``dbplus.drivers.DB2.DBDriver``, which composes the mixins defined here.

The shared :class:`~dbplus.QueryStore.QueryStore` is loaded lazily on first
call to any introspection/explain/workload method so users who never touch
the MCP layer pay zero parse cost.
"""
from __future__ import annotations

from pathlib import Path
from typing import Optional

from dbplus.QueryStore import QueryStore

_query_store: Optional[QueryStore] = None


def get_queries() -> QueryStore:
    """Return the shared DB2 query library, loading it on first call."""
    global _query_store
    if _query_store is None:
        sql_file = Path(__file__).parent / "queries" / "DB2.sql"
        _query_store = QueryStore(sql_file)
    return _query_store
