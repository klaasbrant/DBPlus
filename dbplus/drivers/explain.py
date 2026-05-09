"""Explainer protocol for the DBPlus MCP server.

A driver implements this protocol if it can produce an access plan for a
SQL statement without executing it. The :meth:`explain` method returns a
plain nested ``dict`` (JSON-ready) rather than a dataclass because the
shape is inherently driver-specific: DB2 exposes the ``EXPLAIN_*`` tables,
Postgres uses ``EXPLAIN (FORMAT JSON)``, Oracle has ``DBMS_XPLAN``, etc.
The MCP layer serializes the dict as-is and the calling agent reads it.

Callers gate on ``isinstance(driver, Explainer)`` at wire-up time.
"""
from __future__ import annotations

from typing import Any, Dict, Protocol, runtime_checkable


@runtime_checkable
class Explainer(Protocol):
    """Structural protocol: a driver with ``explain(sql)``."""

    def explain(self, sql: str) -> Dict[str, Any]:
        """Return the access plan for ``sql`` as a nested ``dict``.

        The statement is **not** executed — only its plan is captured.
        The concrete shape is driver-specific.
        """
        ...
