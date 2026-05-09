"""DB2 Explainer implementation.

:class:`DB2ExplainMixin` runs ``EXPLAIN PLAN FOR <sql>`` in the current
connection and then reads back the nine ``EXPLAIN_*`` tables, nesting them
into a single dict shaped like the output of the stand-alone
``explain_export.py`` script:

    instance
      statements
        operators
          arguments
          predicates
        streams
        objects
        diagnostics
          tokens

String values are sanitised to remove XML/JSON-illegal control characters
(including NUL) because DB2's EXPLAIN tables can contain raw CLOB bytes
that silently truncate in downstream string handling.

The mixin assumes the EXPLAIN_* tables exist in the current schema. If
they don't, the ``EXPLAIN PLAN FOR`` call itself will fail with a clear
``SQLSTATE 42704`` and the caller gets that error back unchanged.
"""
from __future__ import annotations

import re
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Dict, List, Optional

from dbplus.drivers._db2 import get_queries
from dbplus.errors import DBError
from dbplus.Statement import Statement

_CTL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")

_INSTANCE_KEY = (
    "explain_requester",
    "explain_time",
    "source_name",
    "source_schema",
    "source_version",
)
_STMT_KEY = _INSTANCE_KEY + ("explain_level", "stmtno", "sectno")
_OPERATOR_KEY = _STMT_KEY + ("operator_id",)
_DIAG_KEY = _STMT_KEY + ("diagnostic_id",)


def _sanitize(value: Any) -> Any:
    if isinstance(value, (bytes, bytearray)):
        value = value.replace(b"\x00", b" ").decode("utf-8", errors="replace")
    if isinstance(value, str):
        return _CTL.sub(" ", value)
    return value


def _to_jsonable(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        return _sanitize(value)
    if isinstance(value, str):
        return _sanitize(value)
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    return value


def _row_to_dict(row: Dict[str, Any]) -> Dict[str, Any]:
    return {k: _to_jsonable(v) for k, v in row.items()}


def _key(row: Dict[str, Any], fields: tuple) -> tuple:
    return tuple(row[f] for f in fields)


def _group_by(rows: List[Dict[str, Any]], fields: tuple) -> Dict[tuple, List[Dict[str, Any]]]:
    groups: Dict[tuple, List[Dict[str, Any]]] = {}
    for row in rows:
        groups.setdefault(_key(row, fields), []).append(row)
    return groups


class DB2ExplainMixin:
    """Mixin that implements :class:`Explainer` for the DB2 driver."""

    def explain(self, sql: str) -> Dict[str, Any]:
        """Return the nested access plan for ``sql``.

        The inner statement is not executed; only its plan is recorded by
        ``EXPLAIN PLAN FOR``. The EXPLAIN tables themselves are written
        to (that is the nature of the feature); we never delete those
        rows — they remain for ad-hoc DBA inspection.
        """
        self._run_explain_plan(sql)
        return self._read_latest_explain()

    def _run_explain_plan(self, sql: str) -> None:
        stmt = Statement(self)
        try:
            stmt.execute(f"EXPLAIN PLAN FOR {sql}")
        except DBError:
            raise
        except Exception as exc:
            raise DBError(f"EXPLAIN PLAN FOR failed: {exc}") from exc

    def _fetch(self, query_name: str) -> List[Dict[str, Any]]:
        q = getattr(get_queries(), query_name)
        stmt = Statement(self)
        stmt.execute(q.sql)
        return [_row_to_dict(r) for r in stmt.iterate()]

    def _read_latest_explain(self) -> Dict[str, Any]:
        instances = self._fetch("explain_latest_instance")
        if not instances:
            raise DBError(
                "EXPLAIN PLAN FOR produced no EXPLAIN_INSTANCE row — "
                "are the EXPLAIN_* tables created in the current schema?"
            )

        statements = self._fetch("explain_latest_statements")
        operators = self._fetch("explain_latest_operators")
        arguments = self._fetch("explain_latest_arguments")
        predicates = self._fetch("explain_latest_predicates")
        streams = self._fetch("explain_latest_streams")
        objects = self._fetch("explain_latest_objects")
        diagnostics = self._fetch_optional("explain_latest_diagnostics")
        diag_data = self._fetch_optional("explain_latest_diagnostic_data")

        stmts_by_instance = _group_by(statements, _INSTANCE_KEY)
        ops_by_stmt = _group_by(operators, _STMT_KEY)
        args_by_op = _group_by(arguments, _OPERATOR_KEY)
        preds_by_op = _group_by(predicates, _OPERATOR_KEY)
        streams_by_stmt = _group_by(streams, _STMT_KEY)
        objects_by_stmt = _group_by(objects, _STMT_KEY)
        diags_by_stmt = _group_by(diagnostics, _STMT_KEY)
        diag_data_by_diag = _group_by(diag_data, _DIAG_KEY)

        # There should be exactly one instance — filter is MAX(EXPLAIN_TIME).
        # If DB2 logged multiple rows in the same tick, pick the first and
        # keep the others out of the response; the nested tree is still
        # correct for the one we chose.
        inst = instances[0]
        inst_key = _key(inst, _INSTANCE_KEY)
        inst_stmts: List[Dict[str, Any]] = []

        for stmt_row in stmts_by_instance.get(inst_key, []):
            stmt_key = _key(stmt_row, _STMT_KEY)

            stmt_ops: List[Dict[str, Any]] = []
            for op in ops_by_stmt.get(stmt_key, []):
                op_key = _key(op, _OPERATOR_KEY)
                op_copy = dict(op)
                op_copy["arguments"] = args_by_op.get(op_key, [])
                op_copy["predicates"] = preds_by_op.get(op_key, [])
                stmt_ops.append(op_copy)

            stmt_diags: List[Dict[str, Any]] = []
            for diag in diags_by_stmt.get(stmt_key, []):
                diag_key_val = _key(diag, _DIAG_KEY)
                diag_copy = dict(diag)
                diag_copy["tokens"] = diag_data_by_diag.get(diag_key_val, [])
                stmt_diags.append(diag_copy)

            stmt_copy = dict(stmt_row)
            stmt_copy["operators"] = stmt_ops
            stmt_copy["streams"] = streams_by_stmt.get(stmt_key, [])
            stmt_copy["objects"] = objects_by_stmt.get(stmt_key, [])
            stmt_copy["diagnostics"] = stmt_diags
            inst_stmts.append(stmt_copy)

        result = dict(inst)
        result["statements"] = inst_stmts
        return result

    def _fetch_optional(self, query_name: str) -> List[Dict[str, Any]]:
        """Like :meth:`_fetch` but tolerant of missing diagnostic tables.

        EXPLAIN_DIAGNOSTIC(_DATA) was introduced in DB2 9.5; on older
        servers, or when a DBA has stripped the optional tables, the
        SELECT raises SQLSTATE 42704. We swallow that and return no
        rows so the rest of the plan is still delivered.
        """
        try:
            return self._fetch(query_name)
        except DBError:
            return []
