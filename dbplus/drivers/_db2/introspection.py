"""DB2 Introspector implementation, split into a mixin.

:class:`DB2IntrospectionMixin` supplies the
:class:`~dbplus.drivers.introspection.Introspector` protocol methods on top
of the DB2 base driver. It relies on the driver providing:

- ``self._conn`` — the ibm_db connection (used only for ``server_info``)
- the standard ``execute()``/``iterate()`` plumbing via :class:`Statement`

All SQL lives in ``_db2/queries/DB2.sql`` and is loaded lazily via
:func:`dbplus.drivers._db2.get_queries`.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from dbplus.drivers._db2 import get_queries
from dbplus.drivers.introspection import (
    ColumnInfo,
    ForeignKeyInfo,
    IndexColumn,
    IndexInfo,
    QueryValidation,
    RoutineInfo,
    SchemaInfo,
    SearchResult,
    ServerInfo,
    StatsInfo,
    TableDetail,
    TableInfo,
    TriggerInfo,
    ViewInfo,
)
from dbplus.errors import DBError
from dbplus.helpers import _validate_identifier
from dbplus.Statement import Statement

_TABLE_TYPE_TO_STR = {
    "T": "TABLE",
    "V": "VIEW",
    "A": "ALIAS",
    "S": "MQT",
    "N": "NICKNAME",
    "G": "GLOBAL TEMPORARY",
    "H": "HIERARCHY",
    "U": "TYPED TABLE",
    "W": "TYPED VIEW",
    "L": "DETACHED",
}
_STR_TO_TABLE_TYPE = {v: k for k, v in _TABLE_TYPE_TO_STR.items()}

_ROUTINE_TYPE_TO_STR = {"P": "PROCEDURE", "F": "FUNCTION", "M": "METHOD"}
_TRIGGER_EVENT_TO_STR = {"I": "INSERT", "U": "UPDATE", "D": "DELETE"}
_TRIGGER_TIME_TO_STR = {"A": "AFTER", "B": "BEFORE", "I": "INSTEAD OF"}
_FK_RULE_TO_STR = {
    "A": "NO ACTION",
    "C": "CASCADE",
    "N": "SET NULL",
    "R": "RESTRICT",
}
_INDEX_ORDER_TO_STR = {"A": "ASC", "D": "DESC", "I": "INCLUDE"}


class DB2IntrospectionMixin:
    """Mixin that implements :class:`Introspector` for the DB2 driver."""

    def _run_query(self, query_name: str, **params: Any) -> List[Dict[str, Any]]:
        q = getattr(get_queries(), query_name)
        stmt = Statement(self)
        stmt.execute(q.sql, **params)
        return list(stmt.iterate())

    @staticmethod
    def _iso(dt: Any) -> Optional[str]:
        if dt is None:
            return None
        iso = getattr(dt, "isoformat", None)
        return iso() if callable(iso) else str(dt)

    def list_schemas(self) -> List[SchemaInfo]:
        rows = self._run_query("list_schemas")
        return [
            SchemaInfo(name=r["name"], owner=r["owner"], remarks=r["remarks"])
            for r in rows
        ]

    def list_tables(
        self, schema: Optional[str] = None, kind: str = "TABLE"
    ) -> List[TableInfo]:
        kind_code: Optional[str]
        if kind is None:
            kind_code = None
        else:
            kind_code = _STR_TO_TABLE_TYPE.get(kind.upper(), kind)
        rows = self._run_query("list_tables", schema=schema, kind=kind_code)
        return [
            TableInfo(
                schema=r["schema"],
                name=r["name"],
                type=_TABLE_TYPE_TO_STR.get(r["type"], r["type"]),
                remarks=r["remarks"],
            )
            for r in rows
        ]

    def describe_table(self, schema: str, table: str) -> TableDetail:
        header = self._run_query(
            "describe_table_header", schema=schema, table=table
        )
        if not header:
            raise DBError(f"Table {schema}.{table} not found")
        h = header[0]
        col_rows = self._run_query(
            "describe_table_columns", schema=schema, table=table
        )
        columns = [
            ColumnInfo(
                name=r["name"],
                type=r["type"],
                nullable=bool(r["nullable"]),
                ordinal=r["ordinal"],
                length=r["length"],
                scale=r["scale"],
                default=r["col_default"],
                remarks=r["remarks"],
            )
            for r in col_rows
        ]
        pk_rows = self._run_query("describe_table_pk", schema=schema, table=table)
        primary_key = [r["column_name"] for r in pk_rows]
        return TableDetail(
            schema=h["schema"],
            name=h["name"],
            type=_TABLE_TYPE_TO_STR.get(h["type"], h["type"]),
            columns=columns,
            primary_key=primary_key,
            remarks=h["remarks"],
        )

    def list_indexes(self, schema: str, table: str) -> List[IndexInfo]:
        rows = self._run_query("list_index_columns", schema=schema, table=table)
        by_index: Dict[tuple, IndexInfo] = {}
        for r in rows:
            key = (r["index_schema"], r["index_name"])
            idx = by_index.get(key)
            if idx is None:
                idx = IndexInfo(
                    name=r["index_name"],
                    schema=r["index_schema"],
                    table_schema=r["table_schema"],
                    table_name=r["table_name"],
                    unique=bool(r["is_unique"]),
                    columns=[],
                    type=r["index_type"],
                )
                by_index[key] = idx
            idx.columns.append(
                IndexColumn(
                    name=r["column_name"],
                    ordinal=r["ordinal"],
                    ordering=_INDEX_ORDER_TO_STR.get(
                        r["ordering"], r["ordering"]
                    ),
                )
            )
        return list(by_index.values())

    def list_foreign_keys(
        self, schema: str, table: str
    ) -> List[ForeignKeyInfo]:
        headers = self._run_query(
            "list_foreign_keys_header", schema=schema, table=table
        )
        cols = self._run_query(
            "list_foreign_keys_columns", schema=schema, table=table
        )
        cols_by_name: Dict[str, List[Dict[str, Any]]] = {}
        for c in cols:
            cols_by_name.setdefault(c["constraint_name"], []).append(c)
        result: List[ForeignKeyInfo] = []
        for h in headers:
            cc = cols_by_name.get(h["name"], [])
            result.append(
                ForeignKeyInfo(
                    name=h["name"],
                    schema=h["schema"],
                    table=h["table_name"],
                    columns=[c["column_name"] for c in cc],
                    ref_schema=h["ref_schema"],
                    ref_table=h["ref_table"],
                    ref_columns=[c["ref_column_name"] for c in cc],
                    on_delete=_FK_RULE_TO_STR.get(
                        h["delete_rule"], h["delete_rule"]
                    ),
                    on_update=_FK_RULE_TO_STR.get(
                        h["update_rule"], h["update_rule"]
                    ),
                )
            )
        return result

    def get_view(self, schema: str, view: str) -> ViewInfo:
        rows = self._run_query("get_view", schema=schema, view=view)
        if not rows:
            raise DBError(f"View {schema}.{view} not found")
        r = rows[0]
        return ViewInfo(
            schema=r["schema"],
            name=r["name"],
            definition=r["definition"],
            readonly=(r["readonly"] == "Y"),
        )

    def list_procedures(
        self, schema: Optional[str] = None
    ) -> List[RoutineInfo]:
        rows = self._run_query("list_procedures", schema=schema)
        return [
            RoutineInfo(
                schema=r["schema"],
                name=r["name"],
                type=_ROUTINE_TYPE_TO_STR.get(r["type"], r["type"]),
                language=r["language"],
                remarks=r["remarks"],
            )
            for r in rows
        ]

    def list_triggers(
        self, schema: Optional[str] = None
    ) -> List[TriggerInfo]:
        rows = self._run_query("list_triggers", schema=schema)
        return [
            TriggerInfo(
                schema=r["schema"],
                name=r["name"],
                table_schema=r["table_schema"],
                table_name=r["table_name"],
                event=_TRIGGER_EVENT_TO_STR.get(r["event"], r["event"]),
                timing=_TRIGGER_TIME_TO_STR.get(r["timing"], r["timing"]),
                definition=r["definition"],
            )
            for r in rows
        ]

    def get_table_stats(self, schema: str, table: str) -> StatsInfo:
        rows = self._run_query("get_table_stats", schema=schema, table=table)
        if not rows:
            raise DBError(f"Table {schema}.{table} not found")
        r = rows[0]
        return StatsInfo(
            schema=r["schema"],
            name=r["name"],
            row_count=r["row_count"] if r["row_count"] not in (None, -1) else None,
            size_bytes=r["size_bytes"],
            last_analyzed=self._iso(r["last_analyzed"]),
        )

    def list_table_stats(self, schema: str) -> List[StatsInfo]:
        rows = self._run_query("list_table_stats", schema=schema)
        return [
            StatsInfo(
                schema=r["schema"],
                name=r["name"],
                row_count=r["row_count"] if r["row_count"] not in (None, -1) else None,
                size_bytes=r["size_bytes"],
                last_analyzed=self._iso(r["last_analyzed"]),
            )
            for r in rows
        ]

    def sample_rows(self, schema: str, table: str, n: int = 5) -> List[Dict[str, Any]]:
        _validate_identifier(schema)
        _validate_identifier(table)
        n = max(1, min(n, 100))
        sql = f"SELECT * FROM {schema}.{table} FETCH FIRST {n} ROWS ONLY"
        stmt = Statement(self)
        stmt.execute(sql)
        return list(stmt.iterate())

    def validate_query(self, sql: str) -> QueryValidation:
        import ibm_db

        try:
            stmt = ibm_db.prepare(self._conn, sql)
        except Exception as exc:
            return QueryValidation(valid=False, error=str(exc))
        if stmt is False:
            error = ibm_db.stmt_errormsg() or ibm_db.conn_errormsg(self._conn) or "PREPARE failed"
            return QueryValidation(valid=False, error=error.strip())
        return QueryValidation(valid=True)

    def describe_query(self, sql: str) -> List[ColumnInfo]:
        import ibm_db

        try:
            stmt = ibm_db.prepare(self._conn, sql)
        except Exception as exc:
            raise DBError(str(exc)) from exc
        if stmt is False:
            raise DBError(ibm_db.stmt_errormsg() or "PREPARE failed")
        num_cols = ibm_db.num_fields(stmt)
        columns = []
        for i in range(num_cols):
            columns.append(
                ColumnInfo(
                    name=ibm_db.field_name(stmt, i),
                    type=ibm_db.field_type(stmt, i),
                    nullable=bool(ibm_db.field_nullable(stmt, i)),
                    ordinal=i + 1,
                    length=ibm_db.field_width(stmt, i) or None,
                    scale=ibm_db.field_scale(stmt, i) or None,
                )
            )
        return columns

    def search_objects(
        self,
        pattern: str,
        kinds: Optional[List[str]] = None,
    ) -> List[SearchResult]:
        if kinds is None:
            kinds = ["TABLE", "VIEW", "COLUMN", "PROCEDURE"]
        _kind_to_query = {
            "TABLE": "search_tables",
            "VIEW": "search_views",
            "COLUMN": "search_columns",
            "PROCEDURE": "search_procedures",
        }
        results: List[SearchResult] = []
        for kind in kinds:
            query_name = _kind_to_query.get(kind.upper())
            if query_name is None:
                continue
            for r in self._run_query(query_name, pattern=pattern):
                results.append(
                    SearchResult(
                        kind=r["kind"],
                        schema=r["schema"],
                        name=r["name"],
                        table=r["parent_name"],
                        remarks=r["remarks"],
                    )
                )
        return results

    def server_info(self) -> ServerInfo:
        import ibm_db

        info = ibm_db.server_info(self._conn)
        raw = {
            k: getattr(info, k)
            for k in dir(info)
            if not k.startswith("_") and not callable(getattr(info, k, None))
        }
        return ServerInfo(
            product=raw.get("DBMS_NAME", "DB2"),
            version=raw.get("DBMS_VER", ""),
            platform=raw.get("INST_NAME"),
            raw=raw,
        )
