"""Integration tests for the DB2 Introspector implementation.

These tests require a live DB2 LUW connection, configured via the
DATABASE_URL environment variable. They are skipped transparently when
DATABASE_URL points to a non-DB2 backend or the server is unreachable.

Run with (bash):
    DATABASE_URL="DB2://user:pwd@host:port/db" pytest tests/test_db2_introspection.py
"""
from __future__ import annotations

import pytest

from dbplus import Database, DBError
from dbplus.drivers.introspection import (
    ColumnInfo,
    ForeignKeyInfo,
    IndexInfo,
    Introspector,
    RoutineInfo,
    SchemaInfo,
    ServerInfo,
    StatsInfo,
    TableDetail,
    TableInfo,
    TriggerInfo,
    ViewInfo,
)
from tests.conftest import DATABASE_URL


def _detect_db2() -> bool:
    try:
        db = Database(DATABASE_URL)
    except Exception:
        return False
    try:
        return db.get_driver().get_name() == "DB2"
    finally:
        db.close()


pytestmark = pytest.mark.skipif(
    not _detect_db2(),
    reason="DB2 integration tests require DATABASE_URL pointing to a DB2 LUW instance",
)


def _safe_exec(database: Database, sql: str) -> None:
    try:
        with database.transaction():
            database.execute(sql)
    except Exception:
        pass


@pytest.fixture(scope="module")
def db2():
    """Connects, provisions the test schema artifacts, yields (db, schema)."""
    database = Database(DATABASE_URL)
    schema = database.query("VALUES CURRENT SCHEMA").scalar().strip()

    # Clean any left-overs from a prior aborted run
    _safe_exec(database, "DROP VIEW DBPLUS_EMP_V")
    _safe_exec(database, "DROP TABLE DBPLUS_EMP")
    _safe_exec(database, "DROP TABLE DBPLUS_DEPT")

    with database.transaction():
        database.execute(
            """
            CREATE TABLE DBPLUS_DEPT (
                DEPT_ID   INTEGER NOT NULL,
                DEPT_NAME VARCHAR(100) NOT NULL,
                PRIMARY KEY (DEPT_ID)
            )
            """
        )
        database.execute(
            """
            CREATE TABLE DBPLUS_EMP (
                EMP_ID  INTEGER NOT NULL,
                NAME    VARCHAR(100) NOT NULL,
                SALARY  DECIMAL(10, 2),
                DEPT_ID INTEGER,
                PRIMARY KEY (EMP_ID),
                CONSTRAINT DBPLUS_FK_DEPT FOREIGN KEY (DEPT_ID)
                    REFERENCES DBPLUS_DEPT(DEPT_ID) ON DELETE CASCADE
            )
            """
        )
        database.execute(
            "CREATE INDEX DBPLUS_EMP_NAME_IDX ON DBPLUS_EMP (NAME DESC)"
        )
        database.execute(
            "CREATE VIEW DBPLUS_EMP_V AS SELECT EMP_ID, NAME FROM DBPLUS_EMP"
        )
        database.execute("INSERT INTO DBPLUS_DEPT VALUES (1, 'Engineering')")
        database.execute(
            "INSERT INTO DBPLUS_EMP VALUES (1, 'Alice', 75000.00, 1)"
        )

    yield database, schema

    _safe_exec(database, "DROP VIEW DBPLUS_EMP_V")
    _safe_exec(database, "DROP TABLE DBPLUS_EMP")
    _safe_exec(database, "DROP TABLE DBPLUS_DEPT")
    database.close()


class TestProtocolConformance:
    def test_driver_is_introspector(self, db2):
        db, _ = db2
        assert isinstance(db.get_driver(), Introspector)

    def test_database_proxies_introspection_methods(self, db2):
        db, _ = db2
        # Database.__getattr__ should route to the driver
        assert callable(db.list_schemas)
        assert callable(db.describe_table)


class TestLazyQueryStore:
    def test_query_store_loaded_lazily(self, db2):
        """The shared QueryStore singleton is populated on first introspection call."""
        from dbplus.drivers import _db2 as db2_pkg

        # Something in the fixture already exercised introspection-adjacent
        # code paths; the cache should be populated by now.
        db, _ = db2
        db.list_schemas()
        assert db2_pkg._query_store is not None


class TestListSchemas:
    def test_returns_list_of_schemainfo(self, db2):
        db, _ = db2
        schemas = db.list_schemas()
        assert schemas
        assert all(isinstance(s, SchemaInfo) for s in schemas)

    def test_includes_current_schema(self, db2):
        db, schema = db2
        names = {s.name for s in db.list_schemas()}
        assert schema in names

    def test_includes_sysibm(self, db2):
        db, _ = db2
        names = {s.name for s in db.list_schemas()}
        assert "SYSIBM" in names


class TestListTables:
    def test_returns_tableinfo(self, db2):
        db, schema = db2
        tables = db.list_tables(schema=schema)
        assert tables
        assert all(isinstance(t, TableInfo) for t in tables)

    def test_default_kind_is_tables_only(self, db2):
        db, schema = db2
        tables = db.list_tables(schema=schema)
        assert {t.type for t in tables} == {"TABLE"}
        names = {t.name for t in tables}
        assert "DBPLUS_EMP" in names
        assert "DBPLUS_DEPT" in names

    def test_filter_by_view_kind(self, db2):
        db, schema = db2
        views = db.list_tables(schema=schema, kind="VIEW")
        names = {v.name for v in views}
        assert "DBPLUS_EMP_V" in names
        assert {v.type for v in views} == {"VIEW"}

    def test_no_schema_filter_returns_many(self, db2):
        db, _ = db2
        tables = db.list_tables()
        # The catalog always has more than a handful of system tables
        assert len(tables) > 10


class TestDescribeTable:
    def test_returns_table_detail(self, db2):
        db, schema = db2
        detail = db.describe_table(schema, "DBPLUS_EMP")
        assert isinstance(detail, TableDetail)
        assert detail.schema == schema
        assert detail.name == "DBPLUS_EMP"
        assert detail.type == "TABLE"

    def test_columns_are_ordered_and_typed(self, db2):
        db, schema = db2
        detail = db.describe_table(schema, "DBPLUS_EMP")
        assert [c.name for c in detail.columns] == [
            "EMP_ID",
            "NAME",
            "SALARY",
            "DEPT_ID",
        ]
        assert all(isinstance(c, ColumnInfo) for c in detail.columns)

    def test_nullability_is_bool(self, db2):
        db, schema = db2
        detail = db.describe_table(schema, "DBPLUS_EMP")
        by_name = {c.name: c for c in detail.columns}
        assert by_name["EMP_ID"].nullable is False
        assert by_name["NAME"].nullable is False
        assert by_name["SALARY"].nullable is True

    def test_decimal_scale_captured(self, db2):
        db, schema = db2
        detail = db.describe_table(schema, "DBPLUS_EMP")
        by_name = {c.name: c for c in detail.columns}
        assert by_name["SALARY"].scale == 2

    def test_primary_key(self, db2):
        db, schema = db2
        detail = db.describe_table(schema, "DBPLUS_EMP")
        assert detail.primary_key == ["EMP_ID"]

    def test_missing_table_raises(self, db2):
        db, schema = db2
        with pytest.raises(DBError):
            db.describe_table(schema, "DOES_NOT_EXIST_TABLE")


class TestListIndexes:
    def test_includes_explicit_index(self, db2):
        db, schema = db2
        indexes = db.list_indexes(schema, "DBPLUS_EMP")
        assert all(isinstance(i, IndexInfo) for i in indexes)
        names = {i.name for i in indexes}
        assert "DBPLUS_EMP_NAME_IDX" in names

    def test_explicit_index_column_ordering_desc(self, db2):
        db, schema = db2
        indexes = db.list_indexes(schema, "DBPLUS_EMP")
        idx = next(i for i in indexes if i.name == "DBPLUS_EMP_NAME_IDX")
        assert len(idx.columns) == 1
        assert idx.columns[0].name == "NAME"
        assert idx.columns[0].ordering == "DESC"
        assert idx.unique is False

    def test_primary_key_index_is_unique(self, db2):
        db, schema = db2
        indexes = db.list_indexes(schema, "DBPLUS_EMP")
        # DB2 creates an implicit unique index to enforce the PK
        assert any(i.unique for i in indexes)


class TestListForeignKeys:
    def test_returns_fk(self, db2):
        db, schema = db2
        fks = db.list_foreign_keys(schema, "DBPLUS_EMP")
        assert len(fks) == 1
        fk = fks[0]
        assert isinstance(fk, ForeignKeyInfo)
        assert fk.name == "DBPLUS_FK_DEPT"
        assert fk.columns == ["DEPT_ID"]
        assert fk.ref_table == "DBPLUS_DEPT"
        assert fk.ref_columns == ["DEPT_ID"]
        assert fk.on_delete == "CASCADE"

    def test_parent_table_has_no_outgoing_fks(self, db2):
        db, schema = db2
        assert db.list_foreign_keys(schema, "DBPLUS_DEPT") == []


class TestGetView:
    def test_returns_view_info(self, db2):
        db, schema = db2
        view = db.get_view(schema, "DBPLUS_EMP_V")
        assert isinstance(view, ViewInfo)
        assert view.name == "DBPLUS_EMP_V"
        assert "DBPLUS_EMP" in view.definition.upper()

    def test_missing_view_raises(self, db2):
        db, schema = db2
        with pytest.raises(DBError):
            db.get_view(schema, "DOES_NOT_EXIST_VIEW")


class TestListProcedures:
    def test_returns_routineinfo(self, db2):
        db, _ = db2
        # SYSPROC always has many procedures on a stock LUW install
        procs = db.list_procedures(schema="SYSPROC")
        assert procs
        assert all(isinstance(p, RoutineInfo) for p in procs)
        assert {p.type for p in procs} == {"PROCEDURE"}

    def test_unknown_schema_returns_empty(self, db2):
        db, _ = db2
        assert db.list_procedures(schema="NOSUCH_SCHEMA_XYZ") == []


class TestListTriggers:
    def test_returns_list(self, db2):
        db, _ = db2
        triggers = db.list_triggers(schema="SYSTOOLS")
        assert isinstance(triggers, list)
        assert all(isinstance(t, TriggerInfo) for t in triggers)


class TestGetTableStats:
    def test_returns_stats_info(self, db2):
        db, schema = db2
        stats = db.get_table_stats(schema, "DBPLUS_EMP")
        assert isinstance(stats, StatsInfo)
        assert stats.schema == schema
        assert stats.name == "DBPLUS_EMP"
        # Stats may be uncollected (None) or a non-negative count
        assert stats.row_count is None or stats.row_count >= 0

    def test_missing_table_raises(self, db2):
        db, schema = db2
        with pytest.raises(DBError):
            db.get_table_stats(schema, "DOES_NOT_EXIST_TABLE")


class TestServerInfo:
    def test_returns_server_info(self, db2):
        db, _ = db2
        info = db.server_info()
        assert isinstance(info, ServerInfo)
        assert info.product  # "DB2/LINUXX8664" or similar
        assert info.version  # "DB2 v11.5.x" or similar
        assert isinstance(info.raw, dict)
