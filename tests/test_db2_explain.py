"""Integration tests for the DB2 Explainer implementation.

These tests require a live DB2 LUW connection, configured via the
DATABASE_URL environment variable, AND the EXPLAIN_* tables must exist
in the current schema. They are skipped transparently otherwise.

Create the EXPLAIN tables once with:

    CALL SYSPROC.SYSINSTALLOBJECTS('EXPLAIN', 'C', NULL, NULL)

Run with (bash):
    DATABASE_URL="DB2://user:pwd@host:port/db" pytest tests/test_db2_explain.py
"""
from __future__ import annotations

import pytest

from dbplus import Database
from dbplus.drivers.explain import Explainer
from tests.conftest import DATABASE_URL


def _detect_db2_with_explain() -> bool:
    try:
        db = Database(DATABASE_URL)
    except Exception:
        return False
    try:
        if db.get_driver().get_name() != "DB2":
            return False
        # Actually try EXPLAIN PLAN FOR. If the EXPLAIN tables are absent,
        # stale (SQL1184N), or the user lacks privilege, skip cleanly.
        try:
            db.execute("EXPLAIN PLAN FOR SELECT 1 FROM SYSIBM.SYSDUMMY1")
            return True
        except Exception:
            return False
    finally:
        db.close()


pytestmark = pytest.mark.skipif(
    not _detect_db2_with_explain(),
    reason=(
        "DB2 explain tests require DATABASE_URL pointing to DB2 LUW with "
        "current-version EXPLAIN_* tables in the current schema. If the "
        "tables are stale, recreate them with "
        "CALL SYSPROC.SYSINSTALLOBJECTS('EXPLAIN', 'C', NULL, NULL)."
    ),
)


@pytest.fixture(scope="module")
def db2():
    database = Database(DATABASE_URL)
    yield database
    database.close()


class TestExplainerProtocol:
    def test_driver_is_explainer(self, db2):
        assert isinstance(db2.get_driver(), Explainer)

    def test_database_proxies_explain(self, db2):
        assert callable(db2.explain)


class TestExplainSimpleSelect:
    def test_returns_instance_dict(self, db2):
        plan = db2.explain("SELECT 1 FROM SYSIBM.SYSDUMMY1")
        assert isinstance(plan, dict)
        assert "explain_time" in plan
        assert "statements" in plan

    def test_has_at_least_one_statement(self, db2):
        plan = db2.explain("SELECT 1 FROM SYSIBM.SYSDUMMY1")
        stmts = plan["statements"]
        assert isinstance(stmts, list) and stmts

    def test_statement_has_operators(self, db2):
        plan = db2.explain("SELECT 1 FROM SYSIBM.SYSDUMMY1")
        for stmt in plan["statements"]:
            assert "operators" in stmt
            assert isinstance(stmt["operators"], list)
        # DB2 records both an "O" (original) and "P" (plan) level per
        # explain; operators live on the "P" row.
        assert any(stmt["operators"] for stmt in plan["statements"])

    def test_has_return_operator(self, db2):
        plan = db2.explain("SELECT 1 FROM SYSIBM.SYSDUMMY1")
        op_types = {
            (op.get("operator_type") or "").strip().upper()
            for stmt in plan["statements"]
            for op in stmt["operators"]
        }
        assert "RETURN" in op_types

    def test_operator_has_arguments_and_predicates_lists(self, db2):
        plan = db2.explain("SELECT 1 FROM SYSIBM.SYSDUMMY1")
        for stmt in plan["statements"]:
            for op in stmt["operators"]:
                assert isinstance(op["arguments"], list)
                assert isinstance(op["predicates"], list)

    def test_statement_has_collections(self, db2):
        plan = db2.explain("SELECT 1 FROM SYSIBM.SYSDUMMY1")
        stmt = plan["statements"][0]
        assert isinstance(stmt["streams"], list)
        assert isinstance(stmt["objects"], list)
        assert isinstance(stmt["diagnostics"], list)


class TestExplainIsJSONSerializable:
    def test_plan_is_json_round_trippable(self, db2):
        import json

        plan = db2.explain("SELECT 1 FROM SYSIBM.SYSDUMMY1")
        text = json.dumps(plan, default=str)
        assert isinstance(text, str) and text
        back = json.loads(text)
        assert any(stmt["operators"] for stmt in back["statements"])


class TestExplainOnlyLatest:
    def test_second_explain_replaces_first(self, db2):
        first = db2.explain("SELECT 1 FROM SYSIBM.SYSDUMMY1")
        second = db2.explain("SELECT 2 FROM SYSIBM.SYSDUMMY1")
        # Each call is a fresh EXPLAIN, so explain_time should differ
        # (or at worst be equal — timestamps in DB2 are coarse; use a
        # structural check instead).
        assert first["statements"][0] != second["statements"][0] or (
            first["explain_time"] != second["explain_time"]
        )


class TestExplainWithCTE:
    def test_cte_query(self, db2):
        plan = db2.explain(
            "WITH x AS (SELECT 1 AS n FROM SYSIBM.SYSDUMMY1) SELECT n FROM x"
        )
        assert plan["statements"]
        assert any(stmt["operators"] for stmt in plan["statements"])
