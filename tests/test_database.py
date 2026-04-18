import pytest

from dbplus import Database, DBError
from dbplus.RecordCollection import RecordCollection


class TestConnection:
    def test_is_connected_after_init(self, db):
        assert db.is_connected() is True

    def test_is_connected_false_after_close(self):
        database = Database("sqlite:///:memory:")
        database.close()
        assert database.is_connected() is False

    def test_ensure_connected_reopens(self):
        database = Database("sqlite:///:memory:")
        database.close()
        database.ensure_connected()
        assert database.is_connected() is True
        database.close()

    def test_context_manager_closes_on_exit(self):
        with Database("sqlite:///:memory:") as database:
            assert database.is_connected() is True
        assert database.is_connected() is False

    def test_repr_has_driver_name(self, db):
        r = repr(db)
        assert "SQLITE" in r

    def test_repr_masks_password(self):
        database = Database("sqlite://user:secret@localhost/db")
        r = repr(database)
        assert "secret" not in r
        database.close()


class TestQuery:
    def test_query_returns_record_collection(self, db):
        result = db.query("SELECT * FROM employees")
        assert isinstance(result, RecordCollection)

    def test_query_named_param(self, db):
        result = db.query("SELECT * FROM employees WHERE name = :name", name="Alice")
        rows = result.all()
        assert len(rows) == 1
        assert rows[0]["name"] == "Alice"

    def test_query_positional_param(self, db):
        result = db.query("SELECT * FROM employees WHERE name = ?", "Bob")
        rows = result.all()
        assert len(rows) == 1
        assert rows[0]["name"] == "Bob"

    def test_query_list_param_in_clause(self, db):
        result = db.query(
            "SELECT * FROM employees WHERE name IN (?, ?)", "Alice", "Carol"
        )
        rows = result.all()
        assert len(rows) == 2

    def test_query_all_rows(self, db):
        result = db.query("SELECT * FROM employees")
        assert len(result.all()) == 3

    def test_query_one(self, db):
        row = db.query("SELECT * FROM employees WHERE name = :name", name="Alice").one()
        assert row["name"] == "Alice"

    def test_query_scalar(self, db):
        count = db.query("SELECT COUNT(*) as cnt FROM employees").scalar()
        assert count == 3

    def test_query_empty_one_returns_default(self, db):
        row = db.query("SELECT * FROM employees WHERE name = 'Nobody'").one()
        assert row is None


class TestExecute:
    def test_execute_insert_returns_count(self, db):
        n = db.execute(
            "INSERT INTO employees (name, salary, dept) VALUES ('Dave', 50000.0, 'HR')"
        )
        assert n == 1

    def test_execute_update_returns_count(self, db):
        n = db.execute(
            "UPDATE employees SET salary = 80000.0 WHERE name = 'Alice'"
        )
        assert n == 1

    def test_execute_delete_returns_count(self, db):
        n = db.execute("DELETE FROM employees WHERE name = 'Bob'")
        assert n == 1
        remaining = db.query("SELECT * FROM employees").all()
        assert len(remaining) == 2

    def test_execute_invalid_sql_raises_dberror(self, db):
        with pytest.raises(DBError):
            db.execute("NOT VALID SQL")


class TestLastInsertId:
    def test_last_insert_id_after_insert(self, db):
        db.execute(
            "INSERT INTO employees (name, salary, dept) VALUES ('Dave', 50000.0, 'HR')"
        )
        last_id = db.last_insert_id()
        assert last_id == 4

    def test_last_insert_id_increments(self, db):
        db.execute(
            "INSERT INTO employees (name, salary, dept) VALUES ('Eve', 55000.0, 'HR')"
        )
        id1 = db.last_insert_id()
        db.execute(
            "INSERT INTO employees (name, salary, dept) VALUES ('Frank', 58000.0, 'HR')"
        )
        id2 = db.last_insert_id()
        assert id2 == id1 + 1


class TestErrorHandling:
    def test_error_code_zero_on_success(self, db):
        db.query("SELECT * FROM employees").all()
        assert db.error_code() == 0

    def test_error_code_nonzero_after_failure(self, db):
        try:
            db.execute("SELECT * FROM nonexistent_table")
        except DBError:
            pass
        assert db.error_code() == 1

    def test_error_info_none_on_success(self, db):
        db.query("SELECT * FROM employees").all()
        assert db.error_info() is None

    def test_error_info_string_after_failure(self, db):
        try:
            db.execute("SELECT * FROM nonexistent_table")
        except DBError:
            pass
        assert db.error_info() is not None
        assert isinstance(db.error_info(), str)


class TestCallproc:
    def test_callproc_sqlite_returns_none(self, db):
        result = db.callproc("nonexistent_proc")
        assert result is None


class TestTransactionState:
    def test_is_transaction_active_false_initially(self, db):
        assert db.is_transaction_active() is False


class TestDriverDelegation:
    def test_getattr_delegates_get_name(self, db):
        assert db.get_name() == "sqlite"

    def test_getattr_delegates_get_database(self, db):
        assert db.get_database() == ":memory:"

    def test_getattr_unknown_raises_attribute_error(self, db):
        with pytest.raises(AttributeError):
            _ = db.completely_nonexistent_method
