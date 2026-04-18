import pytest

from dbplus import Database, DBError
from dbplus.RecordCollection import RecordCollection
from tests.conftest import DATABASE_URL


class TestConnection:
    def test_is_connected_after_init(self, db):
        assert db.is_connected() is True

    def test_is_connected_false_after_close(self, db_url):
        database = Database(db_url)
        database.close()
        assert database.is_connected() is False

    def test_ensure_connected_reopens(self, db_url):
        database = Database(db_url)
        database.close()
        database.ensure_connected()
        assert database.is_connected() is True
        database.close()

    def test_context_manager_closes_on_exit(self, db_url):
        with Database(db_url) as database:
            assert database.is_connected() is True
        assert database.is_connected() is False

    def test_repr_contains_state(self, db):
        r = repr(db)
        assert "connected=" in r

    def test_repr_masks_password(self):
        # SQLite ignores host/credentials — safe to use a fake URL here
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
        assert len(db.query("SELECT * FROM employees").all()) == 3

    def test_query_one(self, db):
        row = db.query("SELECT * FROM employees WHERE name = :name", name="Alice").one()
        assert row["name"] == "Alice"

    def test_query_scalar(self, db):
        count = db.query("SELECT COUNT(*) as cnt FROM employees").scalar()
        assert count == 3

    def test_query_empty_one_returns_default(self, db):
        assert db.query("SELECT * FROM employees WHERE name = 'Nobody'").one() is None


class TestExecute:
    def test_execute_insert_returns_count(self, db):
        n = db.execute("INSERT INTO employees VALUES (4, 'Dave', 50000.0, 'HR')")
        assert n == 1

    def test_execute_update_returns_count(self, db):
        n = db.execute("UPDATE employees SET salary = 80000.0 WHERE name = 'Alice'")
        assert n == 1

    def test_execute_delete_returns_count(self, db):
        n = db.execute("DELETE FROM employees WHERE name = 'Bob'")
        assert n == 1
        assert len(db.query("SELECT * FROM employees").all()) == 2

    def test_execute_invalid_sql_raises_dberror(self, db):
        with pytest.raises(DBError):
            db.execute("NOT VALID SQL")


class TestLastInsertId:
    def test_last_insert_id_does_not_raise(self, db):
        db.execute("INSERT INTO employees VALUES (4, 'Dave', 50000.0, 'HR')")
        db.last_insert_id()  # must not raise

    def test_last_insert_id_sqlite(self, db, driver_name):
        """SQLite returns the rowid of the last inserted row."""
        if driver_name != "sqlite":
            pytest.skip("last_insert_id rowid is SQLite-specific")
        db.execute("INSERT INTO employees VALUES (4, 'Dave', 50000.0, 'HR')")
        assert db.last_insert_id() == 4


class TestErrorHandling:
    def test_error_code_falsy_on_success(self, db):
        db.query("SELECT * FROM employees").all()
        assert not db.error_code()  # 0, None, or '' depending on driver

    def test_error_code_truthy_after_failure(self, db):
        try:
            db.execute("SELECT * FROM nonexistent_table")
        except DBError:
            pass
        assert db.error_code()  # 1, SQLSTATE string, etc.

    def test_error_info_falsy_on_success(self, db):
        db.query("SELECT * FROM employees").all()
        assert not db.error_info()  # None or '' depending on driver

    def test_error_info_set_after_failure(self, db):
        try:
            db.execute("SELECT * FROM nonexistent_table")
        except DBError:
            pass
        info = db.error_info()
        assert info  # truthy — a non-empty string


class TestCallproc:
    def test_callproc_does_not_raise_on_unsupported(self, db, driver_name):
        """SQLite returns None for callproc (unsupported). Other drivers may raise."""
        if driver_name == "sqlite":
            assert db.callproc("nonexistent_proc") is None
        else:
            pytest.skip("callproc behaviour is driver-specific; tested via driver tests")


class TestTransactionState:
    def test_is_transaction_active_false_initially(self, db):
        assert db.is_transaction_active() is False


class TestDriverDelegation:
    def test_getattr_delegates_get_name(self, db):
        name = db.get_name()
        assert isinstance(name, str)
        assert len(name) > 0

    def test_getattr_unknown_raises_attribute_error(self, db):
        with pytest.raises(AttributeError):
            _ = db.completely_nonexistent_method
