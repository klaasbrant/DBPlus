import pytest

from dbplus import Database, DBError
from tests.conftest import DATABASE_URL, _drop


@pytest.fixture
def tdb():
    """Fresh items table per test, using DATABASE_URL (env or SQLite fallback)."""
    database = Database(DATABASE_URL)
    _drop(database, "items")
    with database.transaction():
        database.execute(
            """
            CREATE TABLE items (
                id    INTEGER NOT NULL,
                value VARCHAR(100) NOT NULL,
                PRIMARY KEY (id)
            )
            """
        )
    yield database
    _drop(database, "items")
    database.close()


class TestTransactionContextManager:
    def test_commit_makes_rows_visible(self, tdb):
        with tdb.transaction():
            tdb.execute("INSERT INTO items VALUES (1, 'committed')")
        rows = tdb.query("SELECT * FROM items").all()
        assert len(rows) == 1
        assert rows[0]["value"] == "committed"

    def test_exception_rolls_back_rows(self, tdb):
        with pytest.raises(ValueError):
            with tdb.transaction():
                tdb.execute("INSERT INTO items VALUES (1, 'will_rollback')")
                raise ValueError("intentional failure")
        rows = tdb.query("SELECT * FROM items").all()
        assert len(rows) == 0

    def test_transaction_active_inside_block(self, tdb):
        results = []
        with tdb.transaction():
            results.append(tdb.is_transaction_active())
        assert results[0] is True

    def test_transaction_inactive_after_block(self, tdb):
        with tdb.transaction():
            tdb.execute("INSERT INTO items VALUES (1, 'x')")
        assert tdb.is_transaction_active() is False


class TestManualTransaction:
    def test_begin_commit(self, tdb):
        tdb.begin_transaction()
        tdb.execute("INSERT INTO items VALUES (1, 'manual')")
        tdb.commit()
        assert len(tdb.query("SELECT * FROM items").all()) == 1

    def test_begin_rollback(self, tdb):
        tdb.begin_transaction()
        tdb.execute("INSERT INTO items VALUES (1, 'will_rollback')")
        tdb.rollback()
        assert len(tdb.query("SELECT * FROM items").all()) == 0

    def test_is_active_true_after_begin(self, tdb):
        tdb.begin_transaction()
        assert tdb.is_transaction_active() is True
        tdb.rollback()

    def test_is_active_false_after_commit(self, tdb):
        tdb.begin_transaction()
        tdb.commit()
        assert tdb.is_transaction_active() is False

    def test_is_active_false_after_rollback(self, tdb):
        tdb.begin_transaction()
        tdb.rollback()
        assert tdb.is_transaction_active() is False


class TestTransactionErrors:
    def test_nested_begin_raises(self, tdb):
        tdb.begin_transaction()
        with pytest.raises(DBError, match="Nested"):
            tdb.begin_transaction()
        tdb.rollback()

    def test_commit_without_begin_raises(self, tdb):
        with pytest.raises(DBError, match="never started"):
            tdb.commit()

    def test_rollback_without_begin_raises(self, tdb):
        with pytest.raises(DBError, match="never started"):
            tdb.rollback()

    def test_manual_commit_inside_context_raises(self, tdb):
        with pytest.raises(DBError):
            with tdb.transaction():
                tdb.execute("INSERT INTO items VALUES (1, 'x')")
                tdb.commit()

    def test_manual_rollback_inside_context_raises(self, tdb):
        with pytest.raises(DBError):
            with tdb.transaction():
                tdb.execute("INSERT INTO items VALUES (1, 'x')")
                tdb.rollback()
