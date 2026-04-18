import os

import pytest

from dbplus import Database

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///:memory:")


@pytest.fixture(scope="session")
def db_url():
    return DATABASE_URL


@pytest.fixture(scope="session")
def driver_name():
    db = Database(DATABASE_URL)
    name = db.get_driver().get_name()
    db.close()
    return name


def _drop(database, table):
    """Drop a table, ignoring errors (table may not exist)."""
    try:
        with database.transaction():
            database.execute(f"DROP TABLE {table}")
    except Exception:
        pass


@pytest.fixture
def db():
    """Database with a sample employees table, populated and committed.

    Uses DATABASE_URL env var; falls back to SQLite in-memory.
    Schema uses portable SQL (explicit IDs, VARCHAR) so it runs on any backend.
    """
    database = Database(DATABASE_URL)
    _drop(database, "employees")
    with database.transaction():
        database.execute(
            """
            CREATE TABLE employees (
                id      INTEGER NOT NULL,
                name    VARCHAR(100) NOT NULL,
                salary  FLOAT,
                dept    VARCHAR(100),
                PRIMARY KEY (id)
            )
            """
        )
        database.execute("INSERT INTO employees VALUES (1, 'Alice', 75000.0, 'Engineering')")
        database.execute("INSERT INTO employees VALUES (2, 'Bob',   60000.0, 'Marketing')")
        database.execute("INSERT INTO employees VALUES (3, 'Carol', 90000.0, 'Engineering')")
    yield database
    _drop(database, "employees")
    database.close()


@pytest.fixture
def fresh_db():
    """Empty items table, committed — used by transaction tests."""
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
