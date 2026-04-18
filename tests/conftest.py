import pytest
from dbplus import Database


@pytest.fixture
def db():
    """In-memory SQLite database with a sample employees table."""
    database = Database("sqlite:///:memory:")
    database.execute(
        """
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            salary REAL,
            dept TEXT
        )
    """
    )
    database.execute(
        "INSERT INTO employees (name, salary, dept) VALUES ('Alice', 75000.0, 'Engineering')"
    )
    database.execute(
        "INSERT INTO employees (name, salary, dept) VALUES ('Bob', 60000.0, 'Marketing')"
    )
    database.execute(
        "INSERT INTO employees (name, salary, dept) VALUES ('Carol', 90000.0, 'Engineering')"
    )
    yield database
    database.close()


@pytest.fixture
def fresh_db():
    """Clean in-memory SQLite database with no data — used for transaction tests."""
    database = Database("sqlite:///:memory:")
    database.execute(
        """
        CREATE TABLE items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            value TEXT NOT NULL
        )
    """
    )
    yield database
    database.close()
