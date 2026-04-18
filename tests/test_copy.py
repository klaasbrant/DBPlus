import csv

import pytest

from dbplus import Database


@pytest.fixture
def copy_db():
    """In-memory SQLite db with sample data and a target table for imports."""
    database = Database("sqlite:///:memory:")
    database.execute(
        """
        CREATE TABLE source (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            salary REAL,
            dept TEXT
        )
    """
    )
    database.execute("INSERT INTO source (name, salary, dept) VALUES ('Alice', 75000.0, 'Engineering')")
    database.execute("INSERT INTO source (name, salary, dept) VALUES ('Bob', 60000.0, 'Marketing')")
    database.execute("INSERT INTO source (name, salary, dept) VALUES ('Carol', 90000.0, 'Engineering')")

    database.execute(
        """
        CREATE TABLE target (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            salary REAL,
            dept TEXT
        )
    """
    )
    yield database
    database.close()


class TestCopyTo:
    def test_copy_to_returns_row_count(self, copy_db, tmp_path):
        f = str(tmp_path / "out.tsv")
        count = copy_db.copy_to(f, "source")
        assert count == 3

    def test_copy_to_file_has_correct_rows(self, copy_db, tmp_path):
        f = tmp_path / "out.tsv"
        copy_db.copy_to(str(f), "source")
        content = f.read_text()
        assert "Alice" in content
        assert "Bob" in content
        assert "Carol" in content

    def test_copy_to_with_header(self, copy_db, tmp_path):
        f = tmp_path / "out.tsv"
        copy_db.copy_to(str(f), "source", header=True)
        lines = f.read_text().splitlines()
        assert "name" in lines[0]
        assert "salary" in lines[0]

    def test_copy_to_column_subset(self, copy_db, tmp_path):
        f = tmp_path / "out.tsv"
        copy_db.copy_to(str(f), "source", columns=["name", "dept"], header=True)
        lines = f.read_text().splitlines()
        assert "name" in lines[0]
        assert "salary" not in lines[0]

    def test_copy_to_append(self, copy_db, tmp_path):
        f = str(tmp_path / "out.tsv")
        copy_db.copy_to(f, "source")
        copy_db.copy_to(f, "source", append=True)
        with open(f) as fh:
            rows = fh.readlines()
        assert len(rows) == 6

    def test_copy_to_null_sentinel(self, copy_db, tmp_path):
        copy_db.execute("INSERT INTO source (name, salary, dept) VALUES ('Dave', NULL, NULL)")
        f = tmp_path / "out.tsv"
        copy_db.copy_to(str(f), "source", null="NULL")
        content = f.read_text()
        assert "NULL" in content


class TestCopyFrom:
    def test_copy_from_returns_row_count(self, copy_db, tmp_path):
        export_file = str(tmp_path / "export.tsv")
        copy_db.copy_to(export_file, "source")
        count = copy_db.copy_from(export_file, "target")
        assert count == 3

    def test_copy_from_round_trip(self, copy_db, tmp_path):
        export_file = str(tmp_path / "export.tsv")
        copy_db.copy_to(export_file, "source")
        copy_db.copy_from(export_file, "target")
        rows = copy_db.query("SELECT name, salary, dept FROM target ORDER BY name").all()
        assert rows[0]["name"] == "Alice"
        assert rows[1]["name"] == "Bob"
        assert rows[2]["name"] == "Carol"

    def test_copy_from_with_header(self, copy_db, tmp_path):
        export_file = str(tmp_path / "export.tsv")
        copy_db.copy_to(export_file, "source", header=True)
        copy_db.copy_from(export_file, "target", header=True)
        rows = copy_db.query("SELECT * FROM target").all()
        assert len(rows) == 3

    def test_copy_from_null_sentinel(self, copy_db, tmp_path):
        copy_db.execute("INSERT INTO source (name, salary, dept) VALUES ('Dave', NULL, NULL)")
        export_file = str(tmp_path / "export.tsv")
        copy_db.copy_to(export_file, "source", null="NULL")
        copy_db.copy_from(export_file, "target", null="NULL", columns=["id", "name", "salary", "dept"])
        rows = copy_db.query("SELECT * FROM target WHERE name = 'Dave'").all()
        assert len(rows) == 1
        assert rows[0]["dept"] is None
