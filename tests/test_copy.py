import pytest

from dbplus import Database
from tests.conftest import DATABASE_URL, _drop


@pytest.fixture
def copy_db():
    """Source and target tables for copy_to / copy_from tests.

    source: typed columns (id INTEGER, salary FLOAT)
    target: all VARCHAR — copy_from imports strings, so a typed target
            would reject them on databases with strict typing (Postgres, etc.)
    """
    database = Database(DATABASE_URL)
    _drop(database, "target")
    _drop(database, "source")
    with database.transaction():
        database.execute(
            """
            CREATE TABLE source (
                id     INTEGER NOT NULL,
                name   VARCHAR(100),
                salary FLOAT,
                dept   VARCHAR(100),
                PRIMARY KEY (id)
            )
            """
        )
        database.execute("INSERT INTO source VALUES (1, 'Alice', 75000.0, 'Engineering')")
        database.execute("INSERT INTO source VALUES (2, 'Bob',   60000.0, 'Marketing')")
        database.execute("INSERT INTO source VALUES (3, 'Carol', 90000.0, 'Engineering')")

        database.execute(
            """
            CREATE TABLE target (
                id     VARCHAR(20),
                name   VARCHAR(100),
                salary VARCHAR(20),
                dept   VARCHAR(100)
            )
            """
        )
    yield database
    _drop(database, "target")
    _drop(database, "source")
    database.close()


class TestCopyTo:
    def test_copy_to_returns_row_count(self, copy_db, tmp_path):
        count = copy_db.copy_to(str(tmp_path / "out.tsv"), "source")
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
            assert len(fh.readlines()) == 6

    def test_copy_to_null_sentinel(self, copy_db, tmp_path):
        with copy_db.transaction():
            copy_db.execute("INSERT INTO source VALUES (4, 'Dave', NULL, NULL)")
        f = tmp_path / "out.tsv"
        copy_db.copy_to(str(f), "source", null="NULL")
        assert "NULL" in f.read_text()


class TestCopyFrom:
    def test_copy_from_returns_row_count(self, copy_db, tmp_path):
        export_file = str(tmp_path / "export.tsv")
        copy_db.copy_to(export_file, "source")
        count = copy_db.copy_from(export_file, "target")
        assert count == 3

    def test_copy_from_round_trip_names(self, copy_db, tmp_path):
        export_file = str(tmp_path / "export.tsv")
        copy_db.copy_to(export_file, "source")
        copy_db.copy_from(export_file, "target")
        rows = copy_db.query("SELECT name FROM target ORDER BY name").all()
        names = [r["name"] for r in rows]
        assert names == ["Alice", "Bob", "Carol"]

    def test_copy_from_with_header(self, copy_db, tmp_path):
        export_file = str(tmp_path / "export.tsv")
        copy_db.copy_to(export_file, "source", header=True)
        copy_db.copy_from(export_file, "target", header=True)
        assert len(copy_db.query("SELECT * FROM target").all()) == 3

    def test_copy_from_null_sentinel(self, copy_db, tmp_path):
        with copy_db.transaction():
            copy_db.execute("INSERT INTO source VALUES (4, 'Dave', NULL, NULL)")
        export_file = str(tmp_path / "export.tsv")
        copy_db.copy_to(export_file, "source", null="NULL")
        copy_db.copy_from(
            export_file, "target", null="NULL",
            columns=["id", "name", "salary", "dept"]
        )
        rows = copy_db.query("SELECT * FROM target WHERE name = 'Dave'").all()
        assert len(rows) == 1
        assert rows[0]["dept"] is None
