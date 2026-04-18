import pytest

from dbplus.QueryStore import QueryStore, SQLLoadException, SQLParseException


SINGLE_FILE_SQL = """\
-- name: get_all
-- Returns all rows
SELECT * FROM employees;

-- name: get_by_id
SELECT * FROM employees WHERE id = :id;
"""

SECOND_FILE_SQL = """\
-- name: count_rows
SELECT COUNT(*) FROM employees;
"""


class TestLoadFromFile:
    def test_load_single_file(self, tmp_path):
        f = tmp_path / "queries.sql"
        f.write_text(SINGLE_FILE_SQL)
        qs = QueryStore(str(f))
        assert hasattr(qs, "get_all") or "get_all" in qs.query_store

    def test_query_sql_content(self, tmp_path):
        f = tmp_path / "queries.sql"
        f.write_text(SINGLE_FILE_SQL)
        qs = QueryStore(str(f))
        assert "SELECT" in qs.get_all.sql

    def test_query_comments_captured(self, tmp_path):
        f = tmp_path / "queries.sql"
        f.write_text(SINGLE_FILE_SQL)
        qs = QueryStore(str(f))
        assert "Returns all rows" in qs.get_all.comments

    def test_query_name_attribute(self, tmp_path):
        f = tmp_path / "queries.sql"
        f.write_text(SINGLE_FILE_SQL)
        qs = QueryStore(str(f))
        assert qs.get_all.name == "get_all"

    def test_multiple_queries_in_file(self, tmp_path):
        f = tmp_path / "queries.sql"
        f.write_text(SINGLE_FILE_SQL)
        qs = QueryStore(str(f))
        assert "get_all" in qs.query_store
        assert "get_by_id" in qs.query_store

    def test_missing_query_raises_attribute_error(self, tmp_path):
        f = tmp_path / "queries.sql"
        f.write_text(SINGLE_FILE_SQL)
        qs = QueryStore(str(f))
        with pytest.raises(AttributeError, match="no query named"):
            _ = qs.nonexistent

    def test_nonexistent_path_raises(self, tmp_path):
        with pytest.raises(SQLLoadException):
            QueryStore(str(tmp_path / "missing.sql"))


class TestLoadFromDirectory:
    def test_load_from_directory(self, tmp_path):
        (tmp_path / "a.sql").write_text(SINGLE_FILE_SQL)
        (tmp_path / "b.sql").write_text(SECOND_FILE_SQL)
        qs = QueryStore(str(tmp_path))
        assert "get_all" in qs.query_store
        assert "count_rows" in qs.query_store

    def test_prefix_from_subdirectory(self, tmp_path):
        subdir = tmp_path / "reports"
        subdir.mkdir()
        (subdir / "queries.sql").write_text(SECOND_FILE_SQL)
        qs = QueryStore(str(tmp_path))
        assert any("count_rows" in k for k in qs.query_store)

    def test_duplicate_name_raises(self, tmp_path):
        (tmp_path / "a.sql").write_text(SINGLE_FILE_SQL)
        (tmp_path / "b.sql").write_text(SINGLE_FILE_SQL)
        with pytest.raises(SQLLoadException, match="duplicate"):
            QueryStore(str(tmp_path))
