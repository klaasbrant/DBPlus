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


class TestVersionedQueries:
    def _write(self, tmp_path, body, name="q.sql"):
        f = tmp_path / name
        f.write_text(body)
        return f

    def test_no_version_arg_unversioned_works(self, tmp_path):
        f = self._write(tmp_path, "-- name: plain\nSELECT 1;\n")
        qs = QueryStore(str(f))
        assert qs.plain.sql.startswith("SELECT")

    def test_no_version_arg_versioned_not_resolvable(self, tmp_path):
        f = self._write(
            tmp_path,
            "-- name: only_v\n-- version: >=1.0.0\nSELECT 2;\n",
        )
        qs = QueryStore(str(f))
        with pytest.raises(AttributeError, match="no query named"):
            _ = qs.only_v

    def test_implicit_eq_match(self, tmp_path):
        f = self._write(
            tmp_path,
            "-- name: q\n-- version: 1.2.3\nSELECT 'a';\n",
        )
        qs = QueryStore(str(f), version="1.2.3")
        assert "'a'" in qs.q.sql

    def test_explicit_eq_match(self, tmp_path):
        f = self._write(
            tmp_path,
            "-- name: q\n-- version: ==1.2.3\nSELECT 'b';\n",
        )
        qs = QueryStore(str(f), version="1.2.3")
        assert "'b'" in qs.q.sql

    def test_ge_match(self, tmp_path):
        f = self._write(
            tmp_path,
            "-- name: q\n-- version: >=1.0.0\nSELECT 'ge';\n",
        )
        qs = QueryStore(str(f), version="2.0.0")
        assert "'ge'" in qs.q.sql

    def test_le_match(self, tmp_path):
        f = self._write(
            tmp_path,
            "-- name: q\n-- version: <=2.0.0\nSELECT 'le';\n",
        )
        qs = QueryStore(str(f), version="1.5.0")
        assert "'le'" in qs.q.sql

    def test_gt_miss(self, tmp_path):
        f = self._write(
            tmp_path,
            "-- name: q\n-- version: >2.0.0\nSELECT 'gt';\n",
        )
        qs = QueryStore(str(f), version="2.0.0")
        with pytest.raises(AttributeError):
            _ = qs.q

    def test_lt_match(self, tmp_path):
        f = self._write(
            tmp_path,
            "-- name: q\n-- version: <2.0.0\nSELECT 'lt';\n",
        )
        qs = QueryStore(str(f), version="1.9.9")
        assert "'lt'" in qs.q.sql

    def test_ne_match(self, tmp_path):
        f = self._write(
            tmp_path,
            "-- name: q\n-- version: !=2.0.0\nSELECT 'ne';\n",
        )
        qs = QueryStore(str(f), version="1.0.0")
        assert "'ne'" in qs.q.sql

    def test_first_match_wins(self, tmp_path):
        body = (
            "-- name: q\n-- version: >=1.0.0\nSELECT 'first';\n"
            "-- name: q\n-- version: >=2.0.0\nSELECT 'second';\n"
        )
        f = self._write(tmp_path, body)
        qs = QueryStore(str(f), version="3.0.0")
        assert "'first'" in qs.q.sql

    def test_fallback_to_unversioned(self, tmp_path):
        body = (
            "-- name: q\n-- version: >=5.0.0\nSELECT 'v5';\n"
            "-- name: q\nSELECT 'fallback';\n"
        )
        f = self._write(tmp_path, body)
        qs = QueryStore(str(f), version="1.0.0")
        assert "'fallback'" in qs.q.sql

    def test_version_length_padding(self, tmp_path):
        f = self._write(
            tmp_path,
            "-- name: q\n-- version: ==1.0.0\nSELECT 'pad';\n",
        )
        qs = QueryStore(str(f), version="1.0")
        assert "'pad'" in qs.q.sql

    def test_version_line_anywhere_in_preamble(self, tmp_path):
        body = (
            "-- name: q\n"
            "-- some doc comment\n"
            "-- version: >=1.0.0\n"
            "-- more doc\n"
            "SELECT 'ok';\n"
        )
        f = self._write(tmp_path, body)
        qs = QueryStore(str(f), version="1.5.0")
        assert "'ok'" in qs.q.sql
        assert "some doc comment" in qs.q.comments
        assert "more doc" in qs.q.comments

    def test_indented_version_ignored(self, tmp_path):
        body = (
            "-- name: q\n"
            "    -- version: >=99.0.0\n"
            "SELECT 'kept';\n"
        )
        f = self._write(tmp_path, body)
        qs = QueryStore(str(f), version="1.0.0")
        assert "'kept'" in qs.q.sql

    def test_whitespace_tabs_tolerant(self, tmp_path):
        body = "-- name: q\n--\tversion\t:\t>=1.0.0\nSELECT 'tabs';\n"
        f = self._write(tmp_path, body)
        qs = QueryStore(str(f), version="1.2.3")
        assert "'tabs'" in qs.q.sql

    def test_duplicate_same_version_raises(self, tmp_path):
        body = (
            "-- name: q\n-- version: 1.0.0\nSELECT 'a';\n"
            "-- name: q\n-- version: 1.0.0\nSELECT 'b';\n"
        )
        f = self._write(tmp_path, body)
        with pytest.raises(SQLLoadException, match="duplicate"):
            QueryStore(str(f), version="1.0.0")

    def test_duplicate_unversioned_raises(self, tmp_path):
        body = "-- name: q\nSELECT 'a';\n-- name: q\nSELECT 'b';\n"
        f = self._write(tmp_path, body)
        with pytest.raises(SQLLoadException, match="duplicate"):
            QueryStore(str(f))

    def test_distinct_versions_same_name_allowed(self, tmp_path):
        body = (
            "-- name: q\n-- version: >=2.0\nSELECT 'new';\n"
            "-- name: q\n-- version: <2.0\nSELECT 'old';\n"
        )
        f = self._write(tmp_path, body)
        qs_new = QueryStore(str(f), version="2.5.0")
        qs_old = QueryStore(str(f), version="1.5.0")
        assert "'new'" in qs_new.q.sql
        assert "'old'" in qs_old.q.sql

    def test_cross_file_versioned_resolves(self, tmp_path):
        (tmp_path / "a.sql").write_text(
            "-- name: shared\n-- version: >=2.0.0\nSELECT 'a';\n"
        )
        (tmp_path / "b.sql").write_text(
            "-- name: shared\n-- version: <2.0.0\nSELECT 'b';\n"
        )
        qs = QueryStore(str(tmp_path), version="2.5.0")
        assert "'a'" in qs.shared.sql

    def test_invalid_version_spec_raises(self, tmp_path):
        f = self._write(
            tmp_path,
            "-- name: q\n-- version: abc\nSELECT 1;\n",
        )
        with pytest.raises(SQLParseException):
            QueryStore(str(f), version="1.0.0")

    def test_invalid_constructor_version_raises(self, tmp_path):
        f = self._write(tmp_path, "-- name: q\nSELECT 1;\n")
        with pytest.raises(SQLParseException):
            QueryStore(str(f), version="abc")

    def test_multiple_version_lines_raises(self, tmp_path):
        body = (
            "-- name: q\n"
            "-- version: >=1.0.0\n"
            "-- version: <2.0.0\n"
            "SELECT 1;\n"
        )
        f = self._write(tmp_path, body)
        with pytest.raises(SQLParseException, match="multiple"):
            QueryStore(str(f), version="1.5.0")
