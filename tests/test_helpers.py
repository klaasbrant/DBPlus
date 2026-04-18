import datetime
import decimal
import json

import pytest

from dbplus.helpers import _parse_database_url, _validate_identifier, guess_type, json_handler


class TestParseUrl:
    def test_sqlite_url(self):
        result = _parse_database_url("sqlite:///path/to/db.sqlite")
        assert result["driver"] == "sqlite"
        assert result["database"] == "path/to/db.sqlite"

    def test_memory_sqlite_url(self):
        result = _parse_database_url("sqlite:///:memory:")
        assert result["driver"] == "sqlite"
        assert result["database"] == ":memory:"

    def test_postgres_url_with_credentials(self):
        result = _parse_database_url("postgres://user:secret@localhost:5432/mydb")
        assert result["driver"] == "postgres"
        assert result["uid"] == "user"
        assert result["pwd"] == "secret"
        assert result["host"] == "localhost"
        assert result["port"] == "5432"
        assert result["database"] == "mydb"

    def test_url_without_credentials(self):
        result = _parse_database_url("sqlite:///db.sqlite")
        assert result["uid"] is None
        assert result["pwd"] is None

    def test_none_returns_none(self):
        assert _parse_database_url(None) is None


class TestValidateIdentifier:
    def test_valid_simple_name(self):
        _validate_identifier("employees")

    def test_valid_with_underscore(self):
        _validate_identifier("my_table")

    def test_valid_with_dot(self):
        _validate_identifier("schema.table")

    def test_valid_with_numbers(self):
        _validate_identifier("table1")

    def test_invalid_starts_with_number(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            _validate_identifier("1table")

    def test_invalid_with_space(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            _validate_identifier("my table")

    def test_invalid_sql_injection(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            _validate_identifier("employees; DROP TABLE employees")

    def test_invalid_with_dash(self):
        with pytest.raises(ValueError, match="Invalid SQL identifier"):
            _validate_identifier("my-table")


class TestGuessType:
    def test_integer_string(self):
        assert guess_type("42") == 42
        assert isinstance(guess_type("42"), int)

    def test_float_string(self):
        assert guess_type("3.14") == pytest.approx(3.14)
        assert isinstance(guess_type("3.14"), float)

    def test_date_string(self):
        result = guess_type("2024-01-15")
        assert isinstance(result, datetime.datetime)
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15

    def test_datetime_string(self):
        result = guess_type("2024-01-15 10:30:00")
        assert isinstance(result, datetime.datetime)
        assert result.hour == 10

    def test_plain_string_returned_as_is(self):
        assert guess_type("hello") == "hello"

    def test_unrecognized_string_returned_as_is(self):
        assert guess_type("hello world") == "hello world"


class TestJsonHandler:
    def test_decimal_serialized_as_float(self):
        data = {"value": decimal.Decimal("99.99")}
        result = json.dumps(data, cls=json_handler)
        parsed = json.loads(result)
        assert parsed["value"] == pytest.approx(99.99)

    def test_datetime_serialized_as_iso(self):
        dt = datetime.datetime(2024, 6, 1, 12, 0, 0)
        data = {"ts": dt}
        result = json.dumps(data, cls=json_handler)
        parsed = json.loads(result)
        assert parsed["ts"] == "2024-06-01T12:00:00"

    def test_date_serialized_as_iso(self):
        d = datetime.date(2024, 6, 1)
        data = {"d": d}
        result = json.dumps(data, cls=json_handler)
        parsed = json.loads(result)
        assert parsed["d"] == "2024-06-01"

    def test_unserializable_raises(self):
        data = {"x": object()}
        with pytest.raises(TypeError):
            json.dumps(data, cls=json_handler)
