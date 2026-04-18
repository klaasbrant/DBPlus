import datetime
import decimal
import json

import pytest

from dbplus.Record import Record
from dbplus.helpers import json_handler


def make_record(**kwargs):
    return Record(kwargs)


class TestRecordAccess:
    def test_getitem_by_string_key(self):
        r = make_record(name="Alice", age=30)
        assert r["name"] == "Alice"
        assert r["age"] == 30

    def test_getitem_by_int_index(self):
        r = make_record(name="Alice", age=30)
        assert r[0] == "Alice"
        assert r[1] == 30

    def test_getitem_missing_key_raises(self):
        r = make_record(name="Alice")
        with pytest.raises(KeyError, match="missing"):
            r["missing"]

    def test_getattr_attribute_access(self):
        r = make_record(name="Alice", salary=75000.0)
        assert r.name == "Alice"
        assert r.salary == 75000.0

    def test_getattr_missing_raises_attribute_error(self):
        r = make_record(name="Alice")
        with pytest.raises(AttributeError):
            _ = r.nonexistent

    def test_get_existing_key(self):
        r = make_record(name="Alice")
        assert r.get("name") == "Alice"

    def test_get_missing_key_returns_default(self):
        r = make_record(name="Alice")
        assert r.get("missing") is None
        assert r.get("missing", "fallback") == "fallback"

    def test_keys(self):
        r = make_record(name="Alice", age=30)
        assert r.keys() == ["name", "age"]

    def test_values(self):
        r = make_record(name="Alice", age=30)
        assert r.values() == ["Alice", 30]


class TestRecordConversions:
    def test_as_dict_returns_copy(self):
        r = make_record(name="Alice", age=30)
        d = r.as_dict()
        assert d == {"name": "Alice", "age": 30}
        d["name"] = "Mutated"
        assert r["name"] == "Alice"

    def test_as_tuple(self):
        r = make_record(name="Alice", age=30)
        assert r.as_tuple() == ("Alice", 30)

    def test_as_list(self):
        r = make_record(name="Alice", age=30)
        assert r.as_list() == ["Alice", 30]

    def test_as_json_valid(self):
        r = make_record(name="Alice", age=30)
        parsed = json.loads(r.as_json())
        assert parsed == {"name": "Alice", "age": 30}

    def test_as_json_decimal(self):
        r = make_record(salary=decimal.Decimal("75000.50"))
        parsed = json.loads(r.as_json())
        assert parsed["salary"] == pytest.approx(75000.50)

    def test_as_json_datetime(self):
        dt = datetime.datetime(2024, 1, 15, 10, 30, 0)
        r = make_record(created=dt)
        parsed = json.loads(r.as_json())
        assert parsed["created"] == "2024-01-15T10:30:00"

    def test_as_model_dataclass(self):
        from dataclasses import dataclass

        @dataclass
        class Employee:
            name: str
            age: int

        r = make_record(name="Alice", age=30)
        emp = r.as_model(Employee)
        assert emp.name == "Alice"
        assert emp.age == 30

    def test_as_model_non_class_raises(self):
        r = make_record(name="Alice")
        with pytest.raises(ValueError, match="expects a class"):
            r.as_model("not_a_class")


class TestRecordMeta:
    def test_repr_contains_json(self):
        r = make_record(name="Alice")
        assert "Alice" in repr(r)

    def test_dir_includes_column_names(self):
        r = make_record(name="Alice", salary=75000.0)
        attrs = dir(r)
        assert "name" in attrs
        assert "salary" in attrs

    def test_slots_no_extra_attrs(self):
        r = make_record(name="Alice")
        with pytest.raises(AttributeError):
            r.__dict__
