import pytest

from dbplus.Record import Record
from dbplus.RecordCollection import RecordCollection


def make_collection(*dicts):
    records = [Record(d) for d in dicts]
    return RecordCollection(iter(records), None)


SAMPLE = [
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25},
    {"name": "Carol", "age": 35},
]


class TestLength:
    def test_len_zero_before_consume(self):
        rc = make_collection(*SAMPLE)
        assert len(rc) == 0

    def test_len_after_all(self):
        rc = make_collection(*SAMPLE)
        rc.all()
        assert len(rc) == 3


class TestGetItem:
    def test_getitem_positive_index(self):
        rc = make_collection(*SAMPLE)
        assert rc[0]["name"] == "Alice"
        assert rc[1]["name"] == "Bob"

    def test_getitem_negative_index(self):
        rc = make_collection(*SAMPLE)
        assert rc[-1]["name"] == "Carol"

    def test_getitem_out_of_range_raises(self):
        rc = make_collection(*SAMPLE)
        with pytest.raises(IndexError):
            _ = rc[10]

    def test_slice_returns_record_collection(self):
        rc = make_collection(*SAMPLE)
        sliced = rc[0:2]
        assert isinstance(sliced, RecordCollection)
        rows = sliced.all()
        assert len(rows) == 2

    def test_invalid_key_type_raises(self):
        rc = make_collection(*SAMPLE)
        with pytest.raises(TypeError):
            _ = rc["bad"]


class TestIteration:
    def test_iter_yields_all(self):
        rc = make_collection(*SAMPLE)
        names = [r["name"] for r in rc]
        assert names == ["Alice", "Bob", "Carol"]

    def test_next_advances_cursor(self):
        rc = make_collection(*SAMPLE)
        r = rc.next()
        assert r["name"] == "Alice"
        r = rc.next()
        assert r["name"] == "Bob"

    def test_stop_iteration_sets_pending_false(self):
        rc = make_collection({"x": 1})
        rc.all()
        assert rc.pending is False


class TestOneAndScalar:
    def test_one_returns_first_row(self):
        rc = make_collection(*SAMPLE)
        assert rc.one()["name"] == "Alice"

    def test_one_empty_returns_default(self):
        rc = make_collection()
        assert rc.one() is None
        assert rc.one(default="fallback") == "fallback"

    def test_scalar_returns_first_column(self):
        rc = make_collection({"count": 42})
        assert rc.scalar() == 42

    def test_scalar_empty_returns_default(self):
        rc = make_collection()
        assert rc.scalar() is None
        assert rc.scalar(default=0) == 0


class TestAll:
    def test_all_returns_records(self):
        rc = make_collection(*SAMPLE)
        rows = rc.all()
        assert len(rows) == 3
        assert isinstance(rows[0], Record)

    def test_all_as_dict(self):
        rc = make_collection(*SAMPLE)
        rows = rc.all(as_dict=True)
        assert rows[0] == {"name": "Alice", "age": 30}

    def test_all_as_tuple(self):
        rc = make_collection(*SAMPLE)
        rows = rc.all(as_tuple=True)
        assert rows[0] == ("Alice", 30)

    def test_all_as_json(self):
        import json

        rc = make_collection(*SAMPLE)
        rows = rc.all(as_json=True)
        parsed = json.loads(rows[0])
        assert parsed["name"] == "Alice"

    def test_as_dict_convenience(self):
        rc = make_collection(*SAMPLE)
        rows = rc.as_dict()
        assert rows[1]["name"] == "Bob"

    def test_as_tuple_convenience(self):
        rc = make_collection(*SAMPLE)
        rows = rc.as_tuple()
        assert rows[2] == ("Carol", 35)

    def test_as_json_convenience(self):
        import json

        rc = make_collection(*SAMPLE)
        rows = rc.as_json()
        assert json.loads(rows[0])["age"] == 30


class TestDataFrame:
    def test_as_dataframe(self):
        pd = pytest.importorskip("pandas")
        rc = make_collection(*SAMPLE)
        df = rc.as_DataFrame()
        assert list(df.columns) == ["name", "age"]
        assert len(df) == 3


class TestStrRepr:
    def test_str_non_empty_has_header_separator(self):
        rc = make_collection(*SAMPLE)
        s = str(rc)
        assert "name" in s
        assert "---" in s
        assert "Alice" in s

    def test_str_empty_is_newline(self):
        rc = make_collection()
        assert str(rc) == "\n"

    def test_repr_contains_size_and_pending(self):
        rc = make_collection(*SAMPLE)
        r = repr(rc)
        assert "pending" in r
