import json
from dbplus.helpers import json_handler

class Record(object):
    """A row, from a query, from a database."""
    __slots__ = ('_keys', '_values')

    def __init__(self, row):
        self._keys = list(row.keys())
        self._values = list(row.values())

        # Ensure that lengths match properly.
        assert len(self._keys) == len(self._values)

    def keys(self):
        """Returns the list of column names from the query."""
        return self._keys

    def values(self):
        """Returns the list of values from the query."""
        return self._values

    def __repr__(self):
        return '<Record {}>'.format(json.dumps(self.as_dict(),default=json_handler))

    def __getitem__(self, key):
        # Support for index-based lookup.
        if isinstance(key, int):
            return self.values()[key]

        # Support for string-based lookup.
        if key in self.keys():
            i = self.keys().index(key)
            return self.values()[i]

        raise KeyError("Record contains no '{}' field.".format(key))

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(e)

    def __dir__(self):
        standard = dir(super(Record, self))
        # Merge standard attrs with generated ones (from column names).
        return sorted(standard + [str(k) for k in self.keys()])

    def get(self, key, default=None):
        """Returns the value for a given key, or default."""
        try:
            return self[key]
        except KeyError:
            return default

    def as_dict(self):
        """Returns the row as a dictionary, as ordered."""
        items = zip(self.keys(), self.values())

        return dict(items)

    def as_tuple(self):
        return tuple(self.values())