from __future__ import annotations

import inspect
import json
from typing import Any, Dict, List, Optional, Tuple, Type, Union

from dbplus.helpers import json_handler


class Record:
    """A row, from a query, from a database."""

    __slots__ = ("_keys", "_values", "_dict")

    def __init__(self, row: Dict[str, Any]) -> None:
        self._keys = list(row.keys())
        self._values = list(row.values())
        if len(self._keys) != len(self._values):
            raise ValueError(
                f"Record keys ({len(self._keys)}) and values ({len(self._values)}) length mismatch"
            )
        self._dict = dict(zip(self._keys, self._values))

    def keys(self) -> List[str]:
        """Returns the list of column names from the query."""
        return self._keys

    def values(self) -> List[Any]:
        """Returns the list of values from the query."""
        return self._values

    def __repr__(self) -> str:
        return f"<Record {format(json.dumps(self.as_dict(),cls=json_handler))}>"

    def __getitem__(self, key: Union[int, str]) -> Any:
        # Support for index-based lookup.
        if isinstance(key, int):
            return self._values[key]

        # Support for string-based lookup.
        try:
            return self._dict[key]
        except KeyError:
            raise KeyError(f"Record does not contain '{key}' field.")

    def __getattr__(self, key: str) -> Any:
        try:
            return self[key]
        except KeyError as e:
            raise AttributeError(e)

    def __dir__(self) -> List[str]:
        standard = dir(super(Record, self))
        # Merge standard attrs with generated ones (from column names).
        return sorted(standard + [str(k) for k in self.keys()])

    def get(self, key: str, default: Any = None) -> Any:
        """Returns the value for a given key, or default."""
        try:
            return self[key]
        except KeyError:
            return default

    def as_dict(self) -> Dict[str, Any]:
        """Returns the row as a dictionary, as ordered."""
        return dict(self._dict)

    def as_tuple(self) -> Tuple[Any, ...]:
        return tuple(self.values())

    def as_list(self) -> List[Any]:
        return list(self.values())

    def as_json(self, **kwargs: Any) -> str:
        return json.dumps(self.as_dict(), cls=json_handler, **kwargs)

    def as_model(self, model: Type[Any]) -> Any:
        """Return the row as a pydantic model."""
        if inspect.isclass(model):
            return model(**self.as_dict())
        else:
            raise ValueError("as_model expects a class as input")
