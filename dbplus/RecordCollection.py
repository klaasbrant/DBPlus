from __future__ import annotations

import inspect
import logging
from typing import Any, Dict, Generator, Iterator, List, Optional, Tuple, Type, Union

from dbplus.helpers import _debug
from dbplus.Record import Record
from dbplus.Statement import Statement


class RecordCollection:
    """A set of excellent rows from a query."""

    def __init__(self, rows: Generator[Record, None, None], stmt: Optional[Statement]) -> None:
        self._rows = rows
        self._all_rows: List[Record] = []
        self.pending: bool = True
        self._stmt = stmt
        self._logger = logging.getLogger("RecordCollection")

    def __repr__(self) -> str:
        return "<RecordCollection size={} pending={}>".format(len(self), self.pending)

    @_debug()
    def __str__(self) -> str:
        result: List[List[str]] = []
        data = self.all(as_tuple=True)
        if len(self) > 0:
            headers = self[0].as_dict()
            result.append([str(h) for h in headers.keys()])
            result.extend(list(map(str, row)) for row in data)
            lens = [list(map(len, row)) for row in result]
            field_lens = list(map(max, zip(*lens)))
            result.insert(1, ["-" * length for length in field_lens])
            format_string = "|".join("{%s:%s}" % item for item in enumerate(field_lens))
            return "\n".join(format_string.format(*row) for row in result)
        else:
            return "\n"  # empty set, nothing to report

    def __iter__(self) -> Generator[Record, None, None]:
        """Iterate over all rows, consuming the underlying generator
        only when necessary."""
        i = 0
        while True:
            # Other code may have iterated between yields,
            # so always check the cache.
            if i < len(self):
                yield self[i]
            else:
                # Throws StopIteration when done.
                # Prevent StopIteration bubbling from generator, following https://www.python.org/dev/peps/pep-0479/
                try:
                    yield next(self)
                except StopIteration:
                    return
            i += 1

    def next(self) -> Record:
        return self.__next__()

    def __next__(self) -> Record:
        try:
            nextrow = next(self._rows)
            self._all_rows.append(nextrow)
            return nextrow
        except StopIteration:
            self.pending = False
            raise StopIteration("RecordCollection contains no more rows.")

    def __getitem__(self, key: Union[int, slice]) -> Union[Record, RecordCollection]:
        """
        Argument: index or slice
        """
        # Verify what we are dealing with
        if isinstance(key, int):
            start = key
            stop = key + 1
        else:
            if isinstance(key, slice):
                start = key.start
                if start is None:  # used [:x] ?
                    start = 0
                stop = key.stop
            else:
                raise TypeError("Invalid argument type")

        # do we need to fetch extra to complete ?
        if self.pending:
            if start < 0 or stop is None:  # we must fetch all to evaluate
                fetcher = -1  # get it all
            else:
                fetcher = stop + 1  # stop premature (maybe)
            while fetcher == -1 or fetcher > len(self):  # do it
                try:
                    next(self)
                except StopIteration:
                    break

        if isinstance(key, slice):
            return RecordCollection(iter(self._all_rows[key]), None)
        else:
            if key < 0:  # Handle negative indices
                key += len(self)
            if key >= len(self):
                raise IndexError("Recordcollection index out of range")
            return self._all_rows[key]

    def __len__(self) -> int:
        return len(self._all_rows)

    def close(self) -> None:
        if self._stmt:
            self._stmt.close()

    def next_result(self, fetchall: bool = False) -> Optional[RecordCollection]:
        self._logger.info(f"Resolving next_result {self._stmt}")
        if self._stmt is None:
            raise RuntimeError("Cannot call next_result: no active statement")
        if self._stmt:
            Stmt = Statement(self._stmt._connection)
            next_rs = self._stmt.next_result()  # this the old stmt
            self._logger.info(f"got new rs from driver {next_rs}")
            Stmt._cursor = next_rs
            # Turn the cursor into RecordCollection
            rows = (Record(row) for row in Stmt)
            results = RecordCollection(rows, Stmt)
            # Fetch all results if desired otherwise we fetch when needed (open cursor can be locking problem!
            if fetchall:
                results.all()
            return results

    def as_DataFrame(self) -> Any:
        """A DataFrame representation of the RecordCollection."""
        try:
            from pandas import DataFrame
        except ImportError:
            raise NotImplementedError(
                "DataFrame needs Pandas... try pip install pandas"
            )
        return DataFrame(data=self.all(as_dict=True))

    def all(
        self,
        as_dict: bool = False,
        as_tuple: bool = False,
        as_json: bool = False,
    ) -> Union[List[Record], List[Dict[str, Any]], List[Tuple[Any, ...]], List[str]]:
        """Returns a list of all rows for the RecordCollection. If they haven't
        been fetched yet, consume the iterator and cache the results."""

        # By calling list it calls the __iter__ method for complete set
        rows = list(self)

        if as_dict:
            return [r.as_dict() for r in rows]

        elif as_tuple:
            return [r.as_tuple() for r in rows]

        elif as_json:
            return [r.as_json() for r in rows]

        return rows  # list of records

    def as_model(self, model: Type[Any]) -> List[Any]:
        """Return an array of pydantic models."""
        if inspect.isclass(model):
            rows = list(self)
            return [r.as_model(model) for r in rows]
        else:
            raise ValueError("as_model expects a class as input")

    def as_dict(self) -> List[Dict[str, Any]]:
        return self.all(as_dict=True)

    def as_tuple(self) -> List[Tuple[Any, ...]]:
        return self.all(as_tuple=True)

    def as_json(self) -> List[str]:
        return self.all(as_json=True)

    def one(self, default: Optional[Record] = None) -> Optional[Record]:
        """Returns a single record from the RecordCollection, ensuring there is data else returns `default`."""
        # Try to get a record, or return default.
        try:
            return self[0]
        except (IndexError, StopIteration):
            return default

    def scalar(self, default: Any = None) -> Any:
        """Returns the first column of the first row, or `default`."""
        try:
            return self[0][0]
        except (IndexError, StopIteration):
            return default
        finally:
            self.close()

    @property
    def description(self) -> Any:
        return self._stmt.description()
