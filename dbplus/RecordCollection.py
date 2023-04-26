from dbplus.Statement import Statement
from dbplus.helpers import _reduce_datetimes
import inspect
from dbplus.Record import Record
import logging
from dbplus.helpers import _debug


class RecordCollection(object):
    """A set of excellent rows from a query."""

    def __init__(self, rows, cursor):
        self._rows = rows
        self._all_rows = []
        self.pending = True
        self._cursor = cursor
        self._logger = logging.getLogger("RecordCollection")

    def __repr__(self):
        return "<RecordCollection size={} pending={}>".format(len(self), self.pending)

    def __str__(self):
        return self.__unicode__()

    @_debug()
    def __unicode__(self):
        result = []
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

    def __iter__(self):
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

    def next(self):
        return self.__next__()

    def __next__(self):
        try:
            #self._logger.debug("===== in next ====")
            nextrow = next(self._rows)
            #self._logger.debug("======= out next: row ===>",nextrow)
            self._all_rows.append(nextrow)
            return nextrow
        except StopIteration:
            self.pending = False
            #self._logger.debug("==== EOF ===")
            raise StopIteration("RecordCollection contains no more rows.")

    def __getitem__(self, key):
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
        if self.pending == True:
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

    def __len__(self):
        return len(self._all_rows)

    def __del__(self):
        pass
        #self.close()

    def close(self):
        if (
            self._cursor and self.pending
        ):  # if we have a cursor and cursor is not yet auto closed
            self._cursor.close()

    def next_result(self, fetchall=False):
        self._logger.info(f"Resolving next_result {self._cursor}")
        if self._cursor:
            cursor = Statement(self._cursor._connection)
            next_rs = self._cursor.next_result()  # this the old stmt
            self._logger.info(f"got new rs from driver {next_rs}")
            cursor._cursor = next_rs
            # Turn the cursor into RecordCollection
            rows = (Record(row) for row in cursor)
            results = RecordCollection(rows, cursor)
            # Fetch all results if desired otherwise we fetch when needed (open cursor can be locking problem!
            if fetchall:
                results.all()
            return results

    def export(self, format, **kwargs):
        pass

    def as_DataFrame(self):
        """A DataFrame representation of the RecordCollection."""
        try:
            from pandas import DataFrame
        except ImportError:
            raise NotImplementedError(
                "DataFrame needs Pandas... try pip install pandas"
            )
        return DataFrame(data=self.all(as_dict=True))

    def all(self, as_dict=False, as_tuple=False):
        """Returns a list of all rows for the RecordCollection. If they haven't
        been fetched yet, consume the iterator and cache the results."""

        # By calling list it calls the __iter__ method
        rows = list(self)

        if as_dict:
            return [r.as_dict() for r in rows]
        elif as_tuple:
            return [r.as_tuple() for r in rows]

        return rows  # list of records

    def as_model(self, model):
        """return an array of pydantic models"""
        if inspect.isclass(model):
            rows = list(self)
            return [r.as_model(model) for r in rows]
        else:
            raise ValueError("as_model excepts a class as input")

    def as_dict(self):
        return self.all(as_dict=True)

    def as_tuple(self):
        return self.all(as_tuple=True)

    def one(self, default=None, as_dict=False, as_tuple=False):
        """Returns a single record for the RecordCollection, ensuring that it
        is the only record, or returns `default`."""

        # Ensure that we don't have more than one row.
        try:
            test = self[1]  # force the cursor to the second rowpass
        except:
            pass

        if len(self) > 1:
            raise ValueError(
                "RecordCollection contained more than one row. "
                "Expects only one row when using "
                "RecordCollection.one"
            )

        # Try to get a record, or return default.
        if len(self) == 0:  # bummer, no rows at all
            return default
        record = self[0]
        # Cast and return.
        if as_dict:
            return record.as_dict()
        elif as_tuple:
            return record.as_tuple()
        else:
            return record

    def scalar(self, default=None):
        """Returns the first column of the first row, or `default`."""
        row = self.one()
        return row[0] if row else default
