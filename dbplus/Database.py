from __future__ import annotations

import csv
import logging
import os
import re
from contextlib import contextmanager
from importlib import import_module
from typing import Any, Generator, Iterator, List, Optional, Sequence, Tuple, Union

from dbplus.errors import DBError
from dbplus.helpers import (
    _parse_database_url,
    _validate_identifier,
    guess_type,
)
from dbplus.Record import Record
from dbplus.RecordCollection import RecordCollection
from dbplus.Statement import Statement


class Database:
    """A generic Database connection."""

    def __init__(self, db_url: Optional[str] = None, **kwargs: Any) -> None:
        self._logger = logging.getLogger("dbplus")
        self._transaction_active: bool = False
        self._transaction_context_active: bool = False
        self._driver: Any = None
        # If no db_url was provided, we fallback to DATABASE_URL in environment variables
        self.db_url: Optional[str] = db_url or os.environ.get("DATABASE_URL")
        db_parameters = _parse_database_url(self.db_url)
        if db_parameters is None:  # that means parsing failed!!
            raise ValueError("Database url is missing or has invalid format")
        self.db_driver: str = db_parameters.pop("driver").upper()
        try:
            driver_module = import_module(f"dbplus.drivers.{self.db_driver}")
            self._driver = driver_module.DBDriver(**db_parameters)
            self._logger.info(f"--> Using Database driver: {self.db_driver}")
            self.open()
            self._logger.info(f"--> Database connected")

        except Exception as e:
            raise ValueError(
                f"DBPlus has trouble initializing the {self.db_driver} driver: {e}"
            ) from e

    def open(self) -> None:
        """Opens the connection to the Database."""
        if not self.is_connected():
            self._driver.connect()

    def close(self) -> None:
        """Closes the connection to the Database."""
        if self.is_connected():
            self._driver.close()

    def __del__(self) -> None:
        if self._driver is not None:
            try:
                self._driver.close()  # Say goodbye and
                del self._driver  # allow database interface to gracefully exit
            except Exception:
                pass

    def __enter__(self) -> Database:
        return self

    def __exit__(self, exc: Any, val: Any, traceback: Any) -> None:
        self.close()

    def __repr__(self) -> str:
        safe_url = re.sub(r'://([^:]*):([^@]*)@', r'://\1:***@', self.db_url) if self.db_url else None
        return f"<DBPlus {self.db_driver} database url: {safe_url}), state: connected={self.is_connected()}>"

    ################# Experimental feature, driver might offer extra options ############################
    def __getattr__(self, name: str) -> Any:
        if self._driver is not None and hasattr(self._driver, name) and callable(getattr(self._driver, name)):
            return getattr(self._driver, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

    def get_driver(self) -> Any:
        return self._driver

    def is_connected(self) -> bool:
        return self._driver.is_connected()

    def ensure_connected(self) -> None:
        if not self.is_connected():
            self.open()

    #########################################################################################################################

    def query(self, query: str, *args: Any, **kwargs: Any) -> RecordCollection:
        """Executes the given SQL query against the Database. Parameters
        can, optionally, be provided. Returns a RecordCollection, which can be
        iterated over to get result rows as dictionaries.
        """
        self.ensure_connected()
        stmt = Statement(self)
        stmt.execute(query, *args, **kwargs)

        # Turn the cursor into RecordCollection
        rows = (Record(row) for row in stmt)
        results = RecordCollection(rows, stmt)
        return results

    #########################################################################################################################

    def execute(self, sql: str, *args: Any, **kwargs: Any) -> int:
        self._logger.debug(f"--> Execute: {sql} with arguments [{str(args)}]")
        self.ensure_connected()
        modified = Statement(self).execute(
            sql, *args, **kwargs
        )  # GC will purge Statement
        return modified

    #########################################################################################################################

    def callproc(self, procname: str, *params: Any) -> Optional[Tuple[RecordCollection, Tuple[Any, ...]]]:
        self._logger.info(
            f"--> Calling Stored proc: {procname} with arguments [{str(params)}]"
        )
        self.ensure_connected()
        result = self._driver.callproc(procname, *params)
        if result:
            cursor = Statement(self)
            cursor._cursor = result[0]
            rows = (Record(row) for row in cursor)
            return (
                RecordCollection(rows, cursor),
                result[1:],
            )
        return None

    #########################################################################################################################

    def last_insert_id(self, seq_name: Optional[str] = None) -> Optional[int]:
        self.ensure_connected()
        return self._driver.last_insert_id(seq_name)

    def error_code(self) -> Any:
        self.ensure_connected()
        return self._driver.error_code()

    def error_info(self) -> Any:
        self.ensure_connected()
        return self._driver.error_info()

    #########################################################################################################################

    @contextmanager
    def transaction(self) -> Generator[None, None, None]:
        """Returns with block for transaction. Call ``commit`` or ``rollback`` at end as appropriate."""
        self._logger.info("--> Begin transaction block")
        self._transaction_context_active = True
        try:
            self.begin_transaction()
        except Exception:
            # begin_transaction() failed before the try/yield block was entered,
            # so reset the flag here — otherwise it stays True permanently and
            # every subsequent call raises "Nested transactions is not supported!"
            self._transaction_context_active = False
            raise
        try:
            yield
            self._transaction_context_active = False  # We return here when with block ends
            self.commit()
            self._logger.info("--> Transaction committed")
        except Exception as ex:
            self._logger.info("--> Transaction rollback because failure in transaction")
            self._transaction_context_active = False
            self.rollback()
            raise

    def begin_transaction(self) -> None:
        self.ensure_connected()
        if self._transaction_active:
            raise DBError("Nested transactions is not supported!")
        self._transaction_active = True
        try:
            self._driver.begin_transaction()
        except Exception:
            # Driver call failed after we set the flag — reset it so the next
            # call to begin_transaction() is not permanently blocked.
            self._transaction_active = False
            raise

    def commit(self) -> None:
        if self._transaction_context_active:
            raise DBError("Logic error: Commit not allowed within transaction block!")
        if not self._transaction_active:
            raise DBError("Logic error: Commit on never started transaction?")
        self.ensure_connected()
        self._driver.commit()
        self._transaction_active = False

    def rollback(self) -> None:
        if self._transaction_context_active:
            raise DBError(
                "Rollback called within transaction block, forcing DBError..."
            )
        if not self._transaction_active:
            raise DBError("Logic error: Rollback on never started transaction?")
        self.ensure_connected()
        self._transaction_active = False
        self._driver.rollback()

    def is_transaction_active(self) -> bool:
        return self._transaction_active

    #########################################################################################################################

    def copy_to(
        self,
        file: str,
        table: str,
        sep: str = "\t",
        null: str = "\x00",
        columns: Optional[List[str]] = None,
        header: bool = False,
        append: bool = False,
        recsep: str = "\n",
        **kwargs: Any,
    ) -> int:
        _validate_identifier(table)
        if columns is not None:
            for c in columns:
                _validate_identifier(c)
        col = "*" if columns is None else ",".join(columns)
        sql_query = "select {} from {}".format(col, table)
        cursor = Statement(self)
        cursor.execute(sql_query)
        row_count = 0
        mode = "a" if append else "w"
        writer = None
        with open(file, mode) as csvfile:
            for row in cursor:
                row_count += 1
                if writer is None:
                    csv_columns = row.keys()
                    writer = csv.DictWriter(
                        csvfile,
                        fieldnames=csv_columns,
                        lineterminator=recsep,
                        restval="",
                        delimiter=sep,
                        quoting=csv.QUOTE_MINIMAL,
                        **kwargs,
                    )
                    if header:
                        writer.writeheader()

                row_copy = dict(row)
                for key in row_copy.keys():
                    if row_copy[key] is None:
                        row_copy[key] = null
                writer.writerow(row_copy)

            if writer is None and header and columns is not None:
                writer = csv.DictWriter(
                    csvfile,
                    fieldnames=columns,
                    lineterminator=recsep,
                    restval="",
                    delimiter=sep,
                    quoting=csv.QUOTE_MINIMAL,
                    **kwargs,
                )
                writer.writeheader()
        return row_count

    def copy_from(
        self,
        file: str,
        table: str,
        sep: str = "\t",
        recsep: str = "\n",
        header: bool = False,
        null: str = "\x00",
        batch: int = 500,
        columns: Optional[List[str]] = None,
        **kwargs: Any,
    ) -> int:
        _validate_identifier(table)
        if columns is not None:
            for c in columns:
                _validate_identifier(c)
        col = "" if columns is None else "({})".format(",".join(columns))
        row_count = 0
        queue: List[Tuple[Any, ...]] = list()
        with open(file, "r") as csvfile:
            reader = csv.reader(
                csvfile,
                delimiter=sep,
                lineterminator=recsep,
                quoting=csv.QUOTE_MINIMAL,
                **kwargs,
            )
            if header:
                next(reader)
            for row in reader:
                row_count += 1
                values = tuple(None if x == null else x for x in row)
                queue.append(values)
                if len(queue) >= batch:
                    self._insert_values(table, col, queue)
                    queue = list()
            if len(queue) > 0:
                self._insert_values(table, col, queue)
        return row_count

    def _insert_values(self, table: str, col: str, queue: List[Tuple[Any, ...]]) -> None:
        _validate_identifier(table)
        placeholder = self.get_driver().get_placeholder()
        ncols = len(queue[0])
        if placeholder == ":":
            # Oracle uses numbered bind variables (:1, :2, ...)
            values_literal = "({})".format(
                ",".join(f":{i+1}" for i in range(ncols))
            )
        else:
            values_literal = "({})".format(
                ",".join([placeholder] * ncols)
            )
        sql_query = "insert into {} {} values {}".format(table, col, values_literal)
        stmt = Statement(self)
        self.get_driver().execute_many(stmt, sql_query, queue)

    #########################################################################################################################
