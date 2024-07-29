import csv
import logging
import os
from contextlib import contextmanager
from importlib import import_module

from dbplus.helpers import (
    _debug,
    _parse_database_url,
    _reduce_datetimes,
    fix_sql_type,
    guess_type,
)
from dbplus.Record import Record
from dbplus.RecordCollection import RecordCollection
from dbplus.Statement import Statement


class DBError(Exception):
    pass


class Database(object):
    """A generic Database connection."""

    def __init__(self, db_url=None, **kwargs):
        self._logger = logging.getLogger("dbplus")
        self._transaction_active = False
        self._transactioncontext_active = False
        self._driver = None
        # If no db_url was provided, we fallback to DATABASE_URL in environment variables
        self.db_url = db_url or os.environ.get("DATABASE_URL")
        dbParameters = _parse_database_url(self.db_url)
        if dbParameters is None:  # that means parsing failed!!
            raise ValueError("Database url is missing or has invalid format")
        self.db_driver = dbParameters.pop("driver").upper()
        try:
            driver_module = import_module(f"dbplus.drivers.{self.db_driver}")
            self._driver = driver_module.DBDriver(**dbParameters)
            self._logger.info(f"--> Using Database driver: {self.db_driver}")
            self.open()
            self._logger.info(f"--> Database connected")

        except:
            raise ValueError(
                f"DBPlus has trouble initializing the {self.db_driver} driver... mission aborted!"
            )

    def open(self):
        """Opens the connection to the Database."""
        if not self.is_connected():
            self._driver.connect()

    def close(self):
        """Closes the connection to the Database."""
        if self.is_connected():
            self._driver.close()

    def __del__(self):
        if self._driver is not None:
            try:
                self._driver.close()  # Say goodbye and
                del self._driver  # allow database interface to gracefully exit
            except:
                pass

    def __enter__(self):
        return self

    def __exit__(self, exc, val, traceback):
        self.close()

    def __repr__(self):
        return f"<DBPlus {self.db_type} database url: {self.db_url}), state: connected={self.is_connected()}>"

    ################# Experimental feature, driver might offer extra options ############################
    def __getattr__(self, name):
        def method(*args, **kw):
            if hasattr(self._driver, name) and callable(getattr(self._driver, name)):
                return getattr(self._driver, name)(*args, **kw)

        return method

    def get_driver(self):
        return self._driver

    def is_connected(self):
        return self._driver.is_connected()

    def ensure_connected(self):
        if not self.is_connected():
            self.open()

    #########################################################################################################################

    # def query(self, query, fetchall=False,*args, **kwargs):
    def query(self, query, *args, **kwargs):
        """Executes the given SQL query against the Database. Parameters
        can, optionally, be provided. Returns a RecordCollection, which can be
        iterated over to get result rows as dictionaries.
        """
        self.ensure_connected()
        cursor = Statement(self)
        cursor.execute(query, *args, **kwargs)

        # Turn the cursor into RecordCollection
        rows = (Record(row) for row in cursor)
        results = RecordCollection(rows, cursor)

        # Fetch all results if desired otherwise we fetch when needed (open cursor can be locking problem!
        # if fetchall:
        #    results.all()
        # do not delete de cursor it is needed in the layer below
        return results

    #########################################################################################################################

    def execute(self, sql, *args, **kwargs):
        self._logger.info(f"--> Execute: {sql} with arguments [{str(args)}]")
        self.ensure_connected()
        modified = Statement(self).execute(
            sql, *args, **kwargs
        )  # GC will purge Statement
        return modified

    #########################################################################################################################

    def callproc(self, procname, *params):
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
            )  #  replace stmt by recordcollection
        return None  # can happen like proc not found or no parameter proc that returns nothing (bad practice)

    #########################################################################################################################

    def last_insert_id(self, seq_name=None):
        self.ensure_connected()
        return self._driver.last_insert_id(seq_name)

    def error_code(self):
        self.ensure_connected()
        return self._driver.error_code()

    def error_info(self):
        self.ensure_connected()
        return self._driver.error_info()

    #########################################################################################################################

    @contextmanager
    def transaction(self):
        """Returns with block for transaction. Call ``commit`` or ``rollback`` at end as appropriate."""
        self._logger.info("--> Begin transaction block")
        self._transactioncontext_active = True
        self.begin_transaction()
        try:
            yield
            self._transactioncontext_active = False
            self.commit()
            self._logger.info("--> Transaction committed")
        except Exception as ex:
            self._logger.info("--> Transaction rollback because failure in transaction")
            self._transactioncontext_active = False
            self.rollback()
            raise ex  # allow exception to propagate, but transaction has been aborted

    def begin_transaction(self):
        self.ensure_connected()
        if self._transaction_active == True:
            raise DBError("Nested transactions is not supported!")
        self._transaction_active = True
        self._driver.begin_transaction()

    def commit(self):
        if self._transactioncontext_active:
            raise DBError("Logic error: Commit not allowed within transaction block!")
        if self._transaction_active == False:
            raise DBError("logic error: Commit on never started transaction?")
        self.ensure_connected()
        self._driver.commit()
        self._transaction_active = False

    def rollback(self):
        if self._transactioncontext_active:
            raise DBError(
                "Rollback called within transaction block, forcing DBError..."
            )
        if self._transaction_active == False:
            raise DBError("logic error: Rollback on never started transaction?")
        self.ensure_connected()
        self._transaction_active = False
        self._driver.rollback()

    def is_transaction_active(self):
        return self._transaction_active

    #########################################################################################################################

    def copy_to(
        self,
        file,
        table,
        sep="\t",
        null="\x00",
        columns=None,
        header=False,
        append=False,
        recsep="\n",
        **kwargs,
    ):
        col = "*" if columns == None else ",".join(columns)
        sql_query = "select {} from {}".format(col, table)
        cursor = Statement(self)
        cursor.execute(sql_query)
        row_count = 0
        mode = "a" if append else "w"
        with open(file, mode) as csvfile:
            for row in cursor:
                row_count += 1
                if row_count == 1:
                    csv_columns = row.keys()
                    # csv_columns = [each_column_name.upper() for each_column_name in csv_columns]
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

                for key in row.keys():
                    if row[key] == None:
                        row[key] = null
                writer.writerow(row)
        return row_count

    def copy_from(
        self,
        file,
        table,
        sep="\t",
        recsep="\n",
        header=False,
        null="\x00",
        batch=500,
        columns=None,
        **kwargs,
    ):
        col = "" if columns == None else "({})".format(",".join(columns))
        row_count = 0
        queue = list()
        # values = list()
        with open(file, "r") as csvfile:
            reader = csv.reader(
                csvfile,
                delimiter=sep,
                lineterminator=recsep,
                quoting=csv.QUOTE_MINIMAL,
                **kwargs,
            )
            if header:
                xh = next(reader)
                # print(f'Header: {", ".join(xh)}')
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

    def _insert_values(self, table, col, queue):
        values_literal = "({})".format(
            ",".join([self.get_driver().get_placeholder()] * len(queue[0]))
        )
        sql_query = "insert into {} {} values {}".format(table, col, values_literal)
        self.get_driver().execute_many(self, sql_query, queue)

    #########################################################################################################################
