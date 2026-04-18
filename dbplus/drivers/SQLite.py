import sqlite3

from dbplus.errors import DBError
from dbplus.drivers import BaseDriver


class DBDriver(BaseDriver):
    def __init__(self, timeout=5.0, **params):
        self._cursor = None
        self._error = None
        self._in_transaction = False

        auto_commit = params.pop("auto_commit", True)
        if auto_commit:
            params["isolation_level"] = None
        else:
            params["isolation_level"] = "EXCLUSIVE"

        database = params.pop("database")
        uid = params.pop("uid", None)
        pwd = params.pop("pwd", None)
        port = params.pop("port", None)
        host = params.pop("host", None)
        self._params = dict(database=database, timeout=timeout) | params

    def _get_server_version_info(self):
        return sqlite3.sqlite_version_info

    def get_database(self):
        return self._params["database"]

    @staticmethod
    def _row_factory(cursor, row):  # behold rows as dictionairies :-)
        d = {}
        for idx, col in enumerate(cursor.description):
            d[col[0]] = row[idx]
        return d

    def connect(self):
        self.close()
        try:
            self._conn = sqlite3.connect(**self._params)
            self._conn.row_factory = self._row_factory
        except Exception as ex:
            raise ex

    def close(self):
        self.clear()
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def clear(self):
        pass

    def error_code(self):
        return 1 if self._error else 0

    def error_info(self):
        return self._error

    def execute(self, statement, sql, *params):
        try:
            self._error = None
            if statement._cursor is None:
                statement._cursor = self._conn.cursor()
            statement._cursor = self._conn.execute(sql, params)
            self._cursor = statement._cursor
            if not self._in_transaction:
                self._conn.commit()
            return self.row_count()
        except Exception as ex:
            self._error = str(ex)
            raise DBError(
                "Error executing SQL: {}, with parameters: {} : {}".format(
                    sql, params, ex
                )
            )

    def execute_many(self, statement, sql, params):
        try:
            self._error = None
            statement._cursor = self._conn.cursor()
            statement._cursor.executemany(sql, params)
            self._cursor = statement._cursor
            if not self._in_transaction:
                self._conn.commit()
            return statement._cursor.rowcount
        except Exception as ex:
            self._error = str(ex)
            raise DBError(
                "Error executing SQL: {}, with parameters: {} : {}".format(
                    sql, params, ex
                )
            )

    def iterate(self, statement):
        if statement._cursor is None:
            raise StopIteration

        for row in statement._cursor:
            yield row

        self.clear()

    def row_count(self):
        if self._cursor is not None:
            return self._cursor.rowcount
        return 0

    def last_insert_id(self, seq_name=None):
        if self._cursor is not None:
            return self._cursor.lastrowid
        return None

    def begin_transaction(self):
        self._in_transaction = True
        self._conn.execute("BEGIN TRANSACTION")

    def commit(self):
        self._conn.commit()
        self._in_transaction = False

    def rollback(self):
        self._conn.rollback()
        self._in_transaction = False

    @staticmethod
    def get_placeholder():
        return "?"

    def escape_string(self, value):
        return "'" + value.replace("'", "''") + "'"

    def get_name(self):
        return "sqlite"

    def callproc(self, procname, *params):
        pass

    def describe_cursor(self, stmt):
        return stmt.description
