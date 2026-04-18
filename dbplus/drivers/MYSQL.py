import logging

import mysql.connector
from mysql.connector import errorcode

from dbplus.drivers import BaseDriver


class DBDriver(BaseDriver):
    def __init__(
        self, timeout=0, charset="utf8", timezone="SYSTEM", port=3306, **params
    ):
        self._cursor = None
        self._conn = None
        self._error = None
        self._last_cursor = None
        self._logger = logging.getLogger("dbplus")
        self._params = dict()
        self._params["user"] = params.pop("uid")
        self._params["password"] = params.pop("pwd")
        self._params["database"] = params.pop("database")
        self._params["host"] = params.pop("host", "localhost")
        self._params["port"] = int(params.pop("port", None) or port)

    def _get_server_version_info(self):
        return getattr(self._conn, "_server_version", None)

    def get_database(self):
        if "database" in self._params:
            return self._params["database"]
        return None

    def connect(self):
        try:
            self._conn = mysql.connector.connect(**self._params)
            self._cursor = self._conn.cursor()
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                self._logger.error("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                self._logger.error("Database does not exist")
            else:
                self._logger.error("MySQL connection error: %s", err)
            raise err

    def close(self):
        self.clear()
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def clear(self):
        if self._cursor is not None:
            self._cursor.close()
            self._cursor = None

    def error_code(self):
        return 1 if self._error else 0

    def error_info(self):
        return self._error

    def execute(self, statement, sql, *params):
        try:
            self._error = None
            statement._cursor = self._conn.cursor()
            statement._cursor.execute(sql, params)
            self._last_cursor = statement._cursor
            return statement._cursor.rowcount
        except mysql.connector.Error as err:
            self._error = str(err)
            self._logger.error("Error executing SQL: %s", err)
            raise err

    def execute_many(self, statement, sql, params):
        try:
            self._error = None
            statement._cursor = self._conn.cursor()
            statement._cursor.executemany(sql, params)
            self._last_cursor = statement._cursor
            return statement._cursor.rowcount
        except mysql.connector.Error as err:
            self._error = str(err)
            self._logger.error("Error executing SQL: %s", err)
            raise RuntimeError(
                "Error executing SQL: {}, with parameters: {} : {}".format(
                    sql, params, err
                )
            )

    def iterate(self, statement):
        if statement._cursor is None:
            raise StopIteration
        row = self._next_row(statement)
        while row:
            yield row
            row = self._next_row(statement)
        statement._cursor = None

    def row_count(self):
        if self._last_cursor is not None:
            return self._last_cursor.rowcount
        return 0

    def last_insert_id(self, seq_name=None):
        if self._last_cursor is not None:
            return self._last_cursor.lastrowid
        return None

    def begin_transaction(self):
        if self._cursor is None:
            self._cursor = self._conn.cursor()
        self._cursor.execute("START TRANSACTION")

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def escape_string(self, value):
        return self._conn.literal(value)

    def get_name(self):
        return "mysql"

    def callproc(self, procname, *params):
        try:
            result = self._cursor.callproc(procname, tuple(*params))
            return list(result)
        except mysql.connector.Error as err:
            self._error = str(err)
            self._logger.error("Error calling stored proc %s: %s", procname, err)
            raise err

    def describe_cursor(self, cursor):
        if cursor and cursor.description:
            return cursor.description
        return None

    def next_result(self, cursor):
        raise NotImplementedError("next_result is not supported for MySQL")

    def get_placeholder(self):
        return "%s"
