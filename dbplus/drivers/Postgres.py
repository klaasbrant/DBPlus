from __future__ import absolute_import, division, print_function, with_statement

import logging

import psycopg2

from dbplus.drivers import BaseDriver


class DBDriver(BaseDriver):
    def __init__(self, timeout=0, charset="utf8", timezone="SYSTEM", **params):
        self._cursor = None
        self._conn = None
        self._error = None
        self._last_cursor = None
        self._driver = "Postgresql"
        self._logger = logging.getLogger("dbplus")
        self._params = params
        self._params["user"] = params.pop("uid")
        self._params["password"] = params.pop("pwd")
        self._params["database"] = params.pop("database")
        self._params["host"] = params.pop("host")
        self._params["port"] = int(params.pop("port"))

    def _get_server_version_info(self):
        return getattr(self._conn, "_server_version", None)

    def get_database(self):
        if "database" in self._params:
            return self._params["database"]
        return None

    def connect(self):
        # self.close()
        # self._logger.info("--> CONNECT {}".format(**self._params))
        try:
            self._conn = psycopg2.connect(**self._params)
            self._cursor = self._conn.cursor()
        except psycopg2.Error as ex:
            raise RuntimeError(
                "Problem connection to database {}: {}".format(
                    self._params["database"], ex
                )
            )

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

    def execute(self, Statement, sql, *params):
        try:
            self._error = None
            Statement._cursor = self._conn.cursor()
            Statement._cursor.execute(sql, params)
            self._last_cursor = Statement._cursor
            return Statement._cursor.rowcount
        except Exception as err:
            self._error = str(err)
            self._logger.error("Error executing SQL: %s", err)
            raise err

    def execute_many(self, Statement, sql, params):
        try:
            self._error = None
            Statement._cursor = self._conn.cursor()
            Statement._cursor.executemany(sql, params)
            self._last_cursor = Statement._cursor
            return Statement._cursor.rowcount
        except Exception as err:
            self._error = str(err)
            self._logger.error("Error executing SQL: %s", err)
            raise RuntimeError(
                "Error executing SQL: {}, with parameters: {} : {}".format(
                    sql, params, err
                )
            )

    def iterate(self, Statement):
        if Statement._cursor is None:
            raise StopIteration
        row = self._next_row(Statement)
        while row:
            yield row
            row = self._next_row(Statement)
        Statement._cursor = None

    def _next_row(self, Statement):
        columns = [desc[0] for desc in Statement._cursor.description]
        row = Statement._cursor.fetchone()
        if row is None:
            return row
        else:
            row = tuple(
                [el.decode("utf-8") if type(el) is bytearray else el for el in row]
            )
            return dict(zip(columns, row))

    def row_count(self):
        if self._last_cursor is not None:
            return self._last_cursor.rowcount
        return 0

    def last_insert_id(self, seq_name=None):
        if seq_name and self._conn:
            cursor = self._conn.cursor()
            cursor.execute("SELECT currval(%s)", (seq_name,))
            return cursor.fetchone()[0]
        if self._last_cursor is not None:
            return getattr(self._last_cursor, "lastrowid", None)
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
        return "postgresql"

    def callproc(self, procname, *params):
        try:
            result = self._cursor.callproc(procname, tuple(*params))
            return list(result)
        except psycopg2.Error as err:
            self._error = str(err)
            self._logger.error("Error calling stored proc %s: %s", procname, err)
            raise err

    def describe_cursor(self, stmt):
        if stmt._cursor and stmt._cursor.description:
            return stmt._cursor.description
        return None

    def next_result(self, cursor):
        raise NotImplementedError("next_result is not supported for PostgreSQL")

    def get_placeholder(self):
        return "%s"
