from __future__ import absolute_import, division, print_function, with_statement

import sqlite3
import warnings

from dbplus.drivers import BaseDriver


class SQLiteDriver(BaseDriver):
    _cursor = None
    _error = None

    def __init__(self, timeout=5.0, **params):
        #self._logger = params.pop("logger")
        #self._platform = SQLitePlatform(self)

        auto_commit = True
        if auto_commit:
            params["isolation_level"] = None
        else:
            params["isolation_level"] = "EXCLUSIVE"

        database = params.pop('database')
        self._params = dict(database=database, timeout=timeout)

    def _get_server_version_info(self):
        return sqlite3.sqlite_version_info

    def get_database(self):
        return self._params["database"]

    @staticmethod
    def _row_factory(cursor, row): #behold rows as dictionairies :-)
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
        # if self._cursor is not None:
        #     self._cursor.close()
        #     self._cursor = None
        pass

    def error_code(self):
        return 1 if self._error else 0

    def error_info(self):
        return self._error

    def execute(self, Statement, sql, *params):
        try:
            #self._log(sql, *params)
            print(sql,params)
            self._error = None
            Statement._cursor = self._conn.execute(sql, params)
            return self.row_count()
        except Exception as ex:
            self._error = str(ex)
            raise RuntimeError("Error executing SQL: {}, with parameters: {} : {}".format(sql, params,ex))

    def iterate(self, Statement):
        if Statement._cursor is None:
            raise StopIteration

        for row in Statement._cursor:
            yield row

        self.clear()

    def row_count(self):
        return getattr(self._cursor, "rowcount", 0)

    def last_insert_id(self, seq_name=None):
        return getattr(self._cursor, "lastrowid", None)

    def begin_transaction(self):
        self._conn.execute("BEGIN TRANSACTION")

    def commit(self):
        #self._log("COMMIT")
        self._conn.commit()

    def rollback(self):
        #self._log("ROLLBACK")
        self._conn.rollback()

    @staticmethod
    def get_placeholder():
        return "?"

    def escape_string(self, value):
        return "'" + value.replace("'", "''") + "'"

    def get_name(self):
        return "sqlite"

    def callproc(self, procname, *params):
        pass
        