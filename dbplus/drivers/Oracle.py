from __future__ import absolute_import, division, print_function, with_statement

import logging

import oracledb as ora

from dbplus.Database import DBError
from dbplus.drivers import BaseDriver

# import cx_Oracle as ora


class DBDriver(BaseDriver):
    def __init__(self, timeout=0, charset="utf8", timezone="SYSTEM", **params):
        # self._params = dict(charset=charset, time_zone = timezone, connect_timeout=timeout, autocommit=True)
        self._logger = logging.getLogger("dbplus")
        self._logger.info("Oracle init params {}".format(params))
        self._uid = params.pop("uid")
        self._pwd = params.pop("pwd")
        self._database = params.pop("database")
        self._host = params.pop("host", "localhost")
        self._port = int(params.pop("port", 1521))
        self._dsn = f"{self._host}:{self._port}/{self._database}"
        # self._dsn = ora.makedsn(
        #     self._host, self._port, self._database
        # )  # this fails? DPY-6003: SID "freepdb1" is not registered with the listener at host "localhost" port 1521. (Similar to ORA-12505)

    def connect(self):
        try:
            self._logger.info(f"Oracle connect {self._dsn=}")
            self._conn = ora.connect(
                user=self._uid,
                password=self._pwd,
                dsn=self._dsn,
            )
            self._cursor = self._conn.cursor()
            self._logger.info("Connect OK!")
        except Exception as ex:
            print("Problems connecting to Oracle: {}".format(str(ex)))
            raise ex from None

    def close(self):
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def error_code(self):
        pass

    def error_info(self):
        pass

    def callproc(self, procname, *params):
        try:
            _cursor = self._conn.cursor()
            result = _cursor.callproc(procname, tuple(*params))
            return list(result[0:])
        except Exception as ex:
            raise DBError(
                "Error calling stored proc: {}, with parameters: {} \n{}".format(
                    procname, params, str(ex)
                )
            ) from None

    def execute(self, Statement, sql, **kwargs):
        self._logger.info("Oracle execute sql: {} params {}".format(sql, kwargs))
        try:
            Statement._cursor = self._conn.cursor()
            Statement._cursor.execute(sql, kwargs)
            return Statement._cursor.rowcount
        except Exception as ex:
            raise DBError(
                f"Error executing SQL: {sql}, with parameters: {kwargs}\n{str(ex)}"
            ) from None

    def iterate(self, Statement):
        if Statement._cursor is None:
            raise StopIteration
        row = self._next_row(Statement)
        while row:
            self._logger.info("Oracle next row: {} ".format(row))
            yield row
            row = self._next_row(Statement)
        # ibm_db.free_result(Statement._cursor)
        self._logger.info("Oracle no next row")
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

    def clear(self):
        if self._cursor is not None:
            # ora.free_result(...)
            self._cursor.close()
            self._cursor = None

    def next_result(self, cursor):
        return ora.next_result(cursor)

    def last_insert_id(self, seq_name=None):
        pass

    def begin_transaction(self):
        self._logger.debug(">>> START TRX")
        ora.autocommit(self._conn, ora.SQL_AUTOCOMMIT_OFF)

    def commit(self):
        self._logger.debug("<<< COMMIT")
        ora.commit(self._conn)
        ora.autocommit(self._conn, ora.SQL_AUTOCOMMIT_ON)

    def rollback(self):
        self._logger.debug(">>> ROLLBACK")
        ora.rollback(self._conn)
        ora.autocommit(self._conn, ora.SQL_AUTOCOMMIT_ON)

    def get_placeholder(self):
        return ":"

    def get_name(self):
        return self._driver
