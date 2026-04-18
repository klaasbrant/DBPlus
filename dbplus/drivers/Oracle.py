import logging

import oracledb as ora

from dbplus.errors import DBError
from dbplus.drivers import BaseDriver

# import cx_Oracle as ora


class DBDriver(BaseDriver):
    def __init__(self, timeout=0, charset="utf8", timezone="SYSTEM", **params):
        self._logger = logging.getLogger("dbplus")
        self._cursor = None
        self._conn = None
        self._uid = params.pop("uid")
        self._pwd = params.pop("pwd")
        self._database = params.pop("database")
        self._host = params.pop("host", "localhost")
        self._port = int(params.pop("port", 1521))
        self._dsn = f"{self._host}:{self._port}/{self._database}"
        self._logger.info("Oracle init dsn=%s", self._dsn)
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
            self._logger.error("Problems connecting to Oracle: %s", ex)
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

    def execute(self, statement, sql, **kwargs):
        self._logger.info("Oracle execute sql: {} params {}".format(sql, kwargs))
        try:
            statement._cursor = self._conn.cursor()
            statement._cursor.execute(sql, kwargs)
            return statement._cursor.rowcount
        except Exception as ex:
            raise DBError(
                f"Error executing SQL: {sql}, with parameters: {kwargs}\n{str(ex)}"
            ) from None

    def iterate(self, statement):
        if statement._cursor is None:
            raise StopIteration
        row = self._next_row(statement)
        while row:
            self._logger.info("Oracle next row: {} ".format(row))
            yield row
            row = self._next_row(statement)
        self._logger.info("Oracle no next row")
        statement._cursor = None

    def clear(self):
        if self._cursor is not None:
            # ora.free_result(...)
            self._cursor.close()
            self._cursor = None

    def next_result(self, cursor):
        raise NotImplementedError("next_result is not supported for Oracle")

    def last_insert_id(self, seq_name=None):
        pass

    def begin_transaction(self):
        self._logger.debug(">>> START TRX")
        self._conn.autocommit = False

    def commit(self):
        self._logger.debug("<<< COMMIT")
        self._conn.commit()
        self._conn.autocommit = True

    def rollback(self):
        self._logger.debug(">>> ROLLBACK")
        self._conn.rollback()
        self._conn.autocommit = True

    def get_placeholder(self):
        return ":"

    def get_name(self):
        return "oracle"

    def execute_many(self, statement, sql, params):
        try:
            statement._cursor = self._conn.cursor()
            statement._cursor.executemany(sql, params)
            return statement._cursor.rowcount
        except Exception as ex:
            raise DBError(
                f"Error executing SQL: {sql}, with parameters: {params}\n{str(ex)}"
            ) from None

    def describe_cursor(self, cursor):
        if cursor and cursor.description:
            return cursor.description
        return None
