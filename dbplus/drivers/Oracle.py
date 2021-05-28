from __future__ import absolute_import, division, print_function, with_statement
import cx_Oracle
import logging
from dbplus.drivers import BaseDriver
from dbplus.helpers import _debug

class OracleDriver(BaseDriver):

    def __init__(self, timeout=0, charset="utf8", timezone="SYSTEM",  **params):
        #self._params = dict(charset=charset, time_zone = timezone, connect_timeout=timeout, autocommit=True)
        self._logger = logging.getLogger('dbplus')
        self._logger.info("Oracle init params {}".format(params))
        self._params = dict()
        self._params["user"] = params.pop('uid')
        self._params["password"] = params.pop('pwd')
        self._params["database"] = params.pop('database')
        self._params["host"] = params.pop('host')
        self._params["port"] = int(params.pop('port'))


    def connect(self):
        #self.close()
        try:
            dsn = cx_Oracle.makedsn(self._params["host"],self._params["port"],sid=self._params["database"])
            self._logger.info("Oracle connect dsn={}".format(dsn))
            self._conn = cx_Oracle.connect(user=self._params["user"],password=self._params["password"],dsn=dsn)
            self._cursor = self._conn.cursor()
            self._logger.info("Connect OK!")
        except Exception as ex:
            print("BUMMER")    
            raise ex
    

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
            raise RuntimeError("Error calling stored proc: {}, with parameters: {} \n{}".format(procname, params,str(ex)))


    def execute(self, Statement, sql, **kwargs):
        self._logger.info("Oracle execute sql: {} params {}".format(sql,kwargs))
        try:
            Statement._cursor = self._conn.cursor()
            return Statement._cursor.execute(sql, kwargs)
        except Exception as ex:
            raise RuntimeError("Error executing SQL: {}, with parameters: {} \n{}".format(sql, kwargs,str(ex)))


    def iterate(self, Statement):
        if Statement._cursor is None:
            raise StopIteration
        row = self._next_row(Statement)
        while (row):
            self._logger.info("Oracle next row: {} ".format(row)) 
            yield row
            row = self._next_row(Statement)
        #ibm_db.free_result(Statement._cursor)
        self._logger.info("Oracle no next row")
        Statement._cursor = None


    def _next_row(self,Statement):
        columns = [desc[0] for desc in Statement._cursor.description]
        row = Statement._cursor.fetchone()
        if row is None:
            return row
        else:
            row = tuple([el.decode('utf-8') if type(el) is bytearray else el for el in row])
            return dict(zip(columns, row))


    def clear(self,Statement):
        if Statement._cursor is not None:
            #pass
            #cx_Oracle.free_result(Statement._cursor)
            Statement._cursor = None


    def next_result(self,cursor):
        return cx_Oracle.next_result(cursor)


    def last_insert_id(self, seq_name=None):
        pass


    def begin_transaction(self):
        self._logger.debug(">>> START TRX")
        cx_Oracle.autocommit(self._conn, cx_Oracle.SQL_AUTOCOMMIT_OFF)


    def commit(self):
        self._logger.debug("<<< COMMIT")
        cx_Oracle.commit(self._conn)
        cx_Oracle.autocommit(self._conn, cx_Oracle.SQL_AUTOCOMMIT_ON)


    def rollback(self):
        self._logger.debug(">>> ROLLBACK")
        cx_Oracle.rollback(self._conn)
        cx_Oracle.autocommit(self._conn, cx_Oracle.SQL_AUTOCOMMIT_ON)


    def get_placeholder(self):
        return ":"


    def get_name(self):
        return self._driver
