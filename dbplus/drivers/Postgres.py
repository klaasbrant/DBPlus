from __future__ import absolute_import, division, print_function, with_statement
import psycopg2
import logging
from dbplus.drivers import BaseDriver
from dbplus.helpers import _debug

class PostgresDriver(BaseDriver):
    _cursor = None
    _con = None

    def __init__(self, timeout=0, charset="utf8", timezone="SYSTEM",  **params):
        #self._params = dict(charset=charset, time_zone = timezone, connect_timeout=timeout, autocommit=True)
        #print('>>>',params)
        self._driver = "Postgresql"
        self._logger = logging.getLogger('dbplus')
        self._params = params
        self._params["user"] = params.pop('uid')
        self._params["password"] = params.pop('pwd')
        self._params["database"] = params.pop('database')
        self._params["host"] = params.pop('host')
        self._params["port"] = int(params.pop('port'))

    def _get_server_version_info(self):
        return getattr(self._conn, "_server_version", None)

    def get_database(self):
        if "database" in self._params:
            return self._params["db"]
        self.execute("SELECT DATABASE()")
        return next(self.iterate())[0][1]

    def connect(self):
        #self.close()
        #self._logger.info("--> CONNECT {}".format(**self._params))
        try:
            self._conn = psycopg2.connect(**self._params)
            self._cursor = self._conn.cursor()
        except psycopg2.Error as ex:
            raise RuntimeError('Problem connection to database {}: {}'.format(self._params["database"],ex))

    def close(self):
        self.clear()
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def clear(self,Statement):
        if self._cursor is not None:
            Statement._cursor.close()
            Statement._cursor = None

    def error_code(self):
        return self._conn.errno()

    def error_info(self):
        return self._conn.error()

    def execute(self, Statement, sql, *params):
        try:
            Statement._cursor = self._conn.cursor()
            Statement._cursor.execute(sql, params)
            return Statement._cursor.rowcount
        except Exception as err:
            print(err)
            raise err

    def iterate(self, Statement):
        if Statement._cursor is None:
            raise StopIteration
        row = self._next_row(Statement)
        while (row):
            yield row
            row = self._next_row(Statement)
        #ibm_db.free_result(Statement._cursor)
        Statement._cursor = None


    def _next_row(self,Statement):
        columns = [desc[0] for desc in Statement._cursor.description]
        row = Statement._cursor.fetchone()
        if row is None:
            return row
        else:
            row = tuple([el.decode('utf-8') if type(el) is bytearray else el for el in row])
            return dict(zip(columns, row))

    def row_count(self):
        return self._conn.affected_rows()

    def last_insert_id(self, seq_name=None):
        return self._conn.insert_id() or None

    def begin_transaction(self):
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
            result = self._cursor.callproc(procname,tuple(*params))
            return list(result)
        except psycopg2.Error as err:
            print(err)
            raise err

    def get_placeholder(self):
        return "%s"
