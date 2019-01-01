from __future__ import absolute_import, division, print_function, with_statement
import ibm_db
import logging
from dbplus.drivers import BaseDriver
from dbplus.helpers import _debug

class DB2Driver(BaseDriver):
    @_debug()
    def __init__(self, **params):
    # timeout=5, charset="utf8"
        self._driver = "DB2"
        self._params = params    
        self._logger = logging.getLogger('dbplus')

        self._database = self._params.pop("database",None)
        if self._database is None:
            raise RuntimeError("Database name missing or incorrect")            
        
        self._uid = self._params.pop("uid",None)
        self._pwd = self._params.pop("pwd",None)
        
        self._host = self._params.pop("host",None)
        if self._host and self._host.upper() == 'LOCALHOST':
            self._host = None
        self._port = self._params.pop("port",None)
        if self._port is None:
            self._port = 50000

        if self._host and ((self._uid is None) or (self._pwd is None)):
            raise RuntimeError("Userid and/or Password missing")

        if self._host:
            conn_string='DATABASE={};UID={};PWD={};HOSTNAME={};PORT={};PROTOCOL=TCPIP;'.format(self._database,self._uid,self._pwd,self._host,self._port)
        else:
            if not self._uid:
                self._uid=''
                self._pwd=''
            conn_string='DSN={};UID={};PWD={};'.format(self._database,self._uid,self._pwd)
        
        self._conn_string = conn_string

    @_debug()
    def connect(self):
        options = { ibm_db.ATTR_CASE : ibm_db.CASE_LOWER,ibm_db.SQL_ATTR_AUTOCOMMIT : ibm_db.SQL_AUTOCOMMIT_ON}
        self._logger.info("--> PCONNECT {} - {}".format(self._conn_string,options))
        try:
            self._conn = ibm_db.pconnect(self._conn_string, "", "",options)
        except Exception as ex:
            self._error = ibm_db.conn_errormsg()
            raise RuntimeError('Problem connection to database {} : {}'.format(self._database,ex))
    
    @_debug()
    def close(self):
        if self._conn is not None:
            ibm_db.close(self._conn)
            self._conn = None
    
    @_debug()
    def error_code(self):
        return ibm_db.stmt_error()

    @_debug()
    def error_info(self):
        return ibm_db.stmt_errormsg()

    @_debug()
    def callproc(self, procname, *params):
        try:
            result = ibm_db.callproc(self._conn, procname, tuple(*params))
            return list(result[1:])
        except Exception as ex:
            self._error = ibm_db.stmt_errormsg()
            raise RuntimeError("Error calling stored proc: {}, with parameters: {} : {}".format(procname, params,ex))

    @_debug()
    def execute(self, Statement, sql, *params):
        try:
            stmt = ibm_db.prepare(self._conn, sql)
            ibm_db.execute(stmt, params)
            Statement._cursor = stmt
            return ibm_db.num_rows(stmt)
        except Exception as ex:
            self._error = ibm_db.stmt_errormsg()
            raise RuntimeError("Error executing SQL: {}, with parameters: {} : {}".format(sql, params,ex))

    @_debug()
    def iterate(self, Statement):
        if Statement._cursor is None:
            raise StopIteration
        row = ibm_db.fetch_assoc(Statement._cursor)
        while (row):
            yield row
            row = ibm_db.fetch_assoc(Statement._cursor)
        ibm_db.free_result(Statement._cursor)
        Statement._cursor = None

    @_debug()
    def clear(self,Statement):
        if Statement._cursor is not None:
            pass
            #ibm_db.free_result(Statement._cursor)
            #Statement._cursor = None

    @_debug()
    def next_result(self,cursor):
        return ibm_db.next_result(cursor)

    @_debug()
    def last_insert_id(self, seq_name=None):
        # Code like in ibm_dbi
        operation = 'SELECT IDENTITY_VAL_LOCAL() FROM SYSIBM.SYSDUMMY1'
        try:
            stmt_handler = ibm_db.prepare(self._conn, operation)
            ibm_db.execute(stmt_handler)
            row = ibm_db.fetch_tuple(stmt_handler)
            if row[0] is not None:
                identity_val = int(row[0])
            else:
                identity_val = None
        except Exception as ex:
            self._error = ibm_db.stmt_errormsg()
            raise RuntimeError("Error retrieving identity_val_local() : {}".format(ex))
        return identity_val

    @_debug()
    def begin_transaction(self):
        self._logger.debug(">>> START TRX")
        ibm_db.autocommit(self._conn, ibm_db.SQL_AUTOCOMMIT_OFF)

    @_debug()
    def commit(self):
        self._logger.debug("<<< COMMIT")
        ibm_db.commit(self._conn)
        ibm_db.autocommit(self._conn, ibm_db.SQL_AUTOCOMMIT_ON)

    @_debug()
    def rollback(self):
        self._logger.debug(">>> ROLLBACK")
        ibm_db.rollback(self._conn)
        ibm_db.autocommit(self._conn, ibm_db.SQL_AUTOCOMMIT_ON)

    @_debug()
    def get_placeholder(self):
        return "?"

    @_debug()
    def get_name(self):
        return self._driver
