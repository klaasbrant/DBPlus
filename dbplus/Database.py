from __future__ import absolute_import, division, print_function, with_statement
import logging
import os
import time
from dbplus.Record import Record
from dbplus.RecordCollection import RecordCollection
from dbplus.Statement import Statement
from dbplus.helpers import _parse_database_url
from contextlib import contextmanager
from dbplus.helpers import _debug
from importlib import import_module
#from dbplus.drivers import db2, mysql, sqlite  #add other drivers when available


class Database(object):
    """A Database connection."""
    
    def __init__(self, db_url=None, **kwargs):
        self._logger = logging.getLogger('dbplus')
        # If no db_url was provided, fallback to $DATABASE_URL in environment
        DATABASE_URL = os.environ.get('DATABASE_URL')
        self.db_url = db_url or DATABASE_URL
        dbParameters = None
        if self.db_url:
            dbParameters =_parse_database_url(db_url)
        if dbParameters == None:  # that means parsing failed!!
            raise ValueError('Database url missing or invalid')
        driver= dbParameters.pop('driver').upper()
        if driver == 'DB2':
            from dbplus.drivers import DB2
            self._driver = DB2.DB2Driver(**dbParameters) # and of to the races with DB2!
        elif driver == 'MYSQL':
            from dbplus.drivers import MySQL
            self._driver = MySQL.MySQLDriver(**dbParameters) # and of to the races with MySQL!
        elif driver == 'SQLITE':
            from dbplus.drivers import SQLite 
            self._driver = SQLite.SQLiteDriver(**dbParameters) # and of to the races with SQLite!            
        elif driver == 'ORACLE':
            from dbplus.drivers import Oracle 
            self._driver = Oracle.OracleDriver(**dbParameters) # and of to the races with Oracle!   
        elif driver == 'POSTGRES':
            from dbplus.drivers import Postgres 
            self._driver = Postgres.PostgresDriver(**dbParameters) # and of to the races with Oracle!                      
        else: # add new drivers here
            raise ValueError('DBPlus does not have a driver for: {}'.format(driver))
        self._logger.info("--> Using Database driver: {}".format(driver))
        self._transaction_active = False
        self.open()

    def open(self):
        """Opens the connection to the Database."""
        self._driver.connect()

    def close(self):
        """Closes the connection to the Database."""
        self._driver.close()

    def __del__(self):
        if hasattr(self, "_driver"):
            #self._driver.close()
            self._driver=None

    def __enter__(self):
        return self

    def __exit__(self, exc, val, traceback):
        self.close()

    def __repr__(self):
        return '<Database open={}>'.format(self.is_connected())

    def get_driver(self):
        return self._driver

    def is_connected(self):
        return self._driver.is_connected()

    def ensure_connected(self):
        if not self.is_connected():
            self.open()

    #def query(self, query, fetchall=False,*args, **kwargs):
    def query(self, query, *args, **kwargs):
        """Executes the given SQL query against the Database. Parameters
        can, optionally, be provided. Returns a RecordCollection, which can be
        iterated over to get result rows as dictionaries.
        """
        self.ensure_connected()
        cursor=Statement(self)
        cursor.execute(query, *args, **kwargs)

        # Turn the cursor into RecordCollection
        rows = (Record(row) for row in cursor)
        results = RecordCollection(rows,cursor)

        # Fetch all results if desired otherwise we fetch when needed (open cursor can be locking problem!
        #if fetchall:
        #    results.all()

        return results

    def execute(self, sql, *args, **kwargs):
        self._logger.info("--> Execute: {} with arguments [{}]".format(sql,str(args)))
        self.ensure_connected()
        modified = Statement(self).execute(sql, *args, **kwargs)
        return modified

    def callproc(self, procname, *params):
        self._logger.info("--> Calling Stored proc: {} with arguments [{}]".format(procname,str(params)))
        self.ensure_connected()
        return self._driver.callproc(procname, *params)

    def last_insert_id(self, seq_name=None):
        self.ensure_connected()
        return self._driver.last_insert_id(seq_name)

    def error_code(self):
        self.ensure_connected()
        return self._driver.error_code()

    def error_info(self):
        self.ensure_connected()
        return self._driver.error_info()

    @contextmanager
    def transaction(self):
        """Returns with block for transaction. Call ``commit`` or ``rollback`` at end as appropriate."""
        self._logger.info("--> Begin transaction block")
        self.begin_transaction()
        try:
            yield
            self.commit()
            self._logger.info("--> Transaction committed")
        except Exception as ex:
            self._logger.info("--> Transaction rollback because failure in transaction")
            self.rollback()
            raise ex

    def begin_transaction(self):
        self.ensure_connected()
        if self._transaction_active == True:
            raise RuntimeError('Nested transactions is not supported')
        self._transaction_active = True
        self._driver.begin_transaction()

    def commit(self):
        if self._transaction_active == False:
            raise RuntimeError('Commit on never started transaction')
        self.ensure_connected()
        self._driver.commit()
        self._transaction_active = False

    def rollback(self):
        if self._transaction_active == False:
            raise RuntimeError('Rollback on never started transaction')
        self.ensure_connected()
        self._transaction_active = False
        self._driver.rollback()

    def is_transaction_active(self):
        return self._transaction_active
