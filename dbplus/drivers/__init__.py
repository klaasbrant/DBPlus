from __future__ import absolute_import, division, print_function, with_statement

from abc import ABCMeta, abstractmethod


class BaseDriver:
    __metaclass__ = ABCMeta

    _server_version_info = None

    _logger = None
    _platform = None
    _conn = None

    @abstractmethod
    def __init__(self, **params):
        pass

    def __del__(self):
        pass

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def close(self):
        pass

    def is_connected(self):
        return self._conn is not None

    @abstractmethod
    def error_code(self):
        pass

    @abstractmethod
    def error_info(self):
        pass

    @abstractmethod
    def callproc(self, procname, *params):
        pass

    @abstractmethod
    def execute(self, sql, *params):
        pass

    @abstractmethod
    def iterate(self):
        pass

    @abstractmethod
    def clear(self):
        pass        

    @abstractmethod
    def last_insert_id(self, seq_name=None):
        pass

    @abstractmethod
    def begin_transaction(self):
        pass

    @abstractmethod
    def commit(self):
        pass

    @abstractmethod
    def rollback(self):
        pass

    @staticmethod
    def get_placeholder():
        return "?"
