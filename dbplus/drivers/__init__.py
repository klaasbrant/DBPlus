from abc import ABCMeta, abstractmethod


class BaseDriver(metaclass=ABCMeta):

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
    def execute(self, statement, sql, *params, **kwargs):
        pass

    @abstractmethod
    def execute_many(self, statement, sql, params):
        pass

    @abstractmethod
    def iterate(self, statement):
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

    @abstractmethod
    def describe_cursor(self, cursor):
        pass

    @abstractmethod
    def get_name(self):
        pass

    def _next_row(self, statement):
        """Fetch the next row from the cursor as a dict. Shared by drivers
        that use standard DB-API cursors (Postgres, MySQL, Oracle)."""
        columns = [desc[0] for desc in statement._cursor.description]
        row = statement._cursor.fetchone()
        if row is None:
            return None
        row = tuple(
            el.decode("utf-8") if isinstance(el, bytearray) else el for el in row
        )
        return dict(zip(columns, row))

    def next_result(self, statement):
        raise NotImplementedError("next_result is not supported for this driver")

    def get_database(self):
        raise NotImplementedError("get_database is not supported for this driver")

    @staticmethod
    def get_placeholder():
        return "?"
