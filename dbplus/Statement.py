from __future__ import absolute_import, division, print_function, with_statement
import re
import logging
from dbplus.helpers import _debug

class Statement:
    _cursor = None
    
    _re_params = re.compile(
        r"(\?|(?<!:):[a-zA-Z_][a-zA-Z0-9_]*)(?=(?:(?:\\.|[^'\"\\])*['\"](?:\\.|[^'\"\\])*['\"])*(?:\\.|[^'\"\\])*\Z)")

    def __init__(self, database):
        self._connection = database
        #self._object_name = database._driver.get_name()+'Row'
        self._logger = logging.getLogger('dbplus')

    def __iter__(self):
        return self.iterate()

    def close(self):
        if self._cursor is not None:
            self._connection.get_driver().clear(self)

    def iterate(self):
        #Driver iterate will return a row as dict
        for row in self._connection.get_driver().iterate(self):
            yield row

    def execute(self, sql, *args, **kwargs):
        self._logger.info("--> Execute : {} / {} / {}".format(sql,args,kwargs))

        if self._connection.get_driver().get_placeholder() == ":":
            return self._connection.get_driver().execute(self, sql, **kwargs)
        
        for i, arg in enumerate(args):
            kwargs[i] = arg
        self._logger.info("--> Execute arg : {} ".format(kwargs))    
        params = []
        sql = Statement._re_params.sub(self._prepare(kwargs, params), sql)
        self._logger.info("--> Query: {} {}".format(sql,params))
        return self._connection.get_driver().execute(self, sql, *params)

    def next_result(self):
        self._connection.get_driver().next_result(self)

    def _prepare(self, params, exec_params):
        def replace(match):
            key = match.group()
            if key == "?":
                key = replace._param_counter
                replace._param_counter += 1
            else:
                key = key.lstrip(":")

            if key not in params:
                if isinstance(key, int):
                    raise LookupError("SQL Positional parameter with index #{} not found in arguments: {}".format(key, params))
                else:
                    raise LookupError("SQL Named parameter :{} not found in arguments: {}".format(key, params))

            param = params[key]

            if isinstance(param, (list, tuple)):
                exec_params.extend(param)
                return ", ".join((replace._placeholder,) * len(param))

            exec_params.append(param)
            return replace._placeholder

        replace._placeholder = self._connection.get_driver().get_placeholder()
        replace._param_counter = 0

        return replace