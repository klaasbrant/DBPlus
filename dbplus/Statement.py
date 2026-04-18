from __future__ import annotations

import logging
import re
from typing import Any, Callable, Dict, Generator, List, Optional, Union

from dbplus.helpers import _debug
from dbplus.QueryStore import Query


class Statement:
    _cursor = None

    _re_params = re.compile(
        r"(\?|(?<!:):[a-zA-Z_][a-zA-Z0-9_]*)(?=(?:(?:\\.|[^'\"\\])*['\"](?:\\.|[^'\"\\])*['\"])*(?:\\.|[^'\"\\])*\Z)"
    )

    def __init__(self, database: Any) -> None:
        self._connection = database
        self._logger = logging.getLogger("Statement")
        self._cursor: Any = None
        self._next: Any = None

    def __iter__(self) -> Generator[Dict[str, Any], None, None]:
        return self.iterate()

    def __repr__(self) -> str:
        return f"<Statement {self._connection=} {self._cursor=} {self._next=} >"

    def __enter__(self) -> Statement:
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> bool:
        self.close()
        return False

    def close(self) -> None:
        if self._cursor is not None:
            try:
                self._cursor.close()
            except Exception:
                pass
            self._cursor = None

    def iterate(self) -> Generator[Dict[str, Any], None, None]:
        for row in self._connection.get_driver().iterate(self):
            yield row

    def execute(self, sql: Union[str, Query], *args: Any, **kwargs: Any) -> int:
        if isinstance(sql, Query):
            sql = sql.sql
        self._logger.debug(f"--> Execute : {sql} :: {args=}, {kwargs=}")

        if self._connection.get_driver().get_placeholder() == ":":
            return self._connection.get_driver().execute(self, sql, **kwargs)

        for i, arg in enumerate(args):
            if isinstance(arg, dict):
                kwargs.update(arg)
            else:
                kwargs[i] = arg
        self._logger.info(f"--> Merged args :: {kwargs} ")
        params: List[Any] = []
        sql = Statement._re_params.sub(self._prepare(kwargs, params), sql)
        self._logger.info(f"--> Formatted Query: {sql} {params}")
        return self._connection.get_driver().execute(self, sql, *params)

    def next_result(self) -> Any:
        self._logger.info(f"--> next result {self}")
        stmt = self._connection.get_driver().next_result(self)
        self._logger.info(f"--> next result returns {self}")
        return stmt

    def _prepare(self, params: Dict[Any, Any], exec_params: List[Any]) -> Callable[[re.Match[str]], str]:
        def replace(match: re.Match[str]) -> str:
            key: Any = match.group()
            if key == "?":
                key = replace._param_counter
                replace._param_counter += 1
            else:
                key = key.lstrip(":")

            if key not in params:
                if isinstance(key, int):
                    raise LookupError(
                        f"SQL Positional parameter with index #{key} not found in arguments: {params}"
                    )
                else:
                    raise LookupError(
                        f"SQL Named parameter :{key} not found in arguments: {params}"
                    )

            param = params[key]

            if isinstance(param, (list, tuple)):
                exec_params.extend(param)
                return ", ".join((replace._placeholder,) * len(param))

            exec_params.append(param)
            return replace._placeholder

        replace._placeholder = self._connection.get_driver().get_placeholder()
        replace._param_counter = 0
        return replace

    def description(self) -> Any:
        return self._connection.get_driver().describe_cursor(self._cursor)
