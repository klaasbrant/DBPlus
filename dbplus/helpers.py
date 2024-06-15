import ast
import datetime
import decimal
import json
import logging
import sys
import time
from inspect import isclass


def isexception(obj):
    """Given an object, return a boolean indicating whether it is an instance
    or subclass of :py:class:`Exception`.
    """
    if isinstance(obj, Exception):
        return True
    if isclass(obj) and issubclass(obj, Exception):
        return True
    return False


def guess_type(x):
    # This function guesses the input and returns that type
    attempt_fns = [
        ast.literal_eval,
        lambda x: datetime.datetime.strptime(x, "%Y-%m-%d"),
        lambda x: datetime.datetime.strptime(x, "%Y-%m-%d %H:%M:%S"),
        int,
        float,
    ]
    for fn in attempt_fns:
        try:
            return fn(x)
        except (ValueError, SyntaxError):
            pass
    return x  # not a string, number or date? Just return input


def fix_sql_type(x, null):
    x = null if x == null else x
    return x


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        return False


def _reduce_datetimes(row):
    """Receives a row, converts datetimes to strings."""

    row = list(row)

    for i in range(len(row)):
        if hasattr(row[i], "isoformat"):
            row[i] = row[i].isoformat()
    return tuple(row)


class json_handler(json.JSONEncoder):
    # def json_handler(obj):
    def default(self, obj):
        print(type(obj))
        if isinstance(obj, decimal.Decimal):
            return obj.number()
        elif hasattr(obj, "isoformat"):
            return obj.isoformat()
        else:
            return super().default(obj)

    # return obj.isoformat() if hasattr(obj, 'isoformat') else obj


# Parsing code is simplified version from SQLAlchemy


def _parse_database_url(name):
    import re

    pattern = re.compile(
        r"""
            (?P<driver>[\w\+]+)://
            (?:
                (?P<uid>[^:/]*)
                (?::(?P<pwd>.*))?
            @)?
            (?:
                (?:
                    \[(?P<ipv6host>[^/]+)\] |
                    (?P<ipv4host>[^/:]+)
                )?
                (?::(?P<port>[^/]*))?
            )?
            (?:/(?P<database>.*))?
            """,
        re.X,
    )

    m = pattern.match(name)
    if m is not None:
        components = m.groupdict()
        if components["database"] is not None:
            tokens = components["database"].split("?", 2)
            components["database"] = tokens[0]
            # todo parse parameters from ?x=;y=
        ipv4host = components.pop("ipv4host")
        ipv6host = components.pop("ipv6host")
        components["host"] = ipv4host or ipv6host
        return components
    else:
        return None


# @debug only works in python3 using __qualname__
def debug(loggername):
    logger = logging.getLogger(loggername)

    def log_():
        def wrapper(f):
            def wrapped(*args, **kargs):
                func = f.__qualname__
                logger.debug(
                    ">>> enter {0} args: {1} - kwargs: {2}".format(
                        func, str(args[1:]), str(kargs)
                    )
                )  # omit self in the args...
                tic = time.perf_counter()
                r = f(*args, **kargs)
                toc = time.perf_counter()
                logger.debug("<<< leave {} - time: {:0.4f} sec".format(func, toc - tic))
                return r

            return wrapped

        return wrapper

    return log_


_debug = debug("dbplus")
