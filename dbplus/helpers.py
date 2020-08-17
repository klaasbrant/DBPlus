from inspect import isclass
import decimal
import sys
import logging
import time

if (sys.version_info[0] > 2):
    unicode = str
else:
    unicode=unicode


def isexception(obj):
    """Given an object, return a boolean indicating whether it is an instance
    or subclass of :py:class:`Exception`.
    """
    if isinstance(obj, Exception):
        return True
    if isclass(obj) and issubclass(obj, Exception):
        return True
    return False

def _reduce_datetimes(row):
    """Receives a row, converts datetimes to strings."""

    row = list(row)

    for i in range(len(row)):
        if hasattr(row[i], 'isoformat'):
            row[i] = row[i].isoformat()
    return tuple(row)

def json_handler(obj):
    if isinstance(obj, decimal.Decimal):
        return str(obj)
    elif hasattr(obj, 'isoformat'):
        return obj.isoformat()
    else:
        return obj
    # return obj.isoformat() if hasattr(obj, 'isoformat') else obj

# Parsing code is simplified version from SQLAlchemy

def _parse_database_url(name):
    import re
    pattern = re.compile(r'''
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
            ''', re.X)

    m = pattern.match(name)
    if m is not None:
        components = m.groupdict()
        if components['database'] is not None:
            tokens = components['database'].split('?', 2)
            components['database'] = tokens[0]
            #todo parse parameters from ?x=;y=
        ipv4host = components.pop('ipv4host')
        ipv6host = components.pop('ipv6host')
        components['host'] = ipv4host or ipv6host
        return components
    else:
        return None

# @debug only works in python3 using __qualname__
def debug(loggername):
    logger = logging.getLogger(loggername)
    def log_():
        def wrapper(f):
            def wrapped(*args, **kargs):
                if (sys.version_info > (3, 0)):
                    func=f.__qualname__
                else:
                    func=""
                logger.debug('>>> enter {0} args: {1} - kwargs: {2}'.format(func,str(args[1:]),str(kargs))) #omit self in the args...
                ts = time.time()
                r = f(*args, **kargs)
                te = time.time()
                logger.debug('<<< leave {} - time: {:0.3f} sec'.format(func,te-ts))
                return r
            return wrapped
        return wrapper
    return log_

_debug = debug('dbplus')

