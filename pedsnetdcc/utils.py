import dmsa
import logging
import re
try:
    from urllib.parse import urlparse, parse_qs
except ImportError:
    from urlparse import urlparse, parse_qs
import sqlalchemy

from pedsnetdcc import DATA_MODELS_SERVICE
from pedsnetdcc.dict_logging import secs_since

logger = logging.getLogger(__name__)


def make_conn_str(uri, search_path=None, password=None):
    """Return a libpq-compliant connection string usable with psycopg2.

    If `search_path` is supplied, it is incorporated into the connection string
    via the `options` parameter, overriding any existing search_path in the
    `uri`.

    If `password` is supplied, it is incorporated into the connection string,
    overriding any existing password in the `uri`.

    Keywords recognized: host, port, dbname, user, password, options.

    Other query parameters are passed as is.

    See https://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-CONNSTRING

    :param str uri:     The base uri to build the conn string from.
    :param search_path: An optional search path to include.
    :type search_path:  str or None
    :param password:    An optional password to include.
    :type password:     str or None
    :returns:           The constructed conn string.
    :rtype:             str
    :raises ValueError: if more than one 'options' query parameter is given in `uri`.
    """  # noqa

    components = urlparse(uri)
    params = parse_qs(components.query)
    parts = []

    if components.hostname:
        parts.append('host=' + components.hostname)
    if components.port:
        parts.append('port=' + str(components.port))
    if components.path:
        parts.append('dbname=' + components.path.lstrip('/'))
    if components.username:
        parts.append('user=' + components.username)
    if password:
        parts.append('password=' + password)
    elif components.password:
        parts.append('password=' + components.password)

    o_dict = {}
    if 'options' in params:
        if len(params['options']) > 1:
            raise ValueError("More than one `options` query parameter in uri.")
        options_val = params['options'][0].strip("'")
        options = [o.strip() for o in options_val.split('-c ')]
        for option in options:
            if not option:
                continue
            k, v = option.split('=', 1)
            o_dict[k] = v

    if search_path:
        o_dict['search_path'] = search_path

    options_value = ' '.join(
        ["-c {}={}".format(k, o_dict[k]) for k in sorted(o_dict)])  # noqa
    if options_value:
        params['options'] = [options_value]

    for k in sorted(params):
        values = params[k]
        for value in values:
            if ' ' in value:
                value = "'{}'".format(value)
            parts.append('{0}={1}'.format(k, value))

    return ' '.join(parts)


def get_conn_info_dict(conn_str):
    """Return the connection info form a libpq-compliant conn string as a dict.

    The `user`, `host`, `port`, `dbname`, and `search_path` are extracted using
    regular expressions. If any of them are not found, the corresponding dict
    value will be None.

    See https://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-CONNSTRING

    :param str conn_str: a libpq-compliant connection string
    :returns:            the extracted connection info in dict form
    :rtype:              dict
    """  # noqa

    result = {'user': None, 'host': None, 'port': None, 'dbname': None,
              'search_path': None}

    host_match = re.search(r"host=(\S*)", conn_str)
    port_match = re.search(r"port=(\S*)", conn_str)
    dbname_match = re.search(r"dbname=(\S*)", conn_str)
    user_match = re.search(r"user=(\S*)", conn_str)
    search_path_match = re.search(r"search_path=(.*?)[' ]", conn_str)

    if host_match:
        result['host'] = host_match.group(1)
    if port_match:
        result['port'] = port_match.group(1)
    if dbname_match:
        result['dbname'] = dbname_match.group(1)
    if user_match:
        result['user'] = user_match.group(1)
    if search_path_match:
        result['search_path'] = search_path_match.group(1)

    return result


def combine_dicts(*args):
    """Return a new dict that combines all the args in successive update calls.

    :param args: any number of dict-type objects
    :returns:    a dict which is the result of combining all the args
    :rtype:      dict
    """

    result = {}

    for arg in args:
        result.update(arg)

    return result


class StatementError(RuntimeError):
    pass


class MissingDataError(StatementError):
    pass


class DatabaseError(StatementError):
    pass


def check_stmt_data(stmt, caller_name=''):
    """Log and raise an error if data is missing.

    :param stmt: the statement to check
    :type stmt:  Statement
    :param str caller_name: a name for the calling function, used in logging
    :raises:     MissingDataError if stmt.data is None or 0 length
    """
    if stmt.data is None or len(stmt.data) == 0:
        err = MissingDataError('data not returned from {0}'.format(stmt.msg))
        logger.error({'msg': 'exiting {0}'.format(caller_name), 'err': err})
        raise err


def check_stmt_err(stmt, caller_name='', start_time=None):
    """Log and raise an error if there is an error on the statement.

    If start_time is not None, an `elapsed` key is added to the dictionary
    sent to the logger.

    :param stmt: the statement to check
    :type stmt:  Statement
    :param str caller_name: a name for the calling function, used in logging
    :param float start_time: optional start time, used for logging elapsed time
    :raises:     DatabaseError if stmt.err is not None
    """
    if stmt.err is not None:
        err = DatabaseError('database error while {0} ({1}): {2}'.format(
            stmt.msg, stmt.sql, stmt.err))
        log_dict = {'msg': 'exiting {0}'.format(caller_name), 'err': err}
        if start_time:
            log_dict['elapse'] = secs_since(start_time)
        logger.error(log_dict)
        raise err


def stock_metadata(model_version):
    """Return stock PEDSnet SQLAlchemy MetaData for the given version.
    :param model_version: pedsnet model version, e.g. 2.2.0
    :type: str
    :return: metadata
    :rtype: sqlalchemy.schema.MetaData
    """
    metadata = sqlalchemy.MetaData()
    return dmsa.make_model_from_service('pedsnet', model_version,
                                        DATA_MODELS_SERVICE,
                                        metadata)
