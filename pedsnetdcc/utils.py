import logging
import re
try:
    from urllib.parse import urlparse, parse_qs
except ImportError:
    from urlparse import urlparse, parse_qs
import shlex

import dmsa
import sqlalchemy

from pedsnetdcc import DATA_MODELS_SERVICE, VOCAB_TABLES
from pedsnetdcc.dict_logging import secs_since

logger = logging.getLogger(__name__)


def _parse_options(options):
    """Parse an options value from a connection string.

    :param str options: options string
    :return: dictionary of options keys/values
     :rtype: dict
    """
    o_dict = {}
    options_val = options.strip("'")
    options = [o.strip() for o in options_val.split('-c ')]
    for option in options:
        if not option:
            continue
        k, v = option.split('=', 1)
        o_dict[k] = v
    return o_dict


def _make_options(o_dict):
    """Join options dict back into a valid options string.

    :param str options_dict:
    :return: options string for use in connection string
    :rtype: str
    """
    return ' '.join(
        ["-c {}={}".format(k, o_dict[k]) for k in sorted(o_dict)])


def _conn_str_sort_key(key):
    """Provide pseudo-consistent ordering of components of a connection string.

    This is of no value except to aid in debugging.

    :param key: key
    :return: numeric `key` value usable by e.g. `sorted`
    """
    if key == 'host':
        return ' 1'
    if key == 'port':
        return ' 2'
    if key == 'dbname':
        return ' 3'
    if key == 'user':
        return ' 4'
    if key == 'password':
        return ' 5'
    return key.lower()


def _make_conn_str(parts):
    """Stitch a conn str together from a dictionary"""
    pairs = []
    for parts_key in sorted(parts.keys(), key=_conn_str_sort_key):
        parts_value = parts[parts_key]
        if ' ' in parts_value:
            parts_value = "'{0}'".format(parts_value)
        pairs.append('{0}={1}'.format(parts_key, parts_value))
    return ' '.join(pairs)


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

    TODO: this and `conn_str_with_search_path` can be refactored further.

    :param str uri:     The base uri to build the conn string from.
    :param search_path: An optional search path to include.
    :type search_path:  str or None
    :param password:    An optional password to include.
    :type password:     str or None
    :returns:           The constructed conn string.
    :rtype:             str
    :raises ValueError: if more than one of any value is given in `uri`.
    """  # noqa

    components = urlparse(uri)
    params = parse_qs(components.query)
    parts = dict()

    if components.hostname:
        parts['host'] = components.hostname
    if components.port:
        parts['port'] = str(components.port)
    if components.path:
        parts['dbname'] = components.path.lstrip('/')
    if components.username:
        parts['user'] = components.username
    if password:
        parts['password'] = password
    elif components.password:
        parts['password'] = components.password

    if 'options' in params:
        if len(params['options']) > 1:
            raise ValueError("more than one `options` query parameter in uri")
        o_dict = _parse_options(params['options'][0])
    else:
        o_dict = {}

    if search_path:
        o_dict['search_path'] = search_path
    options_value = _make_options(o_dict)
    if options_value:
        parts['options'] = options_value

    for k in params:
        if k == 'options':
            continue   # We already dealt with options
        values = params[k]
        if len(values) > 1:
            raise ValueError("more than one `k` query parameter in uri")
        parts[k] = values[0]

    return _make_conn_str(parts)


def conn_str_with_search_path(conn_str, search_path):
    """Patch a libpq connection string with the specified search_path.

    `search_path` is incorporated into the connection string
    via the `options` parameter, overriding any existing search_path in the
    connection string.

    See https://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-CONNSTRING

    :param str conn_str: The initial connection string.
    :param str search_path: Search path to include.
    :returns:           The patched connection string.
    :rtype:             str
    :raises ValueError: if more than one 'options' query parameter is given in `uri`.
    """  # noqa
    pairs = shlex.split(conn_str)
    pair_dict = dict()
    for pair in pairs:
        key, value = pair.split('=', 1)
        pair_dict[key] = value
    if 'options' not in pair_dict:
        pair_dict['options'] = '-c search_path={0}'.format(search_path)
    else:
        o_dict = _parse_options(pair_dict['options'])
        o_dict['search_path'] = search_path
        pair_dict['options'] = _make_options(o_dict)
    return _make_conn_str(pair_dict)


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


# TODO: I'm not sure this belongs in utils since it executes SQL.
def set_logged(conn_str, model_version, vocabulary=False, tables=None):
    """Set PEDSnet tables to logged.

    `Logged` is the default state of PostgreSQL tables. Presumably for
    performance reasons, tables are sometimes created as `unlogged` prior
    to batch load. If the `tables` list of table names is given, those tables
    are operated on. Otherwise, all non-vocabulary tables (or only the
    vocabulary tables, depending on the `vocabulary` bool) in the model version
    are operated on.

    :param str conn_str:      pq connection string
    :param str model_version: pedsnet model version
    :param bool vocabulary:   whether to operate on vocabulary tables
    :param list(str) tables:  list of table names to operate on (overrides)
    :return:
    :raises DatabaseError:    if any of the SQL statements cause an error
    """

    from pedsnetdcc.db import Statement, StatementSet

    table_names = tables or []

    if not table_names:
        # TODO: Use transformed version of this?
        metadata = stock_metadata(model_version)
        if vocabulary:
            table_names = list(VOCAB_TABLES)
        else:
            table_names = list(set(metadata.tables.keys()) - set(VOCAB_TABLES))

    stmts = StatementSet()

    sql_tpl = 'alter table {} set logged'
    msg_tpl = 'setting table {} to logged'

    for table in table_names:
        stmts.add(Statement(sql_tpl.format(table), msg_tpl.format(table)))

    stmts.parallel_execute(conn_str)

    # TODO: Implement more consistent error handling. (With force?)
    for stmt in stmts:
        if stmt.err:
            raise DatabaseError(
                'setting tables to logged: {}: {}'.format(stmt.sql, stmt.err))


# TODO: I'm not sure this belongs in utils since it executes SQL.
def vacuum(conn_str, model_version, analyze=False, vocabulary=False,
           tables=None):
    """VACUUM (and optionally ANAYLZE) tables in a PEDSnet database

    VACUUM (ANALYZE)s tables in a PEDSnet database of a particular version. If
    the `tables` list of table names is given, those tables are operated on.
    Otherwise, all non-vocabulary tables (or only the vocabulary tables,
    depending on the `vocabulary` bool) in the model version are operated on.

    :param str conn_str:      libpq connection string
    :param str model_version: pedsnet model version
    :param bool analyze:      whether to ANALYZE or not
    :param bool vocabulary:   whether to operate on vocabulary tables
    :param list(str) tables:  list of table names to operate on (overrides)
    :return:
    :raises DatabaseError:    if any of the SQL statements cause an error
    """

    from pedsnetdcc.db import Statement, StatementSet

    table_names = tables or []

    if not table_names:
        # TODO: Use transformed version of this?
        metadata = stock_metadata(model_version)
        if vocabulary:
            table_names = list(VOCAB_TABLES)
        else:
            table_names = list(set(metadata.tables.keys()) - set(VOCAB_TABLES))

    stmts = StatementSet()

    sql_tpl = 'VACUUM {0}'
    if analyze:
        sql_tpl = 'VACUUM ANALYZE {0}'

    msg_tpl = 'vacuuming {0}'

    for table in table_names:
        stmts.add(Statement(sql_tpl.format(table), msg_tpl.format(table)))

    stmts.parallel_execute(conn_str)

    # TODO: Implement more consistent error handling.
    for stmt in stmts:
        if stmt.err:
            raise DatabaseError(
                'setting tables to logged: {}: {}'.format(stmt.sql, stmt.err))
