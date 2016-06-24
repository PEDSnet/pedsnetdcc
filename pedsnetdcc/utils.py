import logging
from pedsnetdcc.dict_logging import NoFmtQueueHandler
import psycopg2
import urllib.parse

from multiprocessing import Process, Queue
import threading
from queue import Empty

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

    components = urllib.parse.urlparse(uri)
    params = urllib.parse.parse_qs(components.query)
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


def parallel_db_exec_fetchall(conn_str, sql_list):

    resq = Queue()
    logq = Queue()
    workers = []

    for i in range(len(sql_list)):
        wp = Process(target=db_exec_fetchall, name='worker %d' % (i + 1),
                     args=(conn_str, sql_list[i], resq, logq, i + 1))
        workers.append(wp)
        wp.start()

    logp = threading.Thread(target=logger_thread, args=(logq,))
    logp.start()

    for wp in workers:
        wp.join()

    logq.put(None)
    logp.join()

    results = []
    while True:
        try:
            results.append(resq.get_nowait())
        except Empty:
            break

    return sorted(results, key=lambda x: x['order'])


def logger_thread(q):
    """Passes log records from a queue on to the logger."""
    while True:
        record = q.get()
        if record is None:
            break
        logger.handle(record)


def db_exec_fetchall(conn_str, sql, resq=None, logq=None, order=None):

    if logq:
        qh = NoFmtQueueHandler(logq)
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        logger.addHandler(qh)

    result = {'order': order}

    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cursor:

            logger.debug({'msg': 'Executing SQL.', 'sql': sql})
            cursor.execute(sql)
            result['data'] = cursor.fetchall()

            result['field_names'] = []
            for field in cursor.description:
                result['field_names'].append(field[0])

    conn.close()

    if resq:
        resq.put(result)

    return result
