import logging
from multiprocessing import Process, Queue
import psycopg2
from queue import Empty
import threading

from pedsnetdcc.dict_logging import DictQueueHandler

logger = logging.getLogger(__name__)


def parallel_db_exec(conn_str, sqls, count=None):
    """Executes a dict of sqls in parallel.

    `conn_str` is the database connection string to connect with.
    `sqls` is a dict of arbitrary names to sql statements that should be
    executed.
    `count` is the number of results to fetch from the db cursor.

    The `sqls` dict has the sql statement values replaced with the results
    of that statement. The result format is a dict with two entries: `data`
    which holds the single iterable result row if count is 1 or the iterable
    of iterable results rows otherwise and `field_names` which holds an
    iterable of result row field names in the same order as the result row(s)
    in `data`.
    """

    resq = Queue()
    logq = Queue()
    workers = []

    # Start the worker processes.
    for name, sql in sqls.items():
        wp = Process(target=db_exec, name=name, args=(conn_str, sql, count,
                                                      name, resq, logq))
        workers.append(wp)
        wp.start()

    # Start the logging thread to receive logs from the workers
    logp = threading.Thread(target=logger_thread, args=(logq,))
    logp.start()

    # Wait for all the workers to finish.
    for wp in workers:
        wp.join()

    # End the logging thread.
    logq.put(None)
    logp.join()

    # Collect the results.
    while True:
        try:
            result = resq.get_nowait()
            sqls[result['name']] = result['output']
        except Empty:
            break

    return sqls


def logger_thread(q):
    """Passes log records from a queue on to the logger.

    Stops when None is retrieved from the queue.
    """

    while True:
        record = q.get()
        if record is None:
            break
        logger.handle(record)


def db_exec(conn_str, sql, count=None, name=None, resq=None, logq=None):
    """Executes a sql statement against the database.

    `conn_str` is the connection string to use.
    `sql` is the statement to execute.
    `count` is the number of records to return, all is the default.
    `name` is an arbitrary name for the statement.
    `resq` is the queue to pass results to.
    `logq` is the queue to log to.

    The return value is a dict with two entries: `name` which holds the passed
    name and `output` which is a dict also with two entries: `data` which holds
    the single result row iterable if count is 1 or the iterable of result row
    iterables otherwise and `field_names` which is an iterable of result row
    field names in the same order as the result row iterable(s).
    """

    # Configure logging to the queue if it is passed.
    if logq:
        # See dict_logging.py for the reason why the standard library's
        # QueueHandler can't be used here.
        qh = DictQueueHandler(logq)
        logger = logging.getLogger()
        logger.setLevel(logging.DEBUG)
        logger.addHandler(qh)

    # Build the result object. (The `data` entry will be added when fetched.)
    result = {'name': name, 'output': {'field_names': []}}

    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cursor:

            logger.debug({'msg': 'Executing SQL.', 'sql': sql})

            # Errors need to be caught and sent through the logger in order
            # to avoid random interleaving. Perhaps this strategy will need
            # to be expanded to other statements in this method as well.
            try:
                cursor.execute(sql)
            except Exception as err:
                logger.error({'msg': 'Database error.', 'err': str(err)})

            # Get the query output, depending on count size.
            if count == 1:
                result['output']['data'] = cursor.fetchone()
            elif not count:
                result['output']['data'] = cursor.fetchall()
            else:
                result['output']['data'] = cursor.fetchmany(count)

            # Get the result field names.
            for field in cursor.description:
                result['output']['field_names'].append(field[0])

    conn.close()

    if resq:
        resq.put(result)

    return result
