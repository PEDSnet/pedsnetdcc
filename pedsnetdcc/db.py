import collections
import logging
import multiprocessing
import psycopg2
import uuid
import threading

from pedsnetdcc.dict_logging import DictQueueHandler
from pedsnetdcc.utils import get_conn_info_dict, combine_dicts

logger = logging.getLogger(__name__)


class Statement(object):
    """An executable database statement object. Usable in parallel.

    Stores the executable sql as well as a msg describing the purpose of the
    sql statement. Uniquely and immutably identified by an id_ attribute to
    facilitate membership in StatementSets and parallel execution. Also stores
    the resulting data, rowcount, and field names or error from executing the
    sql statement. Able to execute the sql statement if given a connection
    string or an open dbapi connection object. Will reset state on statement
    re-execution.

    If any errors are encountered in the statement execution, they are stored
    in the err attribute and logged at debug level. If data is not returned by
    the statement, the data attribute remains None. **It is very important that
    the calling function looks for and responds to any errors or missing
    data.**

    :raises RuntimeError: if setting the id_ attribute is attempted
    """

    # Keep a class constant reference to the module-level logger.
    logger = logger

    def __init__(self, sql, msg='executing SQL', id_=None):
        """Populate defaults on a new Statement object.

        If msg is not passed, a default 'executing SQL' message is used. If id_
        is not passed, a uuid is generated for the Statement.

        :param str sql: the sql statement
        :param str msg: a message describing the purpose of the sql
        :param id_:     the unique identifer of the Statement
        """
        self.sql = sql
        self.msg = msg
        self._id_ = id_ or uuid.uuid4()
        self.data = None
        self.fields = []
        self.rowcount = None
        self.err = None

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.id_ == other.id_

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        """Return a unique int identifier; allows for StatementSet membership.

        :returns: unique immutable identifier
        :rtype:   int
        """
        return hash(self.id_)

    def __repr__(self):
        return 'Statement(sql={0}, msg={1}, id_={2})'.format(self.sql,
                                                             self.msg,
                                                             self.id_)

    def __str__(self):
        return 'Statement for {0}'.format(self.msg)

    @property
    def id_(self):
        """Get the id_ value from its 'hidden' internal storage spot.

        :returns: the id_ value
        """
        return self._id_

    @id_.setter
    def id_(self, val):
        """Raise a RuntimeError if setting id_ is attempted.

        :param val: a value to set id_ to
        :raises:    RuntimeError on every invocation
        """
        raise RuntimeError('Statement.id_ is immutable.')

    def _get_logger(self, logq):
        """Setup and return a logger for the instance.

        If not logq, this is just the module level logger via self.logger. If
        logq, this is a QueueHandler logger for parallel process logging.

        :param Queue logq: the queue to log on or None
        :returns:          the logger to use
        :rtype:            logging.Logger
        """

        if not logq:
            return self.logger

        else:
            # See the DictQueueHandler docstring for the reason why the
            # standard logging.handlers.QueueHandler can't be used here.
            local_logger = logging.getLogger(str(self.id_))
            local_logger.setLevel(logging.DEBUG)
            if not local_logger.handlers:
                qh = DictQueueHandler(logq)
                local_logger.addHandler(qh)
            return local_logger

    def execute(self, conn_str, resq=None, logq=None):
        """Execute the sql statement against a database. Usable in parallel.

        A new database connection is made using the passed connection string
        and the `execute_on_conn` method is called with that connections and
        `resq` and `logq`. If a connection error occurs, it is stored in
        self.err and logged at debug level. After execution, regardless of
        errors, the connection is closed.

        The statement is executed with an isolation level of 0 (autocommit),
        so it is safe to execute statements like `CREATE DATABASE`, `DROP
        DATABASE`, and `VACUUM`.

        :param str conn_str: connection string for the database
        :param Queue resq:   result queue to pass to `execute_on_conn`
        :param Queue logq:   logging queue to pass to `execute_on_conn`
        :returns:            the processed Statement object itself
        :rtype:              Statement
        """

        local_logger = self._get_logger(logq)
        conn_info = get_conn_info_dict(conn_str)

        conn = None

        try:
            with psycopg2.connect(conn_str) as conn:
                conn.set_isolation_level(
                    psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
                self.execute_on_conn(conn, resq, logq)

        # `execute_on_conn` handles its own errors, so this must be a
        # connection error.
        except Exception as err:
            self.err = err
            msg_dict = combine_dicts({'msg': 'connection error while {0}'.
                                      format(self.msg), 'err': str(err),
                                      'id': self.id_}, conn_info)
            local_logger.debug(msg_dict)

            # If there is a connection error, `execute_on_conn` will not run
            # and self needs to be put on the queue.
            if resq:
                resq.put(self)

        finally:
            if conn:
                conn.close()

        return self

    def execute_on_conn(self, conn, resq=None, logq=None):
        """Execute the sql statement on a connection. Usable in parallel.

        A new cursor is made using the passed database connection and the
        statement is executed in that cursor. The statement sql, msg, id_ and
        conn_info are logged at debug level. The rowcount or None is placed in
        self.rowcount. The field names are stored in the self.fields list. The
        data is fetched using cursor.fetchall() and stored in self.data. If no
        results exist, the error is caught and None is stored instead.

        If an error occurs while executing the statement, it is caught and
        stored in self.err and 'database error while `msg`', str(err), id_ and
        conn_info are logged at debug level. The error is not reraised. The
        Statement object itself is returned.

        If logq is given, a DictQueueHandler is created and the log messages
        are passed through that instead of the module level logger. If resq is
        given, the Statement object is put onto that queue after processing.

        Resets state on every call to make re-execution produce sensible state.

        :param str conn:   dbapi connection object to the database
        :param Queue resq: queue to put resulting Statement objects onto
        :param Queue logq: queue to put log records onto
        :returns:          the processed Statement object itself
        :rtype:            Statement
        """

        # Re-initialize attributes in case this is a re-execution.
        self.data = None
        self.fields = []
        self.rowcount = None
        self.err = None

        local_logger = self._get_logger(logq)
        conn_info = get_conn_info_dict(conn.dsn)

        try:
            with conn.cursor() as cursor:

                # Execute the query.
                msg_dict = combine_dicts({'msg': self.msg, 'sql': self.sql,
                                          'id_': self.id_}, conn_info)
                local_logger.debug(msg_dict)

                cursor.execute(self.sql)

                # Get the effected row count.
                if cursor.rowcount not in [-1, None]:
                    self.rowcount = cursor.rowcount

                # Get the result field names.
                if cursor.description:
                    for field in cursor.description:
                        self.fields.append(field[0])

                # Retrieve the data or, if none, leave data as None.
                try:
                    self.data = cursor.fetchall()
                except psycopg2.ProgrammingError as e:
                    if e.args[0] == 'no results to fetch':
                        pass
                    else:
                        raise

        except Exception as err:
            self.err = err
            msg_dict = combine_dicts({'msg': 'database error while {0}'.
                                      format(self.msg), 'err': str(err),
                                      'id': self.id_}, conn_info)
            local_logger.debug(msg_dict)

        if resq:
            resq.put(self)

        return self


class StatementSet(collections.MutableSet):
    """A set of statements that can be executed in parallel.

    A collections.MutableSet class that adds a `parallel_execute` method that
    calls `execute` on each member of the set. The method intelligently handles
    tasks for a given number of workers, logging messages and errors from
    workers without random interleaving, and collecting the results of the work
    back into the set.

    The class is implemented with a simple underlying set in the `data`
    attribute that can be manipulated directly if needed.

    Although the intent is for all members to be Statement objects, any object
    that meets the `obj.execute(conn_str, resq=None, logq=None) -> obj` API
    should work (no type checking is done).
    """

    def __init__(self, *data):
        self.data = set(data)

    def __contains__(self, elem):
        return elem in self.data

    def __iter__(self):
        for elem in self.data:
            yield elem

    def __len__(self):
        return len(self.data)

    def add(self, elem):
        return self.data.add(elem)

    def discard(self, elem):
        return self.data.discard(elem)

    def parallel_execute(self, conn_str, pool_size=None, taskq=None, resq=None,
                         logq=None):
        """Execute all statements in parallel using pool_size num workers.

        Initialize pool_size number of worker processes (or one worker per
        statement up to 24, by default) running the `_worker_process` module
        method with taskq, resq, and logq as task provisioning, result putting,
        and log record putting queues as arguments. Place all of the Statements
        in the set onto the task queue, start the `_logger_thread` module
        method in a thread to receive log records on logq, and wait for all the
        tasks on the task queue to finish. Stop all the workers, end the
        logging thread, and collect the results by clearing the set and then
        adding the now modified Statements on the result queue back into the
        set. Return self.

        If taskq, resq, or logq are not given, fresh multiprocessing.Queues are
        used for resq and logq and a fresh multiprocessing.JoinableQueue is
        used for taskq.

        :param str conn_str:  connection string for the database
        :param int pool_size: number of workers in the pool
        :param Queue taskq:   task provisioning queue
        :param Queue resq:    result putting queue
        :param Queue logq:    log record putting queue
        :returns:             self with modified Statements
        :rtype:               StatementSet
        """

        workers = []
        max_workers = 24

        if not pool_size:
            if len(self) <= max_workers:
                pool_size = len(self)
            else:
                pool_size = max_workers

        taskq = taskq or multiprocessing.JoinableQueue()
        resq = resq or multiprocessing.Queue()
        logq = logq or multiprocessing.Queue()

        conn_info = get_conn_info_dict(conn_str)
        msg_dict = combine_dicts({'msg': 'executing sql statement set in'
                                  ' parallel', 'len': len(self),
                                  'pool_size': pool_size}, conn_info)
        logger.info(msg_dict)

        # Start the worker processes.
        for i in range(pool_size):
            wp = multiprocessing.Process(target=_worker_process,
                                         args=(conn_str, taskq, resq, logq))
            workers.append(wp)
            wp.start()

        # Load the tasks onto the queue.
        for task in self:
            taskq.put(task)

        # Start the logging thread to receive logs from the workers.
        logp = threading.Thread(target=_logger_thread, args=(logq,))
        logp.start()

        # Wait for all the work to be done.
        taskq.join()

        # Stop all the workers (I'm a computah).
        for i in range(pool_size):
            taskq.put(None)
        for wp in workers:
            wp.join()

        # End the logging thread.
        logq.put(None)
        logp.join()

        # Collect the results.
        self.clear()
        resq.put(None)
        while True:
            result = resq.get()
            if result is None:
                break
            self.add(result)

        return self


class StatementList(collections.MutableSequence):
    """A list of statements that can be executed in serial, guaranteeing order.

    A collections.MutableSequence class that adds a `serial_execute` method
    that calls `execute` on each member of the list in order, optionally inside
    of a single transaction.

    The class is implemented with a simple underlying list in the `data`
    attribute that can be manipulated directly if needed.

    Although the intent is for all members to be Statement objects, any object
    that meets the `obj.execute(conn_str, resq=None, logq=None) -> obj` API
    should work (no type checking is done). For execution in a transaction,
    the `obj.execute_on_conn(conn) -> obj` API must also be met.
    """

    def __init__(self, *data):
        self.data = list(data)

    def __contains__(self, elem):
        return elem in self.data

    def __iter__(self):
        for elem in self.data:
            yield elem

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        return self.data[idx]

    def __setitem__(self, idx, val):
        self.data[idx] = val

    def __delitem__(self, idx):
        del self.data[idx]

    def insert(self, idx, val):
        self.data[idx:idx] = [val]

    def serial_execute(self, conn_str, transaction=False):
        """Serially execute each statement in the list in order.

        Statements are iterated over and executed. If `transaction` is True,
        a connection is created here and the lower level `execute_on_conn` is
        called on each statement instead. The statements are modified in place
        and thus the list is modified in place. The list itself is returned.

        Some statements like `CREATE TABLE`, `DROP TABLE`, and `VACUUM`
        can't be executed inside a transaction block. For such statements,
        make sure to use the default `transaction` value of False.

        :param str conn_str:     connection string for the database
        :param bool transaction: whether to use a transaction or not
        :param int isolation_level: PostgreSQL transaction isolation level
        :returns:                the StatementList itself
        :rtype:                  StatementList
        """
        conn_info = get_conn_info_dict(conn_str)
        msg_dict = combine_dicts({'msg': 'executing sql statement list'
                                  ' serially', 'len': len(self),
                                  'transaction': transaction}, conn_info)
        logger.info(msg_dict)

        if not transaction:
            for each in self:
                each.execute(conn_str)

        else:

            conn = None

            try:
                # The `with` block automatically calls `conn.commit()` if it
                # exits without errors or `conn.rollback()` if hits errors.
                with psycopg2.connect(conn_str) as conn:
                    for each in self:
                        each.execute_on_conn(conn)

            finally:
                if conn:
                    conn.close()

        return self


def _worker_process(conn_str, taskq, resq=None, logq=None):
    """Calls task.execute(conn_str, resq, logq) on tasks in taskq.

    Marks tasks as complete with taskq.task_done(). Stops when None is
    retrieved from the queue. Only intended for internal use by the
    parallel_execute function.

    :param taskq: queue to get tasks from
    :type taskq:  queue.Queue
    :param resq:  result queue to pass to tasks
    :type resq:   queue.Queue
    :param logq:  log queue to pass to tasks
    :type logq:   queue.Queue
    """
    while True:
        task = taskq.get()
        if task is None:
            break
        task.execute(conn_str, resq, logq)
        taskq.task_done()


def _logger_thread(logq):
    """Passes log records from logq on to the module-level logger.

    Because logger.handle (which handles already created log records)
    does not check the logger level, check it before calling that method.
    Stops when None is retrieved from the queue. Only intended for internal
    use by the parallel_execute function.

    :param logq: queue to get log records from
    :type logq:  queue.Queue
    """
    while True:
        record = logq.get()
        if record is None:
            break
        if logger.isEnabledFor(record.levelno):
            logger.handle(record)
