import collections
import logging
import multiprocessing
import psycopg2
import uuid
import threading

from pedsnetdcc.dict_logging import DictQueueHandler
from pedsnetdcc.utils import get_search_path

logger = logging.getLogger(__name__)


class Statement(object):
    """An executable database statement object. Usable in parallel.

    Stores the executable sql as well as a msg describing the purpose of the
    sql statement. Uniquely and immutably identified by an id_ attribute to
    facilitate membership in StatementSets and parallel execution. Also stores
    the resulting data and field names or error from executing the sql
    statement. Able to execute the sql statement if given a connection string.

    :raises: RuntimeError if setting the id_ attribute is attempted
    """

    logger = logger

    def __init__(self, sql, msg='executing SQL', id_=None):
        """Create a new Statement object.

        If msg is not passed, a default 'executing SQL' message is used. If id_
        is not passed, a uuid is generated for the Statement.

        :param str sql: the sql statement
        :param str msg: a message describing the purpose of the sql
        :param id_:     the unique identifer of the Statement
        :returns:       the new Statement object
        :rtype:         Statement
        """
        self.sql = sql
        self.msg = msg
        self._id_ = id_ or uuid.uuid4()
        self.data = None
        self.fields = []
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
        return hash(self._id)

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

    def execute(self, conn_str, resq=None, logq=None):
        """Execute the sql statement against a database. Usable in parallel.

        A new database connection is made using the passed connection string
        and the statement is executed in a new cursor. The statement sql, msg,
        id_ and the search_path are logged at debug level. The data is fetched
        using cursor.fetchall() and stored in self.data and the field names
        are stored in self.fields. If an error occurs, the error is caught and
        stored in self.err and 'database error while `msg`', str(err), id_ and
        search_path are logged at error level. The error is not reraised.
        The Statement object itself is returned.

        If logq is given, a DictQueueHandler is created and the log messages
        are passed through that instead of the module level logger. If resq is
        given, the Statement object is put onto that queue after processing.

        :param str conn_str: connection string for the database
        :param resq:         queue to put resulting Statement objects onto
        :type resq:          queue.Queue or None
        :param logq:         queue to put log records onto
        :type logq:          queue.Queue or None
        :returns:            the processed Statement object itself
        :rtype:              Statement
        """

        local_logger = self.logger
        search_path = get_search_path(conn_str)

        if logq:
            # See the DictQueueHandler docstring for the reason why the
            # standard logging.handlers.QueueHandler can't be used here.
            qh = DictQueueHandler(logq)
            local_logger = logging.getLogger()
            local_logger.setLevel(logging.DEBUG)
            local_logger.addHandler(qh)

        try:
            with psycopg2.connect(conn_str) as conn:
                with conn.cursor() as cursor:

                    # Execute the query.
                    local_logger.debug({'msg': self.msg, 'sql': self.sql,
                                        'id': self.id_,
                                        'search_path': search_path})
                    cursor.execute(self.sql)

                    # Retrieve the data.
                    self.data = cursor.fetchall()

                    # Get the result field names.
                    for field in cursor.description:
                        self.fields.append(field[0])

            conn.close()

        except Exception as err:
            self.err = err
            local_logger.error({'msg': 'database error while {0}'.
                                format(self.msg), 'err': str(err),
                                'id': self._id, 'search_path': search_path})

        if resq:
            resq.put(self)

        return self


class StatementSet(collections.MutableSet):
    """A set of statements that can be executed in serial or in parallel.

    A collections.MutableSet class that adds `serial_execute` and
    `parallel_execute` methods that call `execute` on each member of the set.
    The parallel version intelligently handles tasks for a given number of
    workers, logging messages and errors from workers without random
    interleaving, and collecting the results of the work back into the set.

    Another method, `get_by_id_`, is added for convenience to return a
    particular member based on its `id_` attribute. This is a slow and stupid
    iteration through the set, but it will save a lot of lines of code.

    The class is implemented with a simple underlying set in the `data`
    attribute that can be manipulated directly if needed.

    Although the intent is for all members to be Statement objects, any object
    that meets the `obj.execute(conn_str, resq=None, logq=None) -> obj` API
    should work (no type checking is done).

    :raises: KeyError if `get_by_id_` does not find a matching member
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

    def get_by_id_(self, id_):
        """Iterate through members and return one matching the given id_.

        This is a non-performant method intended to add convenience. If all or
        a significant portion of the members need to be visited, it is much
        better to use `for each in StatementSet`.

        :param str id_: the id_ of the member to search for
        :returns:       the matching member
        :rtype:         Statement
        :raises:        KeyError if a matching member is not found
        """
        for each in self:
            if each.id_ == id_:
                return each
        return KeyError("member with id_ '{0}' not found".format(id_))

    def serial_execute(self, conn_str):
        """Serially execute each statement in the set.

        Statements are iterated over and executed. They are modified in place
        and thus the set is modified in place. The set itself is returned.

        :param str conn_str: connection string for the database
        :returns:            the StatementSet itself
        :rtype:              StatementSet
        """
        for each in self:
            each.execute(conn_str)
        return self

    def parallel_execute(self, conn_str, pool_size=None, taskq=None, resq=None,
                         logq=None):
        """Execute all statements in parallel using pool_size num workers.

        Initialize pool_size number of worker processes (or one worker per
        statement, by default) running the `_worker_process` module method
        with taskq, resq, and logq as task provisioning, result putting, and
        log record putting queues as arguments. Place all of the Statements in
        the set onto the task queue, start the `_logger_thread` module method
        in a thread to receive log records on logq, and wait for all the tasks
        on the task queue to finish. Stop all the workers, end the logging
        thread, and collect the results by clearing the set and then adding
        the now modified Statements on the result queue back into the set.

        If taskq, resq, or logq are not given, fresh multiprocessing.Queues are
        used.
        """

        workers = []
        pool_size = pool_size or len(self)
        taskq = taskq or multiprocessing.Queue()
        resq = resq or multiprocessing.Queue()
        logq = logq or multiprocessing.Queue()

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


def _worker_process(conn_str, taskq, resq, logq):
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
