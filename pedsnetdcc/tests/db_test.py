import json
import logging
import multiprocessing
import psycopg2
import testing.postgresql
import threading
import unittest

from pedsnetdcc.db import (Statement, StatementSet, StatementList,
                           _worker_process, _logger_thread)
from pedsnetdcc.dict_logging import DictLogFilter, DictQueueHandler
from pedsnetdcc.utils import make_conn_str

Postgresql = None
logger = None
handler = None


def setUpModule():

    # Generate a Postgresql class which caches the init-ed database across
    # multiple ephemeral database cluster instances.
    global Postgresql
    Postgresql = testing.postgresql.PostgresqlFactory(
            cache_initialized_db=True)

    # Configure the main logger to log into a handler.messages dict.
    global logger
    global handler
    logger = logging.getLogger('pedsnetdcc')
    handler = MockLoggingHandler()
    handler.addFilter(DictLogFilter('json'))
    logger.addHandler(handler)
    logger.setLevel(logging.getLevelName('DEBUG'))


def tearDownModule(self):
    # Clear cached init-ed database at end of tests.
    Postgresql.clear_cache()


class StatementSetTest(unittest.TestCase):

    def setUp(self):
        # Create a postgres database in a temp directory.
        self.postgresql = Postgresql()
        self.dburi = self.postgresql.url()
        self.conn_str = make_conn_str(self.dburi)
        # Reset the log handler.
        handler.reset()

    def tearDown(self):
        # Destroy the postgres database.
        self.postgresql.stop()

    def test_parallel_execute(self):

        conn = None

        try:
            with psycopg2.connect(self.conn_str) as conn:
                with conn.cursor() as cursor:
                    cursor.execute('CREATE TABLE test1 (foo1 int)')
                    cursor.execute('CREATE TABLE test2 (foo2 int)')
                    cursor.execute('INSERT INTO test1 VALUES (1)')
                    cursor.execute('INSERT INTO test2 VALUES (2)')
        finally:
            if conn:
                conn.close()

        stmts = StatementSet()
        stmts.add(Statement('SELECT foo1 from test1'))
        stmts.add(Statement('SELECT foo2 from test2'))
        stmts.parallel_execute(self.conn_str)

        for stmt in stmts:
            if stmt.sql == 'SELECT foo1 from test1':
                self.assertEqual(stmt.data[0][0], 1)
            elif stmt.sql == 'SELECT foo2 from test2':
                self.assertEqual(stmt.data[0][0], 2)

        self.assertEqual(len(handler.messages['debug']), 2)
        for msg in handler.messages['debug']:
            message = json.loads(msg)
            self.assertEqual(message['msg'], 'executing SQL')


class StatementListTest(unittest.TestCase):

    def setUp(self):
        # Reset the log handler.
        handler.reset()

    def test_serial_execute_no_transcation(self):
        stmts = StatementList()
        stmts.append(MockExecutable('test_task_1'))
        stmts.append(MockExecutable('test_task_2'))
        stmts.serial_execute('connstring')

        # Skip the first info message because it is from the serial execute
        # method itself.
        msg1 = json.loads(handler.messages['info'][1])
        msg2 = json.loads(handler.messages['info'][2])

        self.assertEqual('mock execute called', msg1['msg'])
        self.assertEqual('test_task_1', msg1['name'])
        self.assertEqual('mock execute called', msg2['msg'])
        self.assertEqual('test_task_2', msg2['name'])


class StatementListTestTransaction(unittest.TestCase):

    def setUp(self):
        # Create a postgres database in a temp directory.
        self.postgresql = Postgresql()
        self.dburi = self.postgresql.url()
        self.conn_str = make_conn_str(self.dburi)
        # Reset the log handler.
        handler.reset()

    def tearDown(self):
        # Destroy the postgres database.
        self.postgresql.stop()

    def test_serial_execute_transaction(self):
        stmts = StatementList()
        stmts.append(Statement('CREATE TEMP TABLE test (foo int)'))
        stmts.append(Statement('INSERT INTO test VALUES (1)'))
        stmts.append(Statement('SELECT foo FROM test'))
        stmts.serial_execute(self.conn_str, True)

        self.assertEqual(1, stmts[2].data[0][0])


class StatementTest(unittest.TestCase):

    def setUp(self):
        # Create a postgres database in a temp directory.
        self.postgresql = Postgresql()
        self.dburi = self.postgresql.url()
        self.conn_str = make_conn_str(self.dburi)
        # Reset the log handler.
        handler.reset()

    def tearDown(self):
        # Destroy the postgres database.
        self.postgresql.stop()

    def test_execute(self):
        stmt = Statement('CREATE TABLE test (foo int)')
        stmt.execute(self.conn_str)

        conn = None
        result = None

        try:
            with psycopg2.connect(self.conn_str) as conn:
                with conn.cursor() as cursor:
                    cursor.execute('SELECT COUNT(*) FROM test')
                    result = cursor.fetchall()[0][0]
        except Exception as err:
            self.fail(err)
        finally:
            if conn:
                conn.close()

        self.assertEqual(result, 0)

    def test_execute_connection_err(self):
        stmt = Statement('CREATE TABLE test (foo int)')
        stmt.execute("host=foo dbname=bar")

        self.assertTrue(stmt.err)
        msg = json.loads(handler.messages['error'][0])
        self.assertTrue(msg['msg'].startswith('connection error'))
        self.assertTrue('err' in msg)

    # TODO: Test that execute properly closes connections on failure.

    def test_execute_on_conn_nodata(self):

        stmt = Statement('CREATE TABLE test (foo int)')
        conn = None
        result = None

        try:
            with psycopg2.connect(self.conn_str) as conn:
                stmt.execute_on_conn(conn)
                with conn.cursor() as cursor:
                    cursor.execute('SELECT COUNT(*) FROM test')
                    result = cursor.fetchall()[0][0]
        except Exception as err:
            self.fail(err)
        finally:
            if conn:
                conn.close()

        self.assertFalse(stmt.data)
        self.assertEqual(result, 0)

    def test_execute_on_conn_rowcount(self):

        conn = None
        rowcount = 0
        stmt = Statement('INSERT INTO test VALUES (1)')

        try:
            with psycopg2.connect(self.conn_str) as conn:
                with conn.cursor() as cursor:
                    cursor.execute('CREATE TABLE test (foo int)')
                stmt.execute_on_conn(conn)
                rowcount = stmt.rowcount
        finally:
            if conn:
                conn.close()

        self.assertEqual(rowcount, 1)

    def test_execute_on_conn_data(self):

        conn = None
        result = None
        fieldname = ''
        stmt = Statement('SELECT foo from test')

        try:
            with psycopg2.connect(self.conn_str) as conn:
                with conn.cursor() as cursor:
                    cursor.execute('CREATE TABLE test (foo int)')
                    cursor.execute('INSERT INTO test VALUES (1)')
                stmt.execute_on_conn(conn)
                result = stmt.data[0][0]
                fieldname = stmt.fields[0]
        finally:
            if conn:
                conn.close()

        self.assertEqual(result, 1)
        self.assertEqual(fieldname, 'foo')

    def test_execute_on_conn_error(self):

        conn = None
        stmt = Statement("INSERT INTO test VALUES ('bar')")

        try:
            with psycopg2.connect(self.conn_str) as conn:
                with conn.cursor() as cursor:
                    cursor.execute('CREATE TABLE test (foo int)')
                stmt.execute_on_conn(conn)
        finally:
            if conn:
                conn.close()

        self.assertTrue(stmt.err)
        msg = json.loads(handler.messages['error'][0])
        self.assertTrue(msg['msg'].startswith('database error'))
        self.assertTrue('err' in msg)


class StatementTestId(unittest.TestCase):

    def test_id_immutable(self):
        stmt = Statement('some sql', 'a msg')

        def failure(stmt):
            stmt.id_ = 'foo'

        self.assertRaises(RuntimeError, failure, stmt)


class WorkerProcessTest(unittest.TestCase):

    def test_single_task_single_worker(self):
        # Create the needed queues.
        taskq = multiprocessing.JoinableQueue()
        resq = multiprocessing.Queue()

        # Start the worker process.
        wp = multiprocessing.Process(target=_worker_process,
                                     args=('connstring', taskq, resq))
        wp.start()

        # Put a task on the queue
        task = MockExecutable('test_task')
        taskq.put(task)
        taskq.join()

        # Stop the _worker_process.
        taskq.put(None)
        wp.join()

        # Check that the result (from `MockExecutable.execute` showed up).
        expected = {'msg': 'mock execute called', 'name': 'test_task',
                    'connstring': 'connstring'}
        result = resq.get()
        self.assertEqual(expected, result)

    def test_single_task_multi_worker(self):
        # Create the needed queues.
        taskq = multiprocessing.JoinableQueue()
        resq = multiprocessing.Queue()

        # Start the worker processes.
        workers = []
        for i in range(4):
            wp = multiprocessing.Process(target=_worker_process,
                                         args=('connstring', taskq, resq))
            workers.append(wp)
            wp.start()

        # Put a task on the queue
        task = MockExecutable('test_task')
        taskq.put(task)
        taskq.join()

        # Stop the _worker_processes.
        for i in range(4):
            taskq.put(None)
        for wp in workers:
            wp.join()

        # Check that the result (from `MockExecutable.execute` showed up).
        expected = {'msg': 'mock execute called', 'name': 'test_task',
                    'connstring': 'connstring'}
        result = resq.get()
        self.assertEqual(expected, result)

    def test_multi_task_single_worker(self):
        # Create the needed queues.
        taskq = multiprocessing.JoinableQueue()
        resq = multiprocessing.Queue()

        # Start the worker process.
        wp = multiprocessing.Process(target=_worker_process,
                                     args=('connstring', taskq, resq))
        wp.start()

        # Put tasks on the queue
        for i in range(4):
            task = MockExecutable('test_task_' + str(i))
            taskq.put(task)
            taskq.join()

        # Stop the _worker_process.
        taskq.put(None)
        wp.join()

        # Check that the results (from `MockExecutable.execute` showed up).
        expected = []
        for i in range(4):
            expected.append({'msg': 'mock execute called',
                             'name': 'test_task_' + str(i),
                             'connstring': 'connstring'})
        for i in range(4):
            self.assertIn(resq.get(), expected)

    def test_multi_task_multi_worker(self):
        # Create the needed queues.
        taskq = multiprocessing.JoinableQueue()
        resq = multiprocessing.Queue()

        # Start the worker processes.
        workers = []
        for i in range(4):
            wp = multiprocessing.Process(target=_worker_process,
                                         args=('connstring', taskq, resq))
            workers.append(wp)
            wp.start()

        # Put tasks on the queue
        for i in range(4):
            task = MockExecutable('test_task_' + str(i))
            taskq.put(task)
            taskq.join()

        # Stop the _worker_processes.
        for i in range(4):
            taskq.put(None)
        for wp in workers:
            wp.join()

        # Check that the results (from `MockExecutable.execute` showed up).
        expected = []
        for i in range(4):
            expected.append({'msg': 'mock execute called',
                             'name': 'test_task_' + str(i),
                             'connstring': 'connstring'})
        for i in range(4):
            self.assertTrue(resq.get() in expected)


class LoggerThreadTest(unittest.TestCase):

    def setUp(self):
        # Reset the log handler.
        handler.reset()

    def test_log_passing(self):

        # Set up the thread logger.
        logq = multiprocessing.Queue()
        qh = DictQueueHandler(logq)
        thread_logger = logging.getLogger('test_logger')
        thread_logger.setLevel(logging.DEBUG)
        thread_logger.addHandler(qh)

        # Start the logging thread.
        logp = threading.Thread(target=_logger_thread, args=(logq,))
        logp.start()

        # Log to the thread logger and expect it to show up in the main logger.
        thread_logger.info({'msg': 'foo'})
        thread_logger.error({'msg': 'bar'})

        # End the logging thread.
        logq.put(None)
        logp.join()

        # Check logged messages match expected.
        msg1 = json.loads(handler.messages['info'][0])
        msg2 = json.loads(handler.messages['error'][0])
        self.assertEqual(msg1['msg'], 'foo')
        self.assertEqual(msg1['level'], 'info')
        self.assertTrue('time' in msg1)
        self.assertEqual(msg2['msg'], 'bar')
        self.assertEqual(msg2['level'], 'error')
        self.assertTrue('time' in msg2)


class MockExecutable(object):
    """Mock executable to simulate the Statement API without needing a DB."""

    def __init__(self, name):
        self.name = name

    def execute(self, connstring, resq=None, logq=None):
        message = {'msg': 'mock execute called', 'name': self.name,
                   'connstring': connstring}
        if resq:
            resq.put(message)
        else:
            logger.info(message)

    def execute_on_conn(self, conn, resq=None, logq=None):
        message = {'msg': 'mock execute_on_conn called', 'name': self.name,
                   'conn': conn}
        if resq:
            resq.put(message)
        else:
            logger.info(message)


class MockLoggingHandler(logging.Handler):
    """Mock logging handler to store and check expected log output.

    Messages are available from an instance's `messages` dict, in order,
    indexed by lowercase log level string (e.g., 'debug', 'info', etc.).
    """

    def __init__(self, *args, **kwargs):
        self.messages = {'debug': [], 'info': [], 'warning': [], 'error': [],
                         'critical': []}
        super(MockLoggingHandler, self).__init__(*args, **kwargs)

    def emit(self, record):
        """Store the log record's formatted message in the messages dict."""
        self.acquire()
        try:
            msg = self.format(record)
            self.messages[record.levelname.lower()].append(msg)
            print(msg)
        except Exception:
            self.handleError(record)
        finally:
            self.release()

    def reset(self):
        self.acquire()
        try:
            for msg_list in self.messages.values():
                del msg_list[:]
        finally:
            self.release()
