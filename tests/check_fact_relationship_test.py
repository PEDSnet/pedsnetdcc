import json
import logging
import os
import unittest

from pedsnetdcc.check_fact_relationship import check_fact_relationship
from pedsnetdcc.dict_logging import DictLogFilter
from pedsnetdcc.utils import make_conn_str

logger = None
handler = None


def setUpModule():

    # Configure the main logger to log into a handler.messages dict.
    global logger
    global handler
    logger = logging.getLogger('pedsnetdcc')
    handler = MockLoggingHandler()
    handler.addFilter(DictLogFilter('json'))
    logger.addHandler(handler)
    logger.setLevel(logging.getLevelName('DEBUG'))


class CheckFactRelationshipTest(unittest.TestCase):

    def setUp(self):
        # Reset the log handler.
        handler.reset()

    def test_fact_relationship_results(self):
        dburi_var = 'PEDSNETDCC_TEST_DBURI'
        search_path_var = 'PEDSNETDCC_TEST_SEARCH_PATH'
        if (dburi_var not in os.environ and
                search_path_var not in os.environ):
            self.skipTest(
                '{} and {} required for testing '
                'check_fact_relationship'.format(
                    dburi_var, search_path_var))

        conn_str = make_conn_str(uri=os.environ[dburi_var],
                                 search_path=os.environ[search_path_var])

        success = check_fact_relationship(conn_str)
        self.assertFalse(success)

        # TODO: Based on the fact that nothing prints here, I'm not sure this
        # log message checking is happening...
        print(handler.messages)
        for msg in handler.messages['info']:
            msg = json.loads(msg)
            if msg['msg'].startswith('fact relationship has bad'):
                self.assertEqual(msg['percent'], 100)


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
