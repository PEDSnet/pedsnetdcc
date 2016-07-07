import os
import psycopg2
import unittest

from pedsnetdcc.sync_observation_period import sync_observation_period
from pedsnetdcc.utils import make_conn_str


class SyncObservationPeriodTest(unittest.TestCase):

    def test_observation_period_count(self):
        dburi_var = 'PEDSNETDCC_TEST_DBURI'
        search_path_var = 'PEDSNETDCC_TEST_SEARCH_PATH'
        if (dburi_var not in os.environ and
                search_path_var not in os.environ):
            self.skipTest(
                '{} and {} required for testing '
                'sync_observation_period'.format(
                    dburi_var, search_path_var))

        conn_str = make_conn_str(uri=os.environ[dburi_var],
                                 search_path=os.environ[search_path_var])

        success = sync_observation_period(conn_str)
        self.assertTrue(success)

        conn = None
        r = [0]

        try:
            with psycopg2.connect(conn_str) as conn:
                with conn.cursor() as cursor:

                    cursor.execute('SELECT COUNT(*) FROM observation_period')

                    r = cursor.fetchone()

        finally:
            if conn:
                conn.close()

        self.assertEqual(r[0], 100)
