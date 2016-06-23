import unittest
import psycopg2

from pedsnetdcc.utils import make_conn_str
from pedsnetdcc.sync_observation_period import sync_observation_period


class SyncObservationPeriodTest(unittest.TestCase):

    def test_observation_period_count(self):

        conn_str = make_conn_str('postgresql://localhost/tmp',
                                 search_path='other')

        sync_observation_period(conn_str)

        with psycopg2.connect(conn_str) as conn:
            with conn.cursor() as cursor:

                cursor.execute('SELECT COUNT(*) FROM observation_period')

                r = cursor.fetchone()

                self.assertEqual(r[0], 100)
