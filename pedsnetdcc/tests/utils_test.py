import os
import unittest

from pedsnetdcc.utils import make_conn_str


class ConnTest(unittest.TestCase):

    def test_url_without_query(self):
        """ Test adding search_path where query parameters do not exist"""
        url = "postgresql://ahost/adb"
        cstr = make_conn_str(url, 'testschema')
        expected = "host=ahost dbname=adb options='-c search_path=testschema'"
        self.assertEqual(cstr, expected)

    def test_url_with_query_without_options(self):
        """ Test adding search_path where query parameters exist"""
        url = "postgresql://auser:apass@ahost:5433/adb?sslmode=disable" \
              "&connect_timeout=30"
        cstr = make_conn_str(url, 'testschema')
        expected = "host=ahost port=5433 dbname=adb user=auser " \
                   "password=apass connect_timeout=30 options='-c " \
                   "search_path=testschema' sslmode=disable"
        self.assertEqual(cstr, expected)

    def test_url_with_query_with_options(self):
        """Test adding the search_path into a preexisting options value"""
        url = "postgresql://auser:apass@ahost:5433/adb?sslmode=disable" \
              "&connect_timeout=30&options='-c geqo=off'"
        cstr = make_conn_str(url, 'testschema')
        expected = "host=ahost port=5433 dbname=adb user=auser " \
                   "password=apass connect_timeout=30 options='-c geqo=off " \
                   "-c search_path=testschema' sslmode=disable"
        self.assertEqual(cstr, expected)

    def test_url_with_query_with_options_with_search_path(self):
        """Test overriding the search_path"""
        url = "postgresql://auser:apass@ahost:5433/adb?sslmode=disable" \
              "&connect_timeout=30&options='-c search_path=to_be_overridden'"
        cstr = make_conn_str(url, 'testschema')
        expected = "host=ahost port=5433 dbname=adb user=auser " \
                   "password=apass connect_timeout=30 options='-c " \
                   "search_path=testschema' sslmode=disable"
        self.assertEqual(cstr, expected)

    def test_url_with_password(self):
        """ Test password and no search_path"""
        url = "postgresql://auser@ahost/adb"
        cstr = make_conn_str(url, password='apass')
        expected = "host=ahost dbname=adb user=auser " \
                   "password=apass"
        self.assertEqual(cstr, expected)

    def test_url_no_password_no_search_path(self):
        """ Test with password and no search_path"""
        url = "postgresql://auser@ahost/adb"
        cstr = make_conn_str(url)
        expected = "host=ahost dbname=adb user=auser"
        self.assertEqual(cstr, expected)

    def test_url_with_query_without_options_password_override(self):
        """Test overriding the password"""
        url = "postgresql://auser:apass@ahost:5433/adb?sslmode=disable" \
              "&connect_timeout=30"
        cstr = make_conn_str(url, 'testschema', password='newpass')
        expected = "host=ahost port=5433 dbname=adb user=auser " \
                   "password=newpass connect_timeout=30 options='-c " \
                   "search_path=testschema' sslmode=disable"
        self.assertEqual(cstr, expected)

    def test_custom_url(self):
        env_var = 'PEDSNETDCC_UTILS_URL'
        if not os.environ.get(env_var, None):
            self.skipTest('{} not defined'.format(env_var))
        else:
            import psycopg2
            schema = 'pedsnet_dcc_utils_test_schema'
            cstr = make_conn_str(os.environ[env_var], schema)
            with psycopg2.connect(cstr) as conn:
                with conn.cursor() as cursor:
                    cursor.execute('show search_path')
                    row = cursor.fetchone()
                    self.assertTrue(schema in row[0])
            conn.close()
