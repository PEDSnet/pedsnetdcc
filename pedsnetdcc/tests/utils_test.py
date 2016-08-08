import os
import unittest

from pedsnetdcc.utils import (make_conn_str, get_conn_info_dict,
                              conn_str_with_search_path)


class MakeConnTest(unittest.TestCase):

    def test_url_without_query(self):
        # Test adding search_path where query parameters do not exist.
        url = "postgresql://ahost/adb"
        cstr = make_conn_str(url, 'testschema')
        expected = "host=ahost dbname=adb options='-c search_path=testschema'"
        self.assertEqual(cstr, expected)

    def test_url_with_query_without_options(self):
        # Test adding search_path where query parameters exist.
        url = "postgresql://auser:apass@ahost:5433/adb?sslmode=disable" \
              "&connect_timeout=30"
        cstr = make_conn_str(url, 'testschema')
        expected = "host=ahost port=5433 dbname=adb user=auser " \
                   "password=apass connect_timeout=30 options='-c " \
                   "search_path=testschema' sslmode=disable"
        self.assertEqual(cstr, expected)

    def test_url_with_query_with_options(self):
        # Test adding the search_path into a preexisting options value.
        url = "postgresql://auser:apass@ahost:5433/adb?sslmode=disable" \
              "&connect_timeout=30&options='-c geqo=off'"
        cstr = make_conn_str(url, 'testschema')
        expected = "host=ahost port=5433 dbname=adb user=auser " \
                   "password=apass connect_timeout=30 options='-c geqo=off " \
                   "-c search_path=testschema' sslmode=disable"
        self.assertEqual(cstr, expected)

    def test_url_with_query_with_options_with_search_path(self):
        url = "postgresql://auser:apass@ahost:5433/adb?sslmode=disable" \
              "&connect_timeout=30&options='-c search_path=to_be_overridden'"
        cstr = make_conn_str(url, 'testschema')
        expected = "host=ahost port=5433 dbname=adb user=auser " \
                   "password=apass connect_timeout=30 options='-c " \
                   "search_path=testschema' sslmode=disable"
        self.assertEqual(cstr, expected)

    def test_url_with_password(self):
        url = "postgresql://auser@ahost/adb"
        cstr = make_conn_str(url, password='apass')
        expected = "host=ahost dbname=adb user=auser " \
                   "password=apass"
        self.assertEqual(cstr, expected)

    def test_url_no_password_no_search_path(self):
        url = "postgresql://auser@ahost/adb"
        cstr = make_conn_str(url)
        expected = "host=ahost dbname=adb user=auser"
        self.assertEqual(cstr, expected)

    def test_url_with_query_without_options_password_override(self):
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


class ConnStrWithSearchPathTest(unittest.TestCase):

    def test_url_without_query(self):
        # Test adding search_path where query parameters do not exist.
        url = "postgresql://ahost/adb"
        tmp_conn_str = make_conn_str(url)
        conn_str = conn_str_with_search_path(tmp_conn_str, 'testschema')
        expected = "host=ahost dbname=adb options='-c search_path=testschema'"
        self.assertEqual(conn_str, expected)

    def test_url_without_query_override(self):
        # Test overriding search_path.
        url = "postgresql://ahost/adb"
        tmp_conn_str = make_conn_str(url, 'otherschema')
        conn_str = conn_str_with_search_path(tmp_conn_str, 'testschema')
        expected = "host=ahost dbname=adb options='-c search_path=testschema'"
        self.assertEqual(conn_str, expected)

    def test_url_with_query_without_options(self):
        # Test adding search_path where query parameters exist.
        url = "postgresql://auser:apass@ahost:5433/adb?sslmode=disable" \
              "&connect_timeout=30"
        tmp_conn_str = make_conn_str(url)
        conn_str = conn_str_with_search_path(tmp_conn_str, 'testschema')
        expected = "host=ahost port=5433 dbname=adb user=auser " \
                   "password=apass connect_timeout=30 options='-c " \
                   "search_path=testschema' sslmode=disable"
        self.assertEqual(conn_str, expected)

    def test_url_with_query_without_options_override(self):
        # Test overriding search_path where query parameters exist.
        url = "postgresql://auser:apass@ahost:5433/adb?sslmode=disable" \
              "&connect_timeout=30"
        tmp_conn_str = make_conn_str(url, 'otherschema')
        conn_str = conn_str_with_search_path(tmp_conn_str, 'testschema')
        expected = "host=ahost port=5433 dbname=adb user=auser " \
                   "password=apass connect_timeout=30 options='-c " \
                   "search_path=testschema' sslmode=disable"
        self.assertEqual(conn_str, expected)

    def test_url_with_query_with_options(self):
        # Test adding the search_path into a preexisting options value.
        url = "postgresql://auser:apass@ahost:5433/adb?sslmode=disable" \
              "&connect_timeout=30&options='-c geqo=off'"
        tmp_conn_str = make_conn_str(url)
        conn_str = conn_str_with_search_path(tmp_conn_str, 'testschema')
        expected = "host=ahost port=5433 dbname=adb user=auser " \
                   "password=apass connect_timeout=30 options='-c geqo=off " \
                   "-c search_path=testschema' sslmode=disable"
        self.assertEqual(conn_str, expected)

    def test_url_with_query_with_options_override(self):
        # Test adding the search_path into a preexisting options value.
        url = "postgresql://auser:apass@ahost:5433/adb?sslmode=disable" \
              "&connect_timeout=30&options='-c geqo=off'"
        tmp_conn_str = make_conn_str(url, 'otherschema')
        conn_str = conn_str_with_search_path(tmp_conn_str, 'testschema')
        expected = "host=ahost port=5433 dbname=adb user=auser " \
                   "password=apass connect_timeout=30 options='-c geqo=off " \
                   "-c search_path=testschema' sslmode=disable"
        self.assertEqual(conn_str, expected)


class GetConnInfoDictTest(unittest.TestCase):

    def test_simple_conn_info(self):
        cstr = "host=ahost dbname=adb options='-c search_path=testschema'"
        conn_info = get_conn_info_dict(cstr)
        expected = {'search_path': 'testschema', 'host': 'ahost',
                    'dbname': 'adb', 'port': None, 'user': None}
        self.assertEqual(conn_info, expected)

    def test_conn_info_user_port(self):
        cstr = "host=ahost port=5433 dbname=adb user=auser " \
               "password=apass connect_timeout=30 options='-c " \
               "search_path=testschema' sslmode=disable"
        conn_info = get_conn_info_dict(cstr)
        expected = {'search_path': 'testschema', 'host': 'ahost',
                    'dbname': 'adb', 'port': '5433', 'user': 'auser'}
        self.assertEqual(conn_info, expected)

    def test_conn_info_with_options(self):
        cstr = "host=ahost port=5433 dbname=adb user=auser " \
               "password=apass connect_timeout=30 options='-c geqo=off " \
               "-c search_path=testschema' sslmode=disable"
        conn_info = get_conn_info_dict(cstr)
        expected = {'search_path': 'testschema', 'host': 'ahost',
                    'dbname': 'adb', 'port': '5433', 'user': 'auser'}
        self.assertEqual(conn_info, expected)

    def test_user_at_end(self):
        # Test conn string with no search_path.
        cstr = "host=ahost dbname=adb user=auser"
        conn_info = get_conn_info_dict(cstr)
        expected = {'search_path': None, 'host': 'ahost',
                    'dbname': 'adb', 'port': None, 'user': 'auser'}
        self.assertEqual(conn_info, expected)
