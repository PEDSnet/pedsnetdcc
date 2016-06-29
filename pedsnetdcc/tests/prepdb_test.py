import os
import re
import unittest

import psycopg2

from pedsnetdcc.utils import make_conn_str
from pedsnetdcc.prepdb import (prepare_database, _make_database_name,
                               _conn_str_with_database, _sites_and_dcc)

# Huh, Python 3 is good for something; it has re.fullmatch
if not hasattr(re, 'fullmatch'):
    # http://stackoverflow.com/a/30212799/390663
    def fullmatch(regex, string, flags=0):
        """Emulate python-3.4 re.fullmatch()."""
        return re.match("(?:" + regex + r")\Z", string, flags=flags)
    re.fullmatch = fullmatch


def introspect_schemas(conn_str):
    """Return list of schemas in the database

    A future version of this for more generic testing might produce
    a nested data structure with tables under each schema ....

    :param conn_str: libpq/psycopg2 connection string
    :type: str
    :return: list of schemas
    :rtype: list(str)
    """
    schemas = []
    exclude_schemas = (
        'pg_catalog',
        'pg_toast.*',
        'pg_temp.*',
        'information_schema'
    )

    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cursor:

            cursor.execute(
                'select schema_name from information_schema.schemata')
            for row in cursor.fetchall():
                schema = row[0]
                should_exclude = False
                for pat in exclude_schemas:
                    if re.fullmatch(pat, schema):
                        should_exclude = True
                        break
                if not should_exclude:
                    schemas.append(row[0])
    conn.close()

    return schemas


def exec_ddl(conn_str, sql, no_transaction=False):
    with psycopg2.connect(conn_str) as conn:
        if no_transaction:
            conn.set_isolation_level(0)
        with conn.cursor() as cursor:
            cursor.execute(sql)
    conn.close()


class TestPrepareDatabase(unittest.TestCase):

    roles = ('peds_staff', 'harvest_user', 'achilles_user', 'dqa_user',
             'pcor_et_user', 'loading_user', 'dcc_owner', 'pcornet_sas')

    def create_roles(self):
        """Create roles so grants will not fail.

        :return: None
        """
        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cursor:
                for role in TestPrepareDatabase.roles:
                    cursor.execute('CREATE ROLE {}'.format(role))
        conn.close()

    def drop_roles(self):
        """Drop roles created for tests.

        :return: None
        """
        with psycopg2.connect(self.conn_str) as conn:
            with conn.cursor() as cursor:
                for role in TestPrepareDatabase.roles:
                    cursor.execute('DROP ROLE {}'.format(role))
        conn.close()

    def database_exists(self):
        """Test database existence

        :return: whether the database for the data_model exists
        :rtype: bool
        """
        new_conn_str = _conn_str_with_database(self.conn_str, self.dbname)
        try:
            with psycopg2.connect(new_conn_str) as conn:
                pass
        except psycopg2.OperationalError as e:
            if 'does not exist' in str(e):
                return False
            else:
                raise
        conn.close()
        return True

    def setUp(self):
        """Set up for test(s).

        Ordinarily you will want to define PEDSNETDCC_PREPDB_DROP (in addition
        to the mandatory PEDSNETDCC_PREPDB_URL) in the environment beforehand.
        The DROP variable allows the code to blow away a pre-existing
        database.

        1) Set self.model_version
        2) Set self.dbname based on the model_version
        3) Set self.conn_str based on the PEDSNETDCC_PREPDB_URL env var
        4) Optionally drop self.dbname if PEDSNETDCC_PREPDB_DROP is defined
        5) Create test roles
        :raise: ValueError
        """
        print('In setUp')
        self.model_version = '2.2.0'
        self.dbname = _make_database_name(self.model_version)

        url_var = 'PEDSNETDCC_PREPDB_URL'
        if not os.environ.get(url_var, None):
            raise ValueError('Define {} to test database prep'.format(url_var))
        self.url = os.environ[url_var]

        self.conn_str = make_conn_str(os.environ[url_var])
        self.check_conn_str()

        drop_var = 'PEDSNETDCC_PREPDB_DROP'
        if drop_var in os.environ:
            self.drop = True
            exec_ddl(self.conn_str,
                     'drop database if exists {}'.format(self.dbname),
                     no_transaction=True)
        else:
            if self.database_exists():
                raise ValueError(
                    'Database {} exists, and {} is not defined; refusing to '
                    'continue with tests'.format(
                        self.dbname, drop_var))
            self.drop = False

        self.create_roles()

    def tearDown(self):
        """Clean up after tests.

        Note that if an exception is raised in setUp, tearDown is not invoked.

        Drop roles.
        """
        if self.database_exists():
            # It is always OK to drop the database here, because tearDown
            # will only be invoked if 1) PEDSNETDCC_PREPDB_DROP is defined
            # or 2) the database didn't exist to begin with.
            self.drop_database()
        self.drop_roles()

    def check(self, db_type, dcc_only=False):
        """
        1) Connect to the database that should have been created.
        2) Get a list of schemas and compare

        :param db_type: 'internal' or 'prod'
        :type: str
        """
        conn_str = _conn_str_with_database(self.conn_str, self.dbname)

        # Make a list of expected schemas
        schema_list = []
        if db_type == 'internal':
            schema_suffixes = ('achilles', 'harvest', 'pcornet', 'pedsnet')
        elif db_type == 'prod':
            schema_suffixes = ('harvest', 'pcornet', 'pedsnet')
        else:
            raise ValueError('Invalid db_type: {}'.format(db_type))

        for site in _sites_and_dcc(dcc_only=dcc_only):
            for schema_suffix in schema_suffixes:
                schema_list.append('{}_{}'.format(site, schema_suffix))
        schema_list.append('vocabulary')
        schema_list.append('public')

        expected_schemas = set(schema_list)
        actual_schemas = set(introspect_schemas(conn_str))
        self.assertEqual(actual_schemas, expected_schemas)

    def check_conn_str(self):
        """Validate the connection string.

        1) Make sure dbname is not in the test DB URL.
        2) Make sure a connection can be established.

        :return: None
        :raise: psycopg2.OperationalError, ValueError
        """
        match = re.search(r'\bdbname=([^ ]+)', self.conn_str)
        if match and match.group(1) == self.dbname:
            raise ValueError(
                'URL should not contain the database to be created')

        # An exception is raised if a connection cannot be established
        with psycopg2.connect(self.conn_str) as conn:
            pass
        conn.close()

    def drop_database(self):
        """Drop the test database"""
        exec_ddl(self.conn_str, 'drop database {}'.format(self.dbname),
                 no_transaction=True)

    def test_prep_db_internal(self):
        """Test with 'internal' db_type"""
        prepare_database(self.model_version, self.conn_str, 'internal')
        self.check('internal')
        self.drop_database()

    def test_prep_db_prod(self):
        """Test with 'prod' db_type"""
        prepare_database(self.model_version, self.conn_str, 'prod')
        self.check('prod')
        self.drop_database()

    def test_prep_db_prod_dcc_only(self):
        """Test with 'prod' db_type and dcc_only=True"""
        prepare_database(self.model_version, self.conn_str, 'prod',
                         dcc_only=True)
        self.check('prod', dcc_only=True)
        self.drop_database()
