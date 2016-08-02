import re
import unittest

import psycopg2
import testing.postgresql

from pedsnetdcc.utils import make_conn_str
from pedsnetdcc.prepdb import (prepare_database, _make_database_name,
                               _conn_str_with_database, _sites_and_dcc)
from pedsnetdcc.db import (Statement)

Postgresql = None

# Huh, Python 3 is good for something; it has re.fullmatch
if not hasattr(re, 'fullmatch'):
    # http://stackoverflow.com/a/30212799/390663
    def fullmatch(regex, string, flags=0):
        """Emulate python-3.4 re.fullmatch()."""
        return re.match("(?:" + regex + r")\Z", string, flags=flags)
    re.fullmatch = fullmatch


def _introspect_schemas(conn_str):
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

    stmt = Statement('select schema_name from information_schema.schemata')

    for row in stmt.execute(conn_str).data:
        schema = row[0]
        should_exclude = False
        for pat in exclude_schemas:
            if re.fullmatch(pat, schema):
                should_exclude = True
                break
        if not should_exclude:
            schemas.append(row[0])

    return schemas


def setUpModule():

    # Generate a Postgresql class which caches the init-ed database across
    # multiple ephemeral database cluster instances.
    global Postgresql
    Postgresql = testing.postgresql.PostgresqlFactory(
        cache_initialized_db=True)


def tearDownModule(self):
    # Clear cached init-ed database at end of tests.
    Postgresql.clear_cache()


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

    def setUp(self):
        """Set up for test(s).

        1) Set self.model_version
        2) Set self.dbname based on the model_version
        3) Set self.conn_str based on testing.postgresql test instance
        4) Create test roles
        :raise: ValueError
        """
        self.model_version = '2.2.0'
        self.dbname = _make_database_name(self.model_version)

        self.postgresql = Postgresql()  # See setupModule
        self.dburi = self.postgresql.url()
        self.conn_str = make_conn_str(self.dburi)

        self.create_roles()

    def tearDown(self):
        """Clean up after tests.

        Note that if an exception is raised in setUp, tearDown is not invoked.

        Drop roles.
        """
        self.postgresql.stop()

    def check(self, dcc_only=False):
        """
        1) Connect to the database that should have been created.
        2) Get a list of schemas and compare

        :param bool dcc_only: if True, check dcc-only scenario
        """
        conn_str = _conn_str_with_database(self.conn_str, self.dbname)

        # Make a list of expected schemas
        schema_list = []
        schema_suffixes = ('achilles', 'harvest', 'pcornet', 'pedsnet',
                           'id_maps')

        for site in _sites_and_dcc(dcc_only=dcc_only):
            for schema_suffix in schema_suffixes:
                schema_list.append('{}_{}'.format(site, schema_suffix))
        schema_list.append('vocabulary')
        schema_list.append('public')
        schema_list.extend(('vocabulary', 'public', 'dcc_ids'))

        expected_schemas = set(schema_list)
        actual_schemas = set(_introspect_schemas(conn_str))
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

    def test_prep_db(self):
        prepare_database(self.model_version, self.conn_str)
        self.check()

    def test_prep_db_dcc_only(self):
        prepare_database(self.model_version, self.conn_str, dcc_only=True)
        self.check(dcc_only=True)
