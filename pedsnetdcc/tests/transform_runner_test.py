import unittest
import urllib

import sqlalchemy
import testing.postgresql

from pedsnetdcc import TRANSFORMS
from pedsnetdcc.schema import schema_exists
from pedsnetdcc.transform_runner import run_transformation
from pedsnetdcc.utils import stock_metadata, make_conn_str


def setUpModule():
    # Generate a Postgresql class which caches the init-ed database across
    # multiple ephemeral database cluster instances.
    global Postgresql
    Postgresql = testing.postgresql.PostgresqlFactory(
        cache_intialized_db=True)


def tearDownModule():
    # Clear cached init-ed database at end of tests.
    Postgresql.clear_cache()


class TransformRunnerTest(unittest.TestCase):

    def setUp(self):
        # Create a postgres database in a temp directory.
        self.postgresql = Postgresql()
        self.dburi = self.postgresql.url()
        self.conn_str = make_conn_str(self.dburi)
        self.model_version = '2.3.0'
        self.engine = sqlalchemy.create_engine(self.dburi)

    def tearDown(self):
        # Destroy the postgres database.
        self.postgresql.stop()

    def _expected_columns(self, metadata, table_name):
        """Return set of columns in table from metadata"""
        return set(metadata.tables[table_name].columns)

    def _actual_columns(self, schema, table_name):
        """Return set of columns in table in schema in database"""
        uri_query = "options='-c search_path={}'".format(schema)
        dburi = self.dburi + '?' + urllib.quote_plus(uri_query)
        engine = sqlalchemy.create_engine(dburi)
        table = sqlalchemy.Table(table_name, sqlalchemy.MetaData(),
                                 autoload=True,
                                 autoload_with=engine)
        return set(table.columns)

    def trivial_test(self):

        # Instantiate the stock metadata
        orig_metadata = stock_metadata(self.model_version)
        orig_metadata.create_all(self.engine)

        run_transformation(self.conn_str, self.model_version, 'testsite',
                           'public')

        # Verify that the original tables exist in the `public_backup` schema.
        self.assertEqual(self._expected_columns(orig_metadata, 'measurement'),
                         self._actual_columns('public_backup', 'measurement'))

        # Verify that the transformed tables exist in the `public` schema.
        trans_metadata = stock_metadata(self.model_version)
        for t in TRANSFORMS:
            trans_metadata = t.modify_metadata(trans_metadata)
        self.assertEqual(self._expected_columns(trans_metadata, 'measurement'),
                         self._actual_columns('public', 'measurement'))

        # Verify that the `public_transformed` schema does not exist.
        self.assertFalse(schema_exists(self.conn_str, 'public_transformed'))
