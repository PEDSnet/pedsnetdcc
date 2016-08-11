import unittest
import urllib

import sqlalchemy
import testing.postgresql

from pedsnetdcc import TRANSFORMS
from pedsnetdcc.db import Statement
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

    def _expected_column_names(self, metadata, table_name):
        """Return set of columns in table from metadata"""
        return set(metadata.tables[table_name].columns.keys())

    def _actual_column_names(self, schema, table_name):
        """Return set of columns in table in schema in database"""
        tpl = "select column_name from information_schema.columns " \
              "where table_schema = '{sch}' and table_name = '{tbl}'"
        sql = tpl.format(sch=schema, tbl=table_name)
        stmt = Statement(sql).execute(self.conn_str)
        self.assertIsNone(stmt.err)
        cols = set()
        for row in stmt.data:
            cols.add(row[0])
        return cols

    def test_without_data(self):
        # Instantiate the stock metadata
        orig_metadata = stock_metadata(self.model_version)
        orig_metadata.create_all(self.engine)

        run_transformation(self.conn_str, self.model_version, 'testsite',
                           'public')

        # Verify that the original tables exist in the `public_backup` schema.
        self.assertEqual(self._expected_column_names(orig_metadata, 'measurement'),
                         self._actual_column_names('public_backup', 'measurement'))

        # Verify that the transformed tables exist in the `public` schema.
        trans_metadata = stock_metadata(self.model_version)
        for t in TRANSFORMS:
            trans_metadata = t.modify_metadata(trans_metadata)
        self.assertEqual(self._expected_column_names(trans_metadata, 'measurement'),
                         self._actual_column_names('public', 'measurement'))

        # Verify that the `public_transformed` schema does not exist.
        self.assertFalse(schema_exists(self.conn_str, 'public_transformed'))

    # TODO: a test with test data would be nice
