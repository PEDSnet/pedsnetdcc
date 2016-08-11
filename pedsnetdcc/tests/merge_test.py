import sqlalchemy
import unittest

import testing.postgresql

from pedsnetdcc import SITES, VOCAB_TABLES, TRANSFORMS
from pedsnetdcc.db import Statement
from pedsnetdcc.merge_site_data import merge_site_data, clear_dcc_data
from pedsnetdcc.utils import make_conn_str, stock_metadata

Postgresql = None


def setUpModule():

    # Generate a Postgresql class which caches the init-ed database across
    # multiple ephemeral database cluster instances.
    global Postgresql
    Postgresql = testing.postgresql.PostgresqlFactory(
        cache_initialized_db=True)


def tearDownModule(self):
    # Clear cached init-ed database at end of tests.
    Postgresql.clear_cache()


class TestMerge(unittest.TestCase):

    def setUp(self):
        # Set model version and get metadata.
        self.model_version = '2.3.0'
        self.metadata = stock_metadata(self.model_version)
        for t in TRANSFORMS:
            self.metadata = t.modify_metadata(self.metadata)

        # Create a postgres database in a temp directory.
        self.postgresql = Postgresql()
        self.dburi = self.postgresql.url()
        self.conn_str = make_conn_str(self.dburi)
        self.engine = sqlalchemy.create_engine(self.dburi)

    def tearDown(self):
        # Destroy the postgres database.
        self.postgresql.stop()

    def test_merge(self):
        # Create schemas in the database.
        for site in SITES + ('dcc',):
            Statement('CREATE SCHEMA {0}'.format(site + '_pedsnet')).\
                    execute(self.conn_str)

        Statement('CREATE SCHEMA vocabulary').execute(self.conn_str)

        # Create pedsnet data tables in all site schemas.
        for site in SITES:
            for table_name in (set(self.metadata.tables.keys()) -
                               set(VOCAB_TABLES)):

                table = self.metadata.tables[table_name]
                table.schema = site + '_pedsnet'
                table.create(self.engine)
                table.schema = None

        # Create pedsnet vocab tables in vocab schema.
        for table_name in VOCAB_TABLES:
            table = self.metadata.tables[table_name]
            table.schema = 'vocabulary'
            table.create(self.engine)
            table.schema = None

        merge_site_data(self.model_version, self.conn_str)

        md = sqlalchemy.MetaData(schema='dcc_pedsnet')
        md.reflect(self.engine)

        for table in ('person', 'visit_occurrence', 'location'):
            # Schema qualified table names in the tables dict.
            self.assertIn('dcc_pedsnet.' + table, md.tables)

        self.assertNotIn('dcc_pedsnet.concept', md.tables)

    def test_clear(self):
        # Create dcc schema.
        Statement('CREATE SCHEMA dcc_pedsnet').execute(self.conn_str)

        # Create non-vocab tables in schema.
        for table_name in (set(self.metadata.tables.keys()) -
                           set(VOCAB_TABLES)):

            table = self.metadata.tables[table_name]
            table.schema = 'dcc_pedsnet'
            table.create(self.engine)
            table.schema = None

        # Sanity check.
        md = sqlalchemy.MetaData(schema='dcc_pedsnet')
        md.reflect(self.engine)
        self.assertIn('dcc_pedsnet.person', md.tables)

        clear_dcc_data(self.model_version, self.conn_str)

        # Actual test of functionality.
        md = sqlalchemy.MetaData(schema='dcc_pedsnet')
        md.reflect(self.engine)
        self.assertEqual(len(md.tables), 0)
