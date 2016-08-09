import unittest
import urllib

import psycopg2
import sqlalchemy
import testing.postgresql

from pedsnetdcc import VOCAB_TABLES
from pedsnetdcc.primary_keys import (_primary_keys_from_model_version,
                                     add_primary_keys)

from pedsnetdcc.utils import (make_conn_str, stock_metadata,
                              conn_str_with_search_path)
from pedsnetdcc.db import Statement


def setUpModule():
    # Generate a Postgresql class which caches the init-ed database across
    # multiple ephemeral database cluster instances.
    global Postgresql
    Postgresql = testing.postgresql.PostgresqlFactory(
        cache_intialized_db=True)


def tearDownModule():
    # Clear cached init-ed database at end of tests.
    Postgresql.clear_cache()


class AddPrimaryKeysTest(unittest.TestCase):

    def setUp(self):
        # Create a postgres database in a temp directory.
        self.postgresql = Postgresql()
        self.dburi = self.postgresql.url()
        self.conn_str = make_conn_str(self.dburi)
        self.model_version = '2.3.0'
        self.engine = sqlalchemy.create_engine(self.dburi)
        self.metadata = stock_metadata(self.model_version)

        self.metadata.create_all(self.engine)  # Populate database.

    def tearDown(self):
        # Destroy the postgres database.
        self.postgresql.stop()

    def test_pk_names(self):
        # Get primary keys.
        pks = _primary_keys_from_model_version(self.model_version)
        pk_names = [pk.name for pk in pks]
        some_expected_pk_names = {
            'xpk_death',
            'xpk_condition_occurrence',
            'xpk_observation_period',
            'xpk_drug_exposure',
            'xpk_measurement',
            'xpk_visit_payer',
            'xpk_observation',
            'xpk_person',
            'xpk_procedure_occurrence',
            'xpk_location',
            'xpk_visit_occurrence',
            'xpk_meas_organism',
            'xpk_care_site',
            'xpk_provider'}
        for pk in some_expected_pk_names:
            self.assertIn(pk, pk_names)

    def _make_update_tables(self, conn_str):
        pks = [c for c in _primary_keys_from_model_version(self.model_version)
               if c]
        for pk in pks:
            tpl = 'create table {tbl} as select * from {tbl}'
            sql = tpl.format(tbl=pk.table.name)
            stmt = Statement(sql).execute(conn_str)
            self.assertIsNone(stmt.err)

    def _check_primary_keys(self, dburi):
        # Sadly, creating an engine in sqlalchemy requires a URL, not a
        # connection string.
        new_engine = sqlalchemy.create_engine(dburi)
        for t in self.metadata.sorted_tables:
            if t.name not in VOCAB_TABLES and t.primary_key:
                tbl = sqlalchemy.Table(t.name, sqlalchemy.MetaData(),
                                       autoload=True,
                                       autoload_with=new_engine)
                self.assertTrue(tbl.primary_key and tbl.primary_key.name ==
                                t.primary_key.name)

    def test_add_primary_keys(self):
        target_schema = 'target'
        stmt = Statement('create schema ' + target_schema).execute(
            self.conn_str)
        self.assertIsNone(stmt.err)

        search_path = target_schema + ',' + 'public'
        new_conn_str = conn_str_with_search_path(self.conn_str, search_path)

        self._make_update_tables(new_conn_str)

        add_primary_keys(new_conn_str, self.model_version)

        new_dburi = self.dburi + '?' + urllib.quote_plus("options='-c search_path={0}'".format(
            search_path))
        self._check_primary_keys(new_dburi)

    def test_double_add_primary_keys(self):
        self.test_add_primary_keys()

        target_schema = 'target'
        search_path = target_schema + ',' + 'public'
        new_conn_str = conn_str_with_search_path(self.conn_str, search_path)

        with self.assertRaises(psycopg2.ProgrammingError):
            add_primary_keys(new_conn_str, self.model_version)
