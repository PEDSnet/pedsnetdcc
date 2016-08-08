import unittest

import psycopg2
import sqlalchemy
import testing.postgresql

from pedsnetdcc.add_primary_keys import (_primary_keys_from_model_version,
                                          add_primary_keys)

from pedsnetdcc.utils import make_conn_str, stock_metadata
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

    def _make_update_tables(self):
        pks = [c for c in _primary_keys_from_model_version(self.model_version)
               if c]
        for pk in pks:
            tpl = 'create table {pfx}{tbl} as select * from {tbl}'
            sql = tpl.format(tbl=pk.table.name, pfx=UPDATE_TABLE_PREFIX)
            stmt = Statement(sql).execute(self.conn_str)
            self.assertIsNone(stmt.err)

    def test_move_primary_keys(self):
        target_schema = 'target'

        self._make_update_tables()
        move_primary_keys(self.conn_str, self.model_version)
        with self.assertRaises(psycopg2.ProgrammingError):
            move_primary_keys(self.conn_str, self.model_version)
        move_primary_keys(self.conn_str, self.model_version, force=True)
