import unittest

import sqlalchemy
import testing.postgresql

from pedsnetdcc.utils import (make_conn_str, stock_metadata)
from pedsnetdcc.transform_runner import TRANSFORMS
from pedsnetdcc.not_nulls import set_not_nulls, drop_not_nulls


def setUpModule():
    # Generate a Postgresql class which caches the init-ed database across
    # multiple ephemeral database cluster instances.
    global Postgresql
    Postgresql = testing.postgresql.PostgresqlFactory(
        cache_intialized_db=True)


def tearDownModule():
    # Clear cached init-ed database at end of tests.
    Postgresql.clear_cache()


class NotNulls(unittest.TestCase):

    def setUp(self):
        # Create a postgres database in a temp directory.
        self.postgresql = Postgresql()
        self.dburi = self.postgresql.url()
        self.conn_str = make_conn_str(self.dburi)
        self.model_version = '2.2.0'
        self.engine = sqlalchemy.create_engine(self.dburi)

        # Create transformed pedsnet metadata and instantiate
        self.metadata = stock_metadata(self.model_version)
        for t in TRANSFORMS:
            self.metadata = t.modify_metadata(self.metadata)
        self.metadata.create_all(self.engine)

    def tearDown(self):
        # Destroy the postgres database.
        self.postgresql.stop()

    def test_not_nulls(self):

        care_site_cols = self.metadata.tables['care_site'].columns
        concept_cols = self.metadata.tables['concept'].columns

        drop_not_nulls(self.conn_str, self.model_version)

        # Verify effectiveness of drop for a non-vocab table.
        care_site = sqlalchemy.Table('care_site',
                                     sqlalchemy.MetaData(), autoload=True,
                                     autoload_with=self.engine)
        num_not_null = 0
        for col in care_site_cols:
            if not col.nullable and not col.primary_key:
                num_not_null += 1
                self.assertTrue(care_site.columns[col.name].nullable,
                                'non-vocab drop works')
        self.assertNotEqual(num_not_null, 0)   # Sanity check

        # Verify that a vocabulary table has not been affected by the drop.
        concept = sqlalchemy.Table('concept',
                                   sqlalchemy.MetaData(), autoload=True,
                                   autoload_with=self.engine)
        num_not_null = 0
        for col in concept_cols:
            if not col.nullable and not col.primary_key:
                num_not_null += 1
                self.assertFalse(concept.columns[col.name].nullable,
                                 'non-vocab drop does not affect vocab')
        self.assertNotEqual(num_not_null, 0)   # Sanity check

        # Now drop NOT NULLs on vocabulary.
        drop_not_nulls(self.conn_str, self.model_version, vocabulary=True)

        # Verify drop for a vocab table.
        concept = sqlalchemy.Table('concept',
                                   sqlalchemy.MetaData(), autoload=True,
                                   autoload_with=self.engine)
        for col in concept_cols:
            if not col.nullable and not col.primary_key:
                self.assertTrue(concept.columns[col.name].nullable,
                                'vocab drop works')

        set_not_nulls(self.conn_str, self.model_version)

        # Verify that setting nulls worked for a non-vocab table.
        care_site = sqlalchemy.Table('care_site',
                                     sqlalchemy.MetaData(), autoload=True,
                                     autoload_with=self.engine)
        for col in care_site_cols:
            if not col.nullable and not col.primary_key:
                self.assertFalse(care_site.columns[col.name].nullable,
                                 'non-vocab set works')

        # Verify that a vocab table is unaffected by setting not nulls.
        concept = sqlalchemy.Table('concept',
                                   sqlalchemy.MetaData(), autoload=True,
                                   autoload_with=self.engine)
        for col in concept_cols:
            if not col.nullable and not col.primary_key:
                self.assertTrue(concept.columns[col.name].nullable,
                                'non-vocab set does not affect vocab')

        set_not_nulls(self.conn_str, self.model_version, vocabulary=True)

        # Verify set for a vocab table.
        concept = sqlalchemy.Table('concept',
                                   sqlalchemy.MetaData(), autoload=True,
                                   autoload_with=self.engine)
        for col in concept_cols:
            if not col.nullable and not col.primary_key:
                self.assertFalse(concept.columns[col.name].nullable,
                                 'vocab set works')
