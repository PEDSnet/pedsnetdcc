import logging
import unittest

import psycopg2
import sqlalchemy
import testing.postgresql

from pedsnetdcc.indexes import _indexes_sql, add_indexes, drop_indexes
from pedsnetdcc.utils import make_conn_str, stock_metadata
from pedsnetdcc import TRANSFORMS
from pedsnetdcc.db import Statement

logging.basicConfig(level=logging.DEBUG, filename="logfile")

Postgresql = None


def setUpModule():
    # Generate a Postgresql class which caches the init-ed database across
    # multiple ephemeral database cluster instances.
    global Postgresql
    Postgresql = testing.postgresql.PostgresqlFactory(
        cache_intialized_db=True)


def tearDownModule():
    # Clear cached init-ed database at end of tests.
    Postgresql.clear_cache()


class IndexesTest(unittest.TestCase):

    def setUp(self):
        self.model_version = '2.2.0'

    def test_add_indexes(self):
        sql = _indexes_sql(self.model_version)

        sample_expected = (
            'CREATE INDEX obs_otcn_89a4742c38ecb8ba35_ix ON observation (observation_type_concept_name)',  # noqa
            'CREATE INDEX dea_s_4906dc6995505fc71431f_ix ON death (site)',
            'CREATE INDEX vis_vsaim_f1537dca8da9ab914_ix ON visit_occurrence (visit_start_age_in_months)',  # noqa
        )
        for sample in sample_expected:
            self.assertIn(sample, sql)

        sample_not_expected = (
            'CREATE INDEX idx_concept_vocabulary_id ON concept (vocabulary_id)',  # noqa
        )
        for sample in sample_not_expected:
            self.assertNotIn(sample, sql)

    def test_drop_indexes(self):
        sql = _indexes_sql(self.model_version, drop=True)

        sample_expected = (
            'DROP INDEX obs_otcn_89a4742c38ecb8ba35_ix',
            'DROP INDEX dea_s_4906dc6995505fc71431f_ix',
            'DROP INDEX vis_vsaim_f1537dca8da9ab914_ix'
        )
        for sample in sample_expected:
            self.assertIn(sample, sql)

        sample_not_expected = (
            'DROP INDEX idx_concept_vocabulary_id ON concept (vocabulary_id)',
        )
        for sample in sample_not_expected:
            self.assertNotIn(sample, sql)

    def test_add_indexes_for_vocabulary(self):
        sql = _indexes_sql(self.model_version, vocabulary=True)

        sample_expected = (
            'CREATE INDEX idx_concept_class_id ON concept (concept_class_id)',
            'CREATE INDEX idx_concept_synonym_id ON concept_synonym (concept_id)'  # noqa
        )
        for sample in sample_expected:
            self.assertIn(sample, sql)

        sample_not_expected = (
            'CREATE INDEX con_lcn_f7a508db6a172c78291_ix ON concept_synonym (language_concept_name)',  # noqa
            'CREATE INDEX con_s_d9ad76e415cb919c49e49_ix ON concept_class (site)'  # noqa
        )
        for sample in sample_not_expected:
            self.assertNotIn(sample, sql)


class IndexesDatabaseTest(unittest.TestCase):

    def setUp(self):
        # Create a postgres database in a temp directory.
        self.postgresql = Postgresql()
        self.dburi = self.postgresql.url()
        self.conn_str = make_conn_str(self.dburi)
        self.engine = sqlalchemy.create_engine(self.dburi)

        # Create transformed pedsnet metadata
        self.model_version = '2.2.0'
        self.metadata = stock_metadata(self.model_version)
        for t in TRANSFORMS:
            self.metadata = t.modify_metadata(self.metadata)

    def tearDown(self):
        # Destroy the postgres database.
        self.postgresql.stop()

    def expected_measurement_index_names(self):
        # Return a set of expected measurement (non-vocab) index names.
        # This may need to be modified if the PEDSnet CDM or transformations
        # change.
        return {'idx_measurement_concept_id',
                'idx_measurement_person_id',
                'idx_measurement_visit_id',
                'mea_pcn_74e171086ab53fdef03_ix',
                'mea_maim_fafec5cb283b981155_ix',
                'mea_mcn_2396c11b8e9dc80fad6_ix',
                'mea_mraim_b3652804e85e68491_ix',
                'mea_ucn_a1d8526ef0526700f9b_ix',
                'mea_vacn_cdbccecc93bc04359c_ix',
                'mea_mtcn_0512b6f39c80e05694_ix',
                'mea_ocn_adee9ca63d3ce5cf5ca_ix',
                'mea_mscn_a15f3175cfbed7967a_ix',
                'mea_rlocn_49286b9222656be21_ix',
                'mea_s_c389be51cb02c33ef7d70_ix',
                'mea_rhocn_2ddf11b3636910434_ix',
                }

    def expected_concept_index_names(self):
        # Return a set of expected concept (vocab) index names.
        return {'idx_concept_class_id',
                'idx_concept_code',
                'idx_concept_domain_id',
                'idx_concept_vocabulary_id',
                }

    def test_drop(self):

        # Instantiate the transformed pedsnet database structure.
        self.metadata.create_all(self.engine)

        # Grab the measurement table created
        measurement = sqlalchemy.Table('measurement', sqlalchemy.MetaData(),
                                       autoload=True,
                                       autoload_with=self.engine)
        index_names = [i.name for i in measurement.indexes]

        # Check that the measurement table has all extra indexes.
        for idx in self.expected_measurement_index_names():
            self.assertIn(idx, index_names)

        # Drop indexes on the non-vocabulary tables.
        drop_indexes(self.conn_str, self.model_version)

        # Check that the measurement table has no indexes
        measurement = sqlalchemy.Table('measurement', sqlalchemy.MetaData(),
                                       autoload=True,
                                       autoload_with=self.engine)
        self.assertEqual(len(measurement.indexes), 0)

        # Check that vocab indexes were not dropped
        concept = sqlalchemy.Table('concept', sqlalchemy.MetaData(),
                                   autoload=True, autoload_with=self.engine)
        concept_index_names = [i.name for i in concept.indexes]
        self.assertNotEqual(self.expected_concept_index_names(),
                            concept_index_names)

        # Check that an exception is raised when double-dropping
        with self.assertRaises(psycopg2.ProgrammingError):
            drop_indexes(self.conn_str, self.model_version)

    def test_add(self):

        # Instantiate the transformed pedsnet database structure.
        self.metadata.create_all(self.engine)

        # Drop indexes on the non-vocabulary tables.
        drop_indexes(self.conn_str, self.model_version)

        # Drop indexes on vocabulary tables.
        drop_indexes(self.conn_str, self.model_version, vocabulary=True)

        # Verify that the measurement table has no indexes
        measurement = sqlalchemy.Table('measurement', sqlalchemy.MetaData(),
                                       autoload=True,
                                       autoload_with=self.engine)
        self.assertEqual(len(measurement.indexes), 0)

        # Verify that the concept table has no indexes
        concept = sqlalchemy.Table('concept', sqlalchemy.MetaData(),
                                   autoload=True, autoload_with=self.engine)
        self.assertEqual(len(concept.indexes), 0)

        # Create indexes on non-vocabulary tables.
        add_indexes(self.conn_str, self.model_version)

        # Check that the measurement table has the right indexes
        measurement = sqlalchemy.Table('measurement', sqlalchemy.MetaData(),
                                       autoload=True,
                                       autoload_with=self.engine)
        self.assertEqual(self.expected_measurement_index_names(),
                         set([i.name for i in measurement.indexes]))

        # Check that the concept table has no indexes
        concept = sqlalchemy.Table('concept', sqlalchemy.MetaData(),
                                   autoload=True, autoload_with=self.engine)
        self.assertEqual(len(concept.indexes), 0)

        # Check that an exception is raised if we double-add
        with self.assertRaises(psycopg2.ProgrammingError):
            add_indexes(self.conn_str, self.model_version)

    def test_add_force(self):

        # Instantiate the transformed pedsnet database structure (including
        # indexes)
        self.metadata.create_all(self.engine)

        # Create indexes on non-vocabulary tables. This should not raise
        # an exception, even though the indexes already exist.
        add_indexes(self.conn_str, self.model_version, force=True)

    def test_drop_force(self):

        # Instantiate the transformed pedsnet database structure.
        self.metadata.create_all(self.engine)

        # Remove an index
        Statement('DROP INDEX idx_measurement_concept_id').execute(
            self.conn_str)

        # Verify that this index is gone
        measurement = sqlalchemy.Table('measurement', sqlalchemy.MetaData(),
                                       autoload=True,
                                       autoload_with=self.engine)
        self.assertNotIn('idx_measurement_concept_id',
                         [i.name for i in measurement.indexes])

        # Drop indexes on the non-vocabulary tables.
        # This should not raise an exception.
        drop_indexes(self.conn_str, self.model_version, force=True)
