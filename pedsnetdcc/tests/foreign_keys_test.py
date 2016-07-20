import sqlalchemy
import unittest
try:
    import unittest.mock
except ImportError:
    import mock

import psycopg2
import testing.postgresql

from pedsnetdcc.foreign_keys import (_foreign_keys_from_model_version,
                                     add_foreign_keys, drop_foreign_keys)
from pedsnetdcc.utils import make_conn_str, stock_metadata
from pedsnetdcc import TRANSFORMS
from pedsnetdcc.db import Statement

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


class ForeignKeysTest(unittest.TestCase):

    def setUp(self):
        # Create a postgres database in a temp directory.
        self.postgresql = Postgresql()
        self.dburi = self.postgresql.url()
        self.conn_str = make_conn_str(self.dburi)
        self.model_version = '2.2.0'
        self.engine = sqlalchemy.create_engine(self.dburi)

        # Create transformed pedsnet metadata
        self.metadata = stock_metadata(self.model_version)
        for t in TRANSFORMS:
            self.metadata = t.modify_metadata(self.metadata)

    def tearDown(self):
        # Destroy the postgres database.
        self.postgresql.stop()

    def expected_measurement_fk_names(self):
        # Return a set of expected measurement (non-vocab) foreign key names.
        # This may need to be modified if the PEDSnet CDM or transformations
        # change.
        return {'fpk_measurement_priority',
                 'fpk_measurement_range_high_op',
                 'fpk_measurement_concept',
                 'fpk_measurement_range_low_op', 
                 'fpk_measurement_type_concept', 
                 'fpk_measurement_unit',
                 'fpk_measurement_operator', 
                 'fpk_measurement_person',
                 'fpk_measurement_visit', 
                 'fpk_measurement_provider', 
                 'fpk_measurement_concept_s', 
                 'fpk_measurement_value',
                 }

    def expected_concept_fk_names(self):
        # Return a set of expected concept (vocab) foreign key names.
        return {'fpk_concept_vocabulary',
                'fpk_concept_domain',
                'fpk_concept_class',
                }

    def test_drop_strict(self):

        # Instantiate the transformed pedsnet database structure.
        self.metadata.create_all(self.engine)

        # Grab the measurement table created
        measurement = sqlalchemy.Table('measurement', sqlalchemy.MetaData(),
                                       autoload=True,
                                       autoload_with=self.engine)
        fk_names = [i.name for i in measurement.foreign_key_constraints]

        # Check that the measurement table has all foreign keys.
        for fk in self.expected_measurement_fk_names():
            self.assertIn(fk, fk_names)

        # Drop foreign keys on the non-vocabulary tables.
        drop_foreign_keys(self.conn_str, self.model_version,
                          error_mode='strict')

        # Check that the measurement table has no foreign keys.
        measurement = sqlalchemy.Table('measurement', sqlalchemy.MetaData(),
                                       autoload=True,
                                       autoload_with=self.engine)
        self.assertEqual(len(measurement.foreign_key_constraints), 0)

        # Check that vocab foreign keys were not dropped.
        concept = sqlalchemy.Table('concept', sqlalchemy.MetaData(),
                                   autoload=True, autoload_with=self.engine)
        concept_fk_names = [fk.name for fk in concept.foreign_key_constraints]
        self.assertEqual(self.expected_concept_fk_names(),
                         set(concept_fk_names))

        # Check that an exception is raised when double-dropping in strict mode
        with self.assertRaises(psycopg2.ProgrammingError):
            drop_foreign_keys(self.conn_str, self.model_version,
                         error_mode='strict')

    def test_add_strict(self):

        # Instantiate the transformed pedsnet database structure.
        self.metadata.create_all(self.engine)

        # Drop foreign keys on the non-vocabulary tables.
        drop_foreign_keys(self.conn_str, self.model_version,
                          error_mode='strict')

        # Verify that the measurement table has no foreign keys
        measurement = sqlalchemy.Table('measurement', sqlalchemy.MetaData(),
                                       autoload=True,
                                       autoload_with=self.engine)
        self.assertEqual(len(measurement.foreign_key_constraints), 0)

        # Drop foreign keys on vocabulary tables.
        drop_foreign_keys(self.conn_str, self.model_version,
                          error_mode='strict', vocabulary=True)

        # Verify that the concept table has no foreign keys.
        concept = sqlalchemy.Table('concept', sqlalchemy.MetaData(),
                                   autoload=True, autoload_with=self.engine)
        self.assertEqual(len(concept.foreign_key_constraints), 0)

        # Create foreign keys on non-vocabulary tables.
        add_foreign_keys(self.conn_str, self.model_version,
                         error_mode='strict')

        # Check that the measurement table has the right foreign keys.
        measurement = sqlalchemy.Table('measurement', sqlalchemy.MetaData(),
                                       autoload=True,
                                       autoload_with=self.engine)
        self.assertEqual(self.expected_measurement_fk_names(),
                         set([fk.name for fk in
                              measurement.foreign_key_constraints]))

        # Check that the concept table has no foreign keys.
        concept = sqlalchemy.Table('concept', sqlalchemy.MetaData(),
                                   autoload=True, autoload_with=self.engine)
        self.assertEqual(len(concept.foreign_key_constraints), 0)

        # Check that strict raises an exception if we double-add.
        with self.assertRaises(psycopg2.ProgrammingError):
            add_foreign_keys(self.conn_str, self.model_version,
                             error_mode='strict')

    def test_add_normal(self):

        # Instantiate the transformed pedsnet database structure.
        self.metadata.create_all(self.engine)

        # Drop a foreign key.
        drop_sql = 'ALTER TABLE measurement DROP CONSTRAINT ' \
                   'fpk_measurement_priority'
        Statement(drop_sql).execute(self.conn_str)

        # Verify that this foreign key is gone.
        measurement = sqlalchemy.Table('measurement', sqlalchemy.MetaData(),
                                       autoload=True,
                                       autoload_with=self.engine)
        fks = [fk.name for fk in measurement.foreign_key_constraints]
        self.assertNotIn('fpk_measurement_priority', fks)

        # Create foreign keys on non-vocabulary tables.
        add_foreign_keys(self.conn_str, self.model_version,
                         error_mode='normal')

        # Check that the measurement table has the right foreign keys.
        measurement = sqlalchemy.Table('measurement', sqlalchemy.MetaData(),
                                       autoload=True,
                                       autoload_with=self.engine)
        fks = set([fk.name for fk in measurement.foreign_key_constraints])
        self.assertEqual(self.expected_measurement_fk_names(), fks)

    def test_drop_normal(self):

        # Instantiate the transformed pedsnet database structure.
        self.metadata.create_all(self.engine)

        # Drop a foreign key.
        drop_sql = 'ALTER TABLE measurement DROP CONSTRAINT ' \
                   'fpk_measurement_priority'
        Statement(drop_sql).execute(self.conn_str)

        # Verify that this foreign key is gone.
        measurement = sqlalchemy.Table('measurement', sqlalchemy.MetaData(),
                                       autoload=True,
                                       autoload_with=self.engine)
        fks = [fk.name for fk in measurement.foreign_key_constraints]
        self.assertNotIn('fpk_measurement_priority', fks)

        # Drop foreign keys on the non-vocabulary tables.
        # This should not raise an exception.
        drop_foreign_keys(self.conn_str, self.model_version,
                          error_mode='normal')

    def test_add_force(self):

        # Instantiate the transformed pedsnet database structure.
        self.metadata.create_all(self.engine)

        # Verify nominal success when cursor.execute experiences an
        # unexpected non-connection exception. (In most or all cases, this is
        # not something the user would ever want to tolerate; the 'force' mode
        # may not be useful.)  Drop a table beforehand. This will cause the
        # constraint creation to throw a 42P01 (undefined_table/relation does
        # not exist).
        Statement('DROP TABLE observation').execute(self.conn_str)
        add_foreign_keys(self.conn_str, self.model_version, error_mode='force')

        # Verify failure when execute experiences a connection exception.
        with mock.patch('psycopg2.connect') as mock_connect:
            mock_connect.side_effect = psycopg2.OperationalError(
                'Mock connection failure')
            with self.assertRaises(psycopg2.OperationalError):
                add_foreign_keys(self.conn_str, self.model_version,
                            error_mode='force')

    def test_drop_force(self):

        # Instantiate the transformed pedsnet database structure.
        self.metadata.create_all(self.engine)

        # Verify nominal success when cursor.execute experiences an
        # unexpected non-connection exception. Drop a table beforehand. This
        # will cause the constraint removal to throw a 42P01
        # (undefined_table/relation does not exist).
        Statement('DROP TABLE observation').execute(self.conn_str)
        drop_foreign_keys(self.conn_str, self.model_version,
                          error_mode='force')

        # Verify failure when execute experiences a connection exception.
        with mock.patch('psycopg2.connect') as mock_connect:
            mock_connect.side_effect = psycopg2.OperationalError(
                'Mock connection failure')
            with self.assertRaises(psycopg2.OperationalError):
                drop_foreign_keys(self.conn_str, self.model_version,
                             error_mode='force')
