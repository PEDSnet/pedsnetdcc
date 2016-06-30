import unittest

from sqlalchemy import MetaData, Table, Column, DateTime, Integer, String

from pedsnetdcc.age_transform import AgeTransform
from pedsnetdcc.concept_name_transform import ConceptNameTransform
from pedsnetdcc.site_name_transform import SiteNameTransform

from pedsnetdcc.indexes import add_indexes_sql, drop_indexes_sql, indexes

class IndexesTest(unittest.TestCase):

    def setUp(self):
        self.metadata = MetaData()

        # Add first of two test tables to empty metadata
        self.table1 = Table('table1', self.metadata,
                            Column('start_time', DateTime),
                            Column('table1_concept_id', Integer),
                            Column('person_id', Integer, index=True))

        # Add second of two test tables to metadata
        self.table2 = Table('table2', self.metadata,
                            Column('start_time', DateTime),
                            Column('person_id', Integer))

        # Add a `person` table to metadata
        self.person = Table('person', self.metadata,
                            Column('person_id', Integer),
                            Column('time_of_birth', DateTime))

        # Add a test vocab table, 'concept' to metadata
        self.table1 = Table('concept', self.metadata,
                            Column('concept_id', Integer, index=True),
                            Column('concept_name', String, index=True))

        AgeTransform.columns = (
            ('table1', 'start_time'),
        )

        self.transforms = (
        AgeTransform, ConceptNameTransform, SiteNameTransform)

    def test_add_indexes(self):
        idx_list = indexes(self.metadata, self.transforms)
        sql = add_indexes_sql(idx_list)

        expected = set([
            'CREATE INDEX per_s_1a2331c4688eab6532680_ix ON person (site)',
            'CREATE INDEX tab_s_50b3103c7ae81ac72b766_ix ON table2 (site)',
            'CREATE INDEX tab_s_e8e4cc181f175c06e6d80_ix ON table1 (site)',
            'CREATE INDEX tab_tcn_8b2e3c0037a3e649c77_ix ON table1 (table1_concept_name)',
            'CREATE INDEX tab_saim_5c10efef3b7b0f2c42_ix ON table1 (start_age_in_months)',
            'CREATE INDEX ix_table1_person_id ON table1 (person_id)'
        ])
        self.assertEquals(set(sql), expected)

    def test_drop_indexes(self):
        idx_list = indexes(self.metadata, self.transforms)
        sql = drop_indexes_sql(idx_list)

        expected = set([
            'DROP INDEX per_s_1a2331c4688eab6532680_ix',
            'DROP INDEX tab_s_50b3103c7ae81ac72b766_ix',
            'DROP INDEX tab_s_e8e4cc181f175c06e6d80_ix',
            'DROP INDEX tab_tcn_8b2e3c0037a3e649c77_ix',
            'DROP INDEX tab_saim_5c10efef3b7b0f2c42_ix',
            'DROP INDEX ix_table1_person_id'
        ])
        self.assertEquals(set(sql), expected)

    def test_add_indexes_for_vocabulary(self):
        idx_list = indexes(self.metadata, self.transforms, vocabulary=True)
        sql = add_indexes_sql(idx_list)

        expected = set([
            'CREATE INDEX ix_concept_concept_name ON concept (concept_name)',
            'CREATE INDEX ix_concept_concept_id ON concept (concept_id)'
        ])
        self.assertEqual(set(sql), expected)