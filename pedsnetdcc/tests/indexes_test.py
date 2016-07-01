import os
import unittest

from pedsnetdcc.indexes import indexes_sql

import pedsnetdcc

class IndexesTest(unittest.TestCase):

    def setUp(self):
        self.model_version = '2.2.0'

    def test_add_indexes(self):
        sql = indexes_sql(self.model_version)

        sample_expected = (
            'CREATE INDEX obs_otcn_89a4742c38ecb8ba35_ix ON observation (observation_type_concept_name)',
            'CREATE INDEX dea_s_4906dc6995505fc71431f_ix ON death (site)',
            'CREATE INDEX vis_vsaim_f1537dca8da9ab914_ix ON visit_occurrence (visit_start_age_in_months)',
        )
        for sample in sample_expected:
            self.assertIn(sample, sql)

        sample_not_expected = (
            'CREATE INDEX idx_concept_vocabluary_id ON concept (vocabulary_id)',
        )
        for sample in sample_not_expected:
            self.assertNotIn(sample, sql)

    def test_drop_indexes(self):
        sql = indexes_sql(self.model_version, drop=True)

        sample_expected = (
            'DROP INDEX obs_otcn_89a4742c38ecb8ba35_ix',
            'DROP INDEX dea_s_4906dc6995505fc71431f_ix',
            'DROP INDEX vis_vsaim_f1537dca8da9ab914_ix'
        )
        for sample in sample_expected:
            self.assertIn(sample, sql)

        sample_not_expected = (
            'CREATE INDEX idx_concept_vocabluary_id ON concept (vocabulary_id)',
        )
        for sample in sample_not_expected:
            self.assertNotIn(sample, sql)

    def test_add_indexes_for_vocabulary(self):
        sql = indexes_sql(self.model_version, vocabulary=True)

        sample_expected = (
            'CREATE INDEX idx_concept_class_id ON concept (concept_class_id)',
            'CREATE INDEX idx_concept_synonym_id ON concept_synonym (concept_id)'
        )
        for sample in sample_expected:
            self.assertIn(sample, sql)

        sample_not_expected = (
            'CREATE INDEX con_lcn_f7a508db6a172c78291_ix ON concept_synonym (language_concept_name)',
            'CREATE INDEX con_s_d9ad76e415cb919c49e49_ix ON concept_class (site)'
        )
        for sample in sample_not_expected:
            self.assertNotIn(sample, sql)
