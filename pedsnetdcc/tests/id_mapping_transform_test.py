import os
import sqlalchemy
import sqlalchemy.dialects.postgresql
import unittest

from pedsnetdcc.id_mapping_transform import IDMappingTransform
from pedsnetdcc.utils import make_conn_str, stock_metadata


class IDMappingTransformTest(unittest.TestCase):

    def setUp(self):
        self.metadata = stock_metadata('2.2.0')

    def test_modify_person_select(self):

        person = self.metadata.tables['person']

        s = sqlalchemy.select([person])
        j = person

        s, j = IDMappingTransform.modify_select(self.metadata, 'person', s, j)

        s = s.select_from(j)

        sql = str(s.compile(dialect=sqlalchemy.dialects.postgresql.dialect()))

        self.assertIn('person.person_id AS site_id', sql)
        self.assertIn('person_ids.dcc_id AS person_id', sql)
        self.assertIn('location_ids.dcc_id AS location_id', sql)
        self.assertIn('care_site_ids.dcc_id AS care_site_id', sql)
        self.assertIn('provider_ids.dcc_id AS provider_id', sql)
        self.assertIn('JOIN person_ids ON person.person_id ='
                      ' person_ids.site_id', sql)
        self.assertIn('JOIN care_site_ids ON person.care_site_id ='
                      ' care_site_ids.site_id', sql)
        self.assertIn('LEFT OUTER JOIN provider_ids ON person.provider_id ='
                      ' provider_ids.site_id', sql)
        self.assertIn('LEFT OUTER JOIN location_ids ON person.location_id ='
                      ' location_ids.site_id', sql)

    def test_modify_fact_relationship_select(self):

        fact_rel = self.metadata.tables['fact_relationship']

        s = sqlalchemy.select([fact_rel])
        j = fact_rel

        s, j = IDMappingTransform.modify_select(self.metadata,
                                                'fact_relationship', s, j)

        s = s.select_from(j)

        stmt = s.compile(dialect=sqlalchemy.dialects.postgresql.dialect())

        sql = str(stmt) % stmt.params

        self.assertIn('fact_relationship.fact_id_1 AS site_id_1', sql)
        self.assertIn('fact_relationship.fact_id_2 AS site_id_2', sql)
        self.assertIn('CASE fact_relationship.domain_concept_id_1'
                      ' WHEN 8 THEN visit_occurrence_ids_1.dcc_id'
                      ' WHEN 27 THEN observation_ids_1.dcc_id'
                      ' WHEN 21 THEN measurement_ids_1.dcc_id'
                      ' END AS fact_id_1', sql)
        self.assertIn('CASE fact_relationship.domain_concept_id_2'
                      ' WHEN 8 THEN visit_occurrence_ids_2.dcc_id'
                      ' WHEN 27 THEN observation_ids_2.dcc_id'
                      ' WHEN 21 THEN measurement_ids_2.dcc_id'
                      ' END AS fact_id_2', sql)
        self.assertIn('LEFT OUTER JOIN visit_occurrence_ids AS'
                      ' visit_occurrence_ids_1 ON'
                      ' fact_relationship.fact_id_1 ='
                      ' visit_occurrence_ids_1.site_id AND'
                      ' fact_relationship.domain_concept_id_1 = 8', sql)
        self.assertIn('LEFT OUTER JOIN visit_occurrence_ids AS'
                      ' visit_occurrence_ids_2 ON'
                      ' fact_relationship.fact_id_2 ='
                      ' visit_occurrence_ids_2.site_id AND'
                      ' fact_relationship.domain_concept_id_2 = 8', sql)
        self.assertIn('LEFT OUTER JOIN observation_ids AS'
                      ' observation_ids_1 ON'
                      ' fact_relationship.fact_id_1 ='
                      ' observation_ids_1.site_id AND'
                      ' fact_relationship.domain_concept_id_1 = 27', sql)
        self.assertIn('LEFT OUTER JOIN observation_ids AS'
                      ' observation_ids_2 ON'
                      ' fact_relationship.fact_id_2 ='
                      ' observation_ids_2.site_id AND'
                      ' fact_relationship.domain_concept_id_2 = 27', sql)
        self.assertIn('LEFT OUTER JOIN measurement_ids AS'
                      ' measurement_ids_1 ON'
                      ' fact_relationship.fact_id_1 ='
                      ' measurement_ids_1.site_id AND'
                      ' fact_relationship.domain_concept_id_1 = 21', sql)
        self.assertIn('LEFT OUTER JOIN measurement_ids AS'
                      ' measurement_ids_2 ON'
                      ' fact_relationship.fact_id_2 ='
                      ' measurement_ids_2.site_id AND'
                      ' fact_relationship.domain_concept_id_2 = 21', sql)

    def test_modify_metadata(self):
        metadata = IDMappingTransform.modify_metadata(self.metadata)
        self.assertTrue('site_id' in metadata.tables['person'].c)
        self.assertFalse('site_id' in metadata.tables['fact_relationship'].c)

    def test_pre_transform(self):
        # TODO: run pre transform with test data to ensure it works
        dburi_var = 'PEDSNETDCC_TEST_DBURI'
        search_path_var = 'PEDSNETDCC_TEST_SEARCH_PATH'
        if (dburi_var not in os.environ and
                search_path_var not in os.environ):
            self.skipTest(
                '{} and {} required for testing '
                'IDMappingTransform.pre_transform'.format(
                    dburi_var, search_path_var))
        conn_str = make_conn_str(uri=os.environ[dburi_var],
                                 search_path=os.environ[search_path_var])
        IDMappingTransform.pre_transform(conn_str, self.metadata)

    def test_with_data(self):
        # TODO: use test data and verify transformation results
        self.skipTest('Not implemented yet')
