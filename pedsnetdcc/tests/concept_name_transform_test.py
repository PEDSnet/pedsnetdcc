import unittest

from sqlalchemy import *
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex

from pedsnetdcc.concept_name_transform import ConceptNameTransform
from pedsnetdcc.tests.transform_test_utils import clean


class ConceptNameTest(unittest.TestCase):

    def setUp(self):
        self.metadata = MetaData()

        foo_col = Column('foo_concept_id', Integer)
        bar_col = Column('bar_concept_id', Integer)

        self.table1 = Table('table1', self.metadata,
                            foo_col,
                            bar_col)

        baz_col = Column('baz_concept_id', Integer)

        self.table2 = Table('table2', self.metadata,
                            baz_col)

        # Create and add the `concept` table to the sqlalchemy metadata
        self.concept = Table('concept', self.metadata,
                             Column('concept_id', Integer),
                             Column('concept_name', String(512)))

    def test_modify_select(self):

        select_obj = select([self.table1])
        join_obj = self.table1

        select_obj, join_obj = ConceptNameTransform.modify_select(
            self.metadata,
            'user',
            select_obj,
            join_obj)

        select_obj = select_obj.select_from(join_obj)

        new_sql = str(select_obj.compile(dialect=postgresql.dialect()))

        expected = clean("""
          SELECT table1.foo_concept_id, table1.bar_concept_id,
          concept_1.concept_name AS foo_concept_name, concept_2.concept_name
          AS bar_concept_name
          {NL}FROM table1
          LEFT OUTER JOIN concept AS concept_1
              ON concept_1.concept_id = foo_concept_id
          LEFT OUTER JOIN concept AS concept_2
              ON concept_2.concept_id = bar_concept_id
        """)

        self.maxDiff = None
        self.assertEqual(expected, new_sql)

    def test_modify_metadata(self):

        metadata = ConceptNameTransform.modify_metadata(self.metadata)

        indexes = metadata.tables['table1'].indexes
        self.assertEqual(len(indexes), 2, 'Indexes created')

        for index in indexes:
            index_sql = str(CreateIndex(index).compile(
                dialect=postgresql.dialect()))
            if index.name == 'tab_fcn_81387a6132eed7f69b5_ix':
                expected = clean("""
                  CREATE INDEX tab_fcn_81387a6132eed7f69b5_ix
                    ON table1 (foo_concept_name)
                """)
                self.assertEqual(index_sql, expected)
            elif index.name == 'tab_bcn_7baa5e16ad1f8129b90_ix':
                expected = clean("""
                  CREATE INDEX tab_bcn_7baa5e16ad1f8129b90_ix
                    ON table1 (bar_concept_name)
                """)
                self.assertEqual(index_sql, expected)
            else:
                self.fail(
                    'Unexpected index encountered: {}'.format(index.name))
