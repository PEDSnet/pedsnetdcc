import re
from sqlalchemy import *
from sqlalchemy.dialects import postgresql
import unittest
from pedsnetdcc.concept_name_transform import ConceptNameTransform


def clean(s):
    """Strip leading & trailing space, remove newlines, compress space.
    Also expand '{NL}' to a literal newline.
    """
    s = s.strip()
    s = re.sub(' +', ' ', s)
    s = s.replace('\n', '')
    s = s.replace('\r', '')
    s = s.replace('{NL}', '\n')
    return s


class ConceptNameTest(unittest.TestCase):
    def test(self):
        """
        If you say so ...
        """
        metadata = MetaData()

        col1 = Column('foo_concept_id', Integer)
        col2 = Column('bar_concept_id', Integer)

        user = Table('user', metadata,
                     col1,
                     col2)

        # Create and add the `concept` table to the sqlalchemy metadata
        concept = Table('concept', metadata, Column('concept_id', Integer),
                        Column('concept_name', String(512)))

        select_obj = select([user])
        join_obj = user

        select_obj, join_obj = ConceptNameTransform.modify_select(metadata,
                                                                  'user',
                                                                  select_obj,
                                                                  join_obj)
        select_obj = select_obj.select_from(join_obj)

        new_sql = str(select_obj.compile(dialect=postgresql.dialect()))

        expected = clean("""
          SELECT "user".foo_concept_id, "user".bar_concept_id,
          concept_1.concept_name AS foo_concept_name, concept_2.concept_name
          AS bar_concept_name
          {NL}FROM "user"
          LEFT OUTER JOIN concept AS concept_1
              ON concept_1.concept_id = foo_concept_id
          LEFT OUTER JOIN concept AS concept_2
              ON concept_2.concept_id = bar_concept_id
        """)

        self.maxDiff = None
        self.assertEqual(expected, new_sql)
