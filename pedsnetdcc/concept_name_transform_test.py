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
        col3 = Column('user_name', String(16))

        user = Table('user', metadata,
                     col1,
                     col2,
                     col3)

        # Create and add the `concept` table to the sqlalchemy metadata
        concept = Table('concept', metadata, Column('concept_id', Integer),
                        Column('concept_name', String(512)))

        sel = select().column(col1).column(col2).select_from(user)

        new_sel = ConceptNameTransform.modify_select(metadata, 'user', sel)
        new_sql = str(new_sel.compile(dialect=postgresql.dialect()))

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
