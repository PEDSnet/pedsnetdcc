import unittest

from sqlalchemy import Column, MetaData, Integer, Table, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex

from pedsnetdcc.site_name_transform import SiteNameTransform
from pedsnetdcc.transform_test_utils import clean


class SiteNameTest(unittest.TestCase):

    def setUp(self):
        self.metadata = MetaData()
        self.metadata.info['site'] = 'test'

        foo_col = Column('foo', Integer)
        bar_col = Column('bar', Integer)

        self.table1 = Table('table1', self.metadata,
                            foo_col,
                            bar_col)

        baz_col = Column('baz', Integer)

        self.table2 = Table('table2', self.metadata,
                            baz_col)

    def test_modify_select(self):

        select_obj = select([self.table1])
        join_obj = self.table1

        select_obj, join_obj = SiteNameTransform.modify_select(
            self.metadata,
            'table1',
            select_obj,
            join_obj)

        select_obj = select_obj.select_from(join_obj)

        new_sql = str(select_obj.compile(dialect=postgresql.dialect()))

        expected = clean("""
          SELECT table1.foo, table1.bar,
          'test'::varchar(32) AS site
          {NL}FROM table1
        """)

        self.maxDiff = None
        self.assertEqual(expected, new_sql)

    def test_modify_metadata(self):

        metadata = SiteNameTransform.modify_metadata(self.metadata)

        indexes = metadata.tables['table1'].indexes
        self.assertEqual(len(indexes), 1, 'Indexes created')

        for index in indexes:
            index_sql = str(CreateIndex(index).compile(
                dialect=postgresql.dialect()))
            expected = clean("""
                CREATE INDEX tab_s_e8e4cc181f175c06e6d80_ix
                  ON table1 (site)
                """)
            self.assertEqual(index_sql, expected)
