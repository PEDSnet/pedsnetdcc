import os
import sqlalchemy
import sqlalchemy.dialects.postgresql
import unittest

from pedsnetdcc.id_mapping_transform import IDMappingTransform
from pedsnetdcc.tests.transform_test_utils import clean
from pedsnetdcc.utils import make_conn_str


class IDMappingTransformTest(unittest.TestCase):

    def setUp(self):
        self.metadata = sqlalchemy.MetaData()

        foo_id_col = sqlalchemy.Column('id', sqlalchemy.Integer,
                                       primary_key=True)
        bar_fk_col = sqlalchemy.Column('bar_id', sqlalchemy.Integer,
                                       sqlalchemy.ForeignKey('bar.id'))
        baz_fk_col = sqlalchemy.Column('baz_id', sqlalchemy.Integer,
                                       sqlalchemy.ForeignKey('baz.id'),
                                       nullable=False)
        self.foo_tbl = sqlalchemy.Table('foo', self.metadata, foo_id_col,
                                        bar_fk_col, baz_fk_col)

        bar_id_col = sqlalchemy.Column('id', sqlalchemy.Integer,
                                       primary_key=True)
        self.bar_tbl = sqlalchemy.Table('bar', self.metadata, bar_id_col)

        baz_id_col = sqlalchemy.Column('id', sqlalchemy.Integer,
                                       primary_key=True)
        self.baz_tbl = sqlalchemy.Table('baz', self.metadata, baz_id_col)

    def test_modify_select(self):

        select_obj, join_obj = IDMappingTransform.modify_select(self.metadata,
                                                                'foo')

        select_obj = select_obj.select_from(join_obj)

        new_sql = str(select_obj.compile(
            dialect=sqlalchemy.dialects.postgresql.dialect()))

        # Two versions since ordering is not guaranteed.
        expected1 = clean("""
          SELECT foo_ids.dcc_id AS id,
            bar_ids.dcc_id AS bar_id,
            baz_ids.dcc_id AS baz_id
          {NL}FROM foo
            JOIN foo_ids ON foo.id = foo_ids.site_id
            LEFT OUTER JOIN bar_ids ON foo.bar_id = bar_ids.site_id
            JOIN baz_ids ON foo.baz_id = baz_ids.site_id
          """)

        expected2 = clean("""
          SELECT foo_ids.dcc_id AS id,
            bar_ids.dcc_id AS bar_id,
            baz_ids.dcc_id AS baz_id
          {NL}FROM foo
            JOIN foo_ids ON foo.id = foo_ids.site_id
            LEFT OUTER JOIN bar_ids ON foo.bar_id = bar_ids.site_id
            JOIN baz_ids ON foo.baz_id = baz_ids.site_id
          """)

        self.assertTrue(new_sql == expected1 or new_sql == expected2)

    def test_modify_metadata(self):
        metadata = IDMappingTransform.modify_metadata(self.metadata)

        self.assertTrue('site_id' in metadata.tables['foo'].c)

    def test_pre_transform(self):

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
        IDMappingTransform.pre_transform(conn_str, self.metadata, 'foo')
        # TODO: verify function creation via introspection

    def test_with_data(self):
        # TODO: use test data and verify transformation results
        self.skipTest('Not implemented yet')
