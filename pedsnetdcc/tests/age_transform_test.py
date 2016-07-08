import os
import unittest

from sqlalchemy import MetaData, Table, Column, Integer, DateTime, select
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex

from pedsnetdcc.age_transform import AgeTransform
from pedsnetdcc.utils import make_conn_str
from pedsnetdcc.tests.transform_test_utils import clean


class AgeTest(unittest.TestCase):

    def setUp(self):
        self.metadata = MetaData()

        foo_col = Column('foo_start_time', DateTime)
        bar_col = Column('bar_start_time', DateTime)
        person_col = Column('person_id', Integer)

        self.table1 = Table('table1', self.metadata,
                            foo_col,
                            bar_col,
                            person_col)

        baz_col = Column('baz_start_time', DateTime)
        baz_person_col = Column('person_id', Integer)

        self.table2 = Table('table2', self.metadata,
                            baz_col, baz_person_col)

        # Create and add the `person` table to the sqlalchemy metadata
        self.person = Table('person', self.metadata,
                            Column('person_id', Integer),
                            Column('time_of_birth', DateTime))

        AgeTransform.columns = (
            ('table1', 'foo_start_time'),
            ('table1', 'bar_start_time')
        )

    def test_modify_select(self):

        select_obj = select([self.table1])
        join_obj = self.table1

        select_obj, join_obj = AgeTransform.modify_select(
            self.metadata,
            'user',
            select_obj,
            join_obj)

        select_obj = select_obj.select_from(join_obj)

        new_sql = str(select_obj.compile(dialect=postgresql.dialect()))

        expected = clean("""
          SELECT table1.foo_start_time,
          table1.bar_start_time,
          table1.person_id,
          months_in_interval(person.time_of_birth, table1.foo_start_time)
            AS foo_start_age_in_months,
          months_in_interval(person.time_of_birth, table1.bar_start_time)
            AS bar_start_age_in_months
          {NL}FROM table1
            JOIN person ON person.person_id = table1.person_id
          """)

        self.maxDiff = None
        self.assertEqual(new_sql, expected)

    def test_modify_metadata(self):

        metadata = AgeTransform.modify_metadata(self.metadata)

        indexes = metadata.tables['table1'].indexes
        self.assertEqual(len(indexes), 2, 'Indexes created')

        for index in indexes:
            index_sql = str(CreateIndex(index).compile(
                dialect=postgresql.dialect()))
            if index.name == 'tab_fsaim_107eee9e009461416_ix':
                expected = clean("""
                  CREATE INDEX tab_fsaim_107eee9e009461416_ix
                    ON table1 (foo_start_age_in_months)
                """)
                self.assertEqual(index_sql, expected)
            elif index.name == 'tab_bsaim_ca07fdbcdf9bfef7a_ix':
                expected = clean("""
                  CREATE INDEX tab_bsaim_ca07fdbcdf9bfef7a_ix
                    ON table1 (bar_start_age_in_months)
                """)
                self.assertEqual(index_sql, expected)
            else:
                self.fail(
                    'Unexpected index encountered: {}'.format(index.name))

    def test_pre_transform(self):
        dburi_var = 'PEDSNETDCC_TEST_DBURI'
        search_path_var = 'PEDSNETDCC_TEST_SEARCH_PATH'
        if (dburi_var not in os.environ and
                search_path_var not in os.environ):
            self.skipTest(
                '{} and {} required for testing '
                'AgeTransform.pre_transform'.format(
                    dburi_var, search_path_var))
        conn_str = make_conn_str(uri=os.environ[dburi_var],
                                 search_path=os.environ[search_path_var])
        AgeTransform.pre_transform(conn_str)
        # TODO: verify function creation via introspection

    def test_with_data(self):
        # TODO: use test data and verify transformation results
        self.skipTest('Not implemented yet')
