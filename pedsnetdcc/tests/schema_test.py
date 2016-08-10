import unittest

import sqlalchemy
import testing.postgresql

from pedsnetdcc.schema import (create_schema, drop_schema, schema_exists)
from pedsnetdcc.utils import (make_conn_str, DatabaseError)
from pedsnetdcc.db import Statement


def setUpModule():
    # Generate a Postgresql class which caches the init-ed database across
    # multiple ephemeral database cluster instances.
    global Postgresql
    Postgresql = testing.postgresql.PostgresqlFactory(
        cache_intialized_db=True)


def tearDownModule():
    # Clear cached init-ed database at end of tests.
    Postgresql.clear_cache()


class SchemaTest(unittest.TestCase):

    def setUp(self):
        # Create a postgres database in a temp directory.
        self.postgresql = Postgresql()
        self.dburi = self.postgresql.url()
        self.conn_str = make_conn_str(self.dburi)
        self.engine = sqlalchemy.create_engine(self.dburi)
        self.schema = 'peds_test'

    def tearDown(self):
        # Destroy the postgres database.
        self.postgresql.stop()

    def _schema_exists(self):
        return schema_exists(self.conn_str, self.schema)

    def test_create(self):
        create_schema(self.conn_str, self.schema)
        self.assertTrue(self._schema_exists())

        with self.assertRaises(DatabaseError):
            create_schema(self.conn_str, self.schema)

        create_schema(self.conn_str, self.schema, force=True)

    def test_drop(self):
        create_schema(self.conn_str, self.schema)

        drop_schema(self.conn_str, self.schema, if_exists=False, cascade=False)
        self.assertFalse(self._schema_exists())

        with self.assertRaises(DatabaseError):
            drop_schema(self.conn_str, self.schema, if_exists=False,
                        cascade=False)

        drop_schema(self.conn_str, self.schema, if_exists=True, cascade=False)

    def test_drop_cascading(self):
        create_schema(self.conn_str, self.schema)

        sql = "create table {}.junk(junk int)".format(self.schema)
        stmt = Statement(sql)
        stmt.execute(self.conn_str)
        self.assertIsNone(stmt.err)

        with self.assertRaises(DatabaseError):
            drop_schema(self.conn_str, self.schema, if_exists=False,
                        cascade=False)

        drop_schema(self.conn_str, self.schema, if_exists=True, cascade=True)
        self.assertFalse(self._schema_exists())
