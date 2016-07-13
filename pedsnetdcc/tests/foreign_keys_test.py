import testing.postgresql
import unittest

from pedsnetdcc.foreign_keys import (_foreign_keys_from_model_version,
                                     create_foreign_keys, drop_foreign_keys)
from pedsnetdcc.utils import make_conn_str, stock_metadata

Postgresql = None


def setUpModule():
    # Generate a Postgresql class which caches the init-ed database across
    # multiple ephemeral database cluster instances.
    global Postgresql
    Postgresql = testing.postgresql.PostgresqlFactory(
            cache_intialized_db=True)


def tearDownModule():
    # Clear cached init-ed database at end of tests.
    Postgresql.clear_cache()


class ForeignKeysTest(unittest.TestCase):

    def setUp(self):
        # Create a postgres database in a temp directory.
        self.postgresql = Postgresql()
        self.dburi = self.postgresql.url()
        self.conn_str = make_conn_str(self.dburi)

        # Create stock PEDSnet 2.2 metadata.
        self.metadata = stock_metadata('2.2.0')

    def tearDown(self):
        # Destroy the postgres database.
        self.postgresql.stop()

    def test_foreign_keys_from_model_version(self):
        # expected_fkey1 = self.metadata.tables['person'].constraints

        # TODO: Implement tests once rebased onto Python 2 fixing commits.
        pass
