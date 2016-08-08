import unittest

from pedsnetdcc.transform_runner import _transform_select_sql

class TransformRunnerTest(unittest.TestCase):

    def setUp(self):
        self.model_version = '2.3.0'

    def test_select_sql(self):
        sql_set = _transform_select_sql('2.3.0', 'testsite', 'foo_transformed')
        self.assertEqual(sql_set, set())
