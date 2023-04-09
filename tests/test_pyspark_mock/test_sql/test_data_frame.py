import sys
sys.path.append('src')
import unittest

import pandas as pd

from pyspark_mock.sql import DataFrame as MockedDataFrame
from pyspark_mock.sql import functions as F


class TestDataFrame(unittest.TestCase):

    def test_count_method(self):

        observations = [
            ('A', 1),
            ('B', 2),
            ('C', 3),
        ]
        columns = ['col1', 'col2']

        df = self._createDataFrame(observations, columns)

        self.assertEqual(df.count(), 3)

    def test_columns_property(self):

        observations = [
            (1,2,3,4),
            (1,2,3,4)
        ]
        columns = ['col1', 'col2', 'col3', 'col4']
        
        df = self._createDataFrame(observations, columns)

        self.assertListEqual(df.columns, columns)

    def test_lit_function(self):

        observations = [
            ('A', 1),
            ('B', 2),
            ('C', 3),
        ]
        columns = ['col1', 'col2']

        df = self._createDataFrame(observations, columns)
        df = df.withColumn('literal_value', F.lit('literal'))

        self.assertListEqual(list(df.pd_df.literal_value), ['literal'] * 3)

    def test_col_function(self):

        observations = [
            ('A1', 'B1'),
            ('A2', 'B2'),
            ('A3', 'B3'),
            ('A4', 'B4'),
            ('A5', 'B5')
        ]
        columns = ['A', 'B']

        df = self._createDataFrame(observations, columns)
        df = df.withColumn('A_copy', F.col('A'))
        df = df.withColumn('B_copy', F.col('B'))

        self.assertListEqual(list(df.pd_df.A_copy), ['A1', 'A2', 'A3', 'A4', 'A5'])
        self.assertListEqual(list(df.pd_df.B_copy), ['B1', 'B2', 'B3', 'B4', 'B5'])

    def test_asc_function(self):
        self.assertFalse(True) #TODO: testar
        
    def test_desc_function(self):
        self.assertFalse(True) #TODO: testar

    def test_sqrt_function(self):
        
        observations = [
            (1),
            (4),
            (9),
            (16),
            (100)
        ]
        columns = ['number']
        df = self._createDataFrame(observations, columns)
        df = df.withColumn('result', F.sqrt('number'))

        self.assertListEqual(list(df.pd_df.result), [1.0, 2.0, 3.0, 4.0, 10.0])

    def test_abs_function(self):
        
        observations = [
            (1),
            (2),
            (-95),
            (-16),
            (10)
        ]
        columns = ['number']
        df = self._createDataFrame(observations, columns)
        df = df.withColumn('result', F.abs('number'))

        self.assertListEqual(list(df.pd_df.result), [1, 2, 95, 16, 10])

    def _createDataFrame(self, observations, columns):

        pd_df = pd.DataFrame(observations, columns=columns)

        df = MockedDataFrame(pd_df)

        return df

    def test_mode_function(self):
        self.assertFalse(True) #TODO: testar

    def test_min_function(self):
        self.assertFalse(True) #TODO: testar

    def test_max_function(self):
        self.assertFalse(True) #TODO: testar

    def test_max_by_function(self):
        self.assertFalse(True) #TODO: testar

    def test_count_function(self):
        self.assertFalse(True) #TODO: testar

    def test_greatest_function(self):
        
        observations = [
            (1, 2, 3, 4),
            (-3, 0, 1, None),
            (0, 0, 2, 2),
            (-2, None, -1, 0)
        ]
        columns = ['n1', 'n2', 'n3', 'n4']
        df = self._createDataFrame(observations, columns)
        df = df.withColumn('result', F.greatest('n1', 'n2', 'n3', 'n4'))

        self.assertListEqual(list(df.pd_df.result), [4, 1, 2, 0])

    def test_add_operator(self):

        observations = [
            (1, 1),
            (2, 2),
            (-2, 2),
            (4, 2),
            (0, 0),
            (1, 0),
            (0, 1)
        ]
        columns = ['n1', 'n2']
        df = self._createDataFrame(observations, columns)
        df = df.withColumn('result', F.col('n1') + F.col('n2'))

        self.assertListEqual(list(df.pd_df.result), [2, 4, 0, 6, 0, 1, 1])

    def test_sub_operator(self):

        observations = [
            (1, 1),
            (2, 2),
            (-2, 2),
            (4, 2),
            (0, 0),
            (1, 0),
            (0, 1)
        ]
        columns = ['n1', 'n2']
        df = self._createDataFrame(observations, columns)
        df = df.withColumn('result', F.col('n1') - F.col('n2'))

        self.assertListEqual(list(df.pd_df.result), [0, 0, -4, 2, 0, 1, -1])

    def test_mult_operator(self):

        observations = [
            (1, 1),
            (2, 2),
            (-2, 2),
            (4, 2),
            (0, 0),
            (1, 0),
            (0, 1)
        ]
        columns = ['n1', 'n2']
        df = self._createDataFrame(observations, columns)
        df = df.withColumn('result', F.col('n1') * F.col('n2'))

        self.assertListEqual(list(df.pd_df.result), [1, 4, -4, 8, 0, 0, 0])

    def test_div_operator(self):

        observations = [
            (4, 2),
            (1, 1),
            (3, 1),
            (0, 1),
            (0, 2),
            (-2, 2),
            (-4, 2),
            (1, 2),
            (1, -2)
        ]
        columns = ['n1', 'n2']
        df = self._createDataFrame(observations, columns)
        df = df.withColumn('result', F.col('n1') / F.col('n2'))

        expected_results = [2.0, 1.0, 3.0, 0.0, 0.0, -1.0, -2.0, 0.5, -0.5]

        self.assertListEqual(list(df.pd_df.result), expected_results)

    def test_less_than_operator(self):
        observations = [
            (1, 0),
            (1, 1),
            (0, 1),
            (-2, 0),
            (-2, -2),
            (4, 2),
            (4, 6)
        ]

        columns = ['n1', 'n2']
        df = self._createDataFrame(observations, columns)
        df = df.withColumn('result', F.col('n1') < F.col('n2'))

        expected_results = [False, False, True, True, False, False, True]

        self.assertListEqual(list(df.pd_df.result), expected_results)
