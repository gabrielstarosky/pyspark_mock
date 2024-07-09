import sys
sys.path.append('src')
from typing import List, Tuple
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

    def _createDataFrame(self, observations: List[Tuple[str]], columns: List[str]):

        pd_df = pd.DataFrame(observations, columns=columns)

        df = MockedDataFrame(pd_df)

        return df

    def test_mode_function(self):
        self.assertFalse(True) #TODO: testar

    def test_min_function(self):
        observations = [
            ('A', 'Cat1',1),
            ('A', 'Cat1',4),
            ('A', 'Cat2',1),
            ('A', 'Cat3',6),
            ('A', 'Cat4',-4),
            ('B', 'Cat1',0),
            ('B', 'Cat2',4),
            ('B', 'Cat3',2),
            ('B', 'Cat4',4),
            ('C', 'Cat2',5),
            ('C', 'Cat2',2),
            ('D', 'Cat3',4),
            ('D', 'Cat3',5),
            ('D', 'Cat4',5),
        ]
        columns = ['agg1', 'agg2', 'value1']

        df = self._createDataFrame(observations, columns)
        with self.subTest():

            list_df = lambda df: df.pd_df.values.tolist()

            actual_df = df.groupby('agg1').min('value1')

            self.assertListEqual(list_df(actual_df), [[-4], [0], [2], [4]])

            actual_df = df.groupby('agg2').min('value1')

            self.assertListEqual(list_df(actual_df), [[0], [1], [2], [-4]])

            actual_df = df.groupby(['agg1', 'agg2']).min('value1')

            self.assertListEqual(list_df(actual_df),
                                 [[1], [1], [6], [-4],
                                  [0], [4], [2], [4],
                                  [2],
                                  [4], [5]]
                                 )
            
            actual_df = df.groupby().min('value1')

            self.assertListEqual(list_df(actual_df), [[-4]])


    def test_max_function(self):
        observations = [
            ('A', 'Cat1',1),
            ('A', 'Cat1',4),
            ('A', 'Cat2',1),
            ('A', 'Cat3',6),
            ('A', 'Cat4',-4),
            ('B', 'Cat1',0),
            ('B', 'Cat2',4),
            ('B', 'Cat3',2),
            ('B', 'Cat4',4),
            ('C', 'Cat2',5),
            ('C', 'Cat2',2),
            ('D', 'Cat3',4),
            ('D', 'Cat3',5),
            ('D', 'Cat4',5),
        ]
        columns = ['agg1', 'agg2', 'value1']

        df = self._createDataFrame(observations, columns)
        with self.subTest():

            list_df = lambda df: df.pd_df.values.tolist()

            actual_df = df.groupby('agg1').max('value1')

            self.assertListEqual(list_df(actual_df), [[6], [4], [5], [5]])

            actual_df = df.groupby('agg2').max('value1')

            self.assertListEqual(list_df(actual_df), [[4], [5], [6], [5]])

            actual_df = df.groupby(['agg1', 'agg2']).max('value1')

            self.assertListEqual(list_df(actual_df),
                                 [[4], [1], [6], [-4],
                                  [0], [4], [2], [4],
                                  [5],
                                  [5], [5]]
                                 )
            
            actual_df = df.groupby().max('value1')

            self.assertListEqual(list_df(actual_df), [[6]])

    def test_max_by_function(self):
        self.assertFalse(True) #TODO: testar

    def test_count_function(self):
        
        observations = [
            ('A', 'Cat1', 1000, 2000, 3000),
            ('A', 'Cat1', 1000, 2000, 3000),
            ('A', 'Cat2', 1000, 2000, 3000),
            ('A', 'Cat3', 1000, 2000, 3000),
            ('A', 'Cat4', 1000, 2000, 3000),
            ('B', 'Cat1', 1000, 2000, 3000),
            ('B', 'Cat2', 1000, 2000, 3000),
            ('B', 'Cat3', 1000, 2000, 3000),
            ('B', 'Cat4', 1000, 2000, 3000),
            ('C', 'Cat2', 1000, 2000, 3000),
            ('C', 'Cat2', 1000, 2000, 3000),
            ('D', 'Cat3', 1000, 2000, 3000),
            ('D', 'Cat3', 1000, 2000, 3000),
            ('D', 'Cat4', 1000, 2000, 3000),
            ('E', None, 1000, 2000, 3000),
            ('E', None, 1000, 2000, 3000),
            ('E', None, 1000, 2000, 3000),
            ('E', None, 1000, 2000, 3000),
            ('E', None, 1000, 2000, 3000),
            ('E', None, 1000, 2000, 3000),
        ]
        columns = ['agg1', 'agg2', 'value1', 'value2', 'value3']

        df = self._createDataFrame(observations, columns)
        with self.subTest():

            list_df = lambda df: df.pd_df.values.tolist()

            actual_df = df.groupby('agg1').count()

            self.assertListEqual(list_df(actual_df), [[5], [4], [2], [3], [6]])

            actual_df = df.groupby('agg2').count()

            self.assertListEqual(list_df(actual_df), [[3], [4], [4], [3]])

            actual_df = df.groupby(['agg1', 'agg2']).count()

            self.assertListEqual(list_df(actual_df), [[2], [1], [1], [1], [1], [1], [1], [1], [2], [2], [1]])

            actual_df = df.groupby().count()

            self.assertListEqual(list_df(actual_df), [20])

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

    def test_less_or_equal_than_operator(self):
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
        df = df.withColumn('result', F.col('n1') <= F.col('n2'))

        expected_results = [False, True, True, True, True, False, True]

        self.assertListEqual(list(df.pd_df.result), expected_results)

    def test_equal_operator(self):
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
        df = df.withColumn('result', F.col('n1') == F.col('n2'))

        expected_results = [False, True, False, False, True, False, False]

        self.assertListEqual(list(df.pd_df.result), expected_results)

    def test_not_equal_operator(self):
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
        df = df.withColumn('result', F.col('n1') != F.col('n2'))

        expected_results = [True, False, True, True, False, True, True]

        self.assertListEqual(list(df.pd_df.result), expected_results)

    def test_greater_than_operator(self):
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
        df = df.withColumn('result', F.col('n1') > F.col('n2'))

        expected_results = [True, False, False, False, False, True, False]

        self.assertListEqual(list(df.pd_df.result), expected_results)

    def test_greater_or_equal_than_operator(self):
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
        df = df.withColumn('result', F.col('n1') >= F.col('n2'))

        expected_results = [True, True, False, False, True, True, False]

        self.assertListEqual(list(df.pd_df.result), expected_results)

    def test_when_otherwise_function(self):
        self.assertFalse(True) #TODO: testar

    def test_multiple_aggregations_produce_correct_results(self):
        
        observations = [
            ('2008', 'Coca-cola', 5000),
            ('2008', 'Pepsi', 4000),
            ('2008', 'Antartica', 6000),
            ('2008', 'Pureza', 3000),
            ('2009', 'Coca-cola', 6000),
            ('2009', 'Pepsi', 2000),
            ('2009', 'Antartica', 5000),
            ('2009', 'Pureza', 7000),
            ('2010', 'Coca-cola', 8000),
            ('2010', 'Pepsi', 4000),
            ('2010', 'Antartica', 6000),
            ('2010', 'Pureza', 4000),
        ]
        columns = ['year', 'company', 'earnings']

        df = self._createDataFrame(observations, columns)

        actual_df = df.groupby('year').agg(F.min('earnings'), F.max('earnings'))

        list_df = lambda df: df.pd_df.values.tolist()
        self.assertListEqual(list_df(actual_df), [[3000 , 6000], [2000, 7000], [4000, 8000]])
