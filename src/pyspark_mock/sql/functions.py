import math
from typing import List

from pyspark_mock.sql import Column 
from pyspark_mock.sql.column import AggregatedColumn, PureWindowColumn
from pyspark_mock.sql import DataFrame


from ._utils import _f_in_df


class PysparkMockFunction1:

    def __init__(self, label_lambda: callable, calculus: callable):
        self.label_lambda = label_lambda
        self.calculus = calculus

    def __call__(self, column: str | Column):
        return self._calculate_column_by_lambda(column)

    def _calculate_column_by_lambda(self, column: str | Column): 

        calculation = lambda col : (lambda pd_df: pd_df[col].apply(self.calculus))
        
        if isinstance(column, str):
            imp_f_in_df = _f_in_df(calculation(column))
            return Column(self.label_lambda(column), imp_f_in_df)

        final_column_name = self.label_lambda(column.column_name)

        def calculate_column_by_intermediate(pd_df):
            pd_df['intermediate_column'] = column.alias('intermediate_column').apply(DataFrame(pd_df)).pd_df['intermediate_column']
            pd_df[final_column_name] = calculation('intermediate_column')(pd_df)
            return pd_df[final_column_name]

        return Column(final_column_name, _f_in_df(calculate_column_by_intermediate))

def lit(literal_value):
    imp_f_in_df = _f_in_df(lambda pd_df : literal_value)
    return Column(str(literal_value), imp_f_in_df)

def col(column_name):
    imp_f_in_df = _f_in_df(lambda pd_df : pd_df[column_name])
    return Column(column_name, imp_f_in_df)

sqrt = PysparkMockFunction1(lambda x: f'SQRT({x})', lambda x: math.sqrt(x))
abs = PysparkMockFunction1(lambda x: f'ABS({x})', lambda x: x if x >= 0 else -x)

def greatest(*cols):

    imp_f_in_df = _f_in_df(lambda pd_df : pd_df[list(cols)].max(axis=1))
   
    str_list_of_cols = ','.join(cols)
    return Column(f'GREATEST({str_list_of_cols})', imp_f_in_df)

def when(col, value_when_true):

    imp_f_in_df = _f_in_df(lambda pd_df : pd_df[col].apply(lambda x : value_when_true if x else None))
    return Column(f'WHEN({col})', imp_f_in_df)


def min(col):

    def _windowing_function(pd_df, partition_by: str | List[str]=None, order_by: str | List[str] = None):

        pd_df_copy = pd_df.copy()
        if partition_by is None:
            pd_df['dummy'] = 1
            return pd_df_copy.groupby('dummy')[col].transform(lambda x: x.min())

        return pd_df_copy.groupby(partition_by)[col].transform(lambda x: x.min())

    return AggregatedColumn(f'min({col})', col, 'min', _windowing_function)

def max(col):

    def _windowing_function(pd_df, partition_by: str | List[str]=None, order_by: str | List[str] = None):

        pd_df_copy = pd_df.copy()
        if partition_by is None:
            pd_df_copy['dummy'] = 1
            return pd_df_copy.groupby('dummy')[col].transform(lambda x: x.max())

        return pd_df_copy.groupby(partition_by)[col].transform(lambda x: x.max())

    return AggregatedColumn(f'max({col})', col, 'max', _windowing_function)

def row_number():
    
    def _windowing_function(pd_df, partition_by: str | List[str]=None, order_by: str | List[str] = None):

        pd_df_copy = pd_df.copy()
        if partition_by is None:
            pd_df_copy['dummy'] = 1
            return pd_df_copy.groupby(partition_by)['dummy'].transform(lambda x: x.rank())

        return pd_df_copy.groupby(partition_by)[order_by].transform(lambda x: x.rank())
    

    return PureWindowColumn(f'row_number()',  _windowing_function)

