import math
from typing import List

import numpy as np
from pyspark_mock.sql import Column 
from pyspark_mock.sql.column import AggregatedColumn, PureWindowColumn
from pyspark_mock.sql import DataFrame


from ._utils import _f_in_df


def convert_columns_to_pandas_columns(cols: List[str | Column], pd_df):
    return [pd_df[col] if isinstance(col, str) else col.apply(DataFrame(pd_df)).pd_df[col.column_name] for col in cols]

class PysparkMockFunctionWithColsAndParams:

    def __init__(self, label_lambda: callable, calculus: callable):
        self.label_lambda = label_lambda
        self.calculus = calculus

    def __call__(self, cols: List[str | Column], params: List[any]):
        return self._calculate_column_by_lambda(cols, params)

    def _calculate_column_by_lambda(self, cols: List[str | Column], params: List[any]): 

        calculation = lambda cols, params: (lambda pd_df: pd_df.apply(lambda x: self.calculus(*convert_columns_to_pandas_columns(cols, x), *params), axis=1))

        imp_f_in_df = _f_in_df(calculation(cols, params))
        return Column(self.label_lambda(*cols, *params), imp_f_in_df)

class PysparkMockFunctionWithCols(PysparkMockFunctionWithColsAndParams):

    def __call__(self, *cols: List[str | Column]):
        return self._calculate_column_by_lambda(cols, [])

class PysparkMockFunctionWithParams(PysparkMockFunctionWithColsAndParams):

    def __call__(self, *params: List[str | Column]):
        return self._calculate_column_by_lambda([], params)

def lit(literal_value):
    imp_f_in_df = _f_in_df(lambda pd_df : literal_value)
    return Column(str(literal_value), imp_f_in_df)

def col(column_name):
    imp_f_in_df = _f_in_df(lambda pd_df : pd_df[column_name])
    return Column(column_name, imp_f_in_df)

lit = PysparkMockFunctionWithParams(lambda x: x, lambda x: x)
col = PysparkMockFunctionWithCols(lambda x: x, lambda x: x)
sqrt = PysparkMockFunctionWithCols(lambda x: f'SQRT({x})', lambda x: math.sqrt(x))
abs = PysparkMockFunctionWithCols(lambda x: f'ABS({x})', lambda x: x if x >= 0 else -x)

def _max(*x):

    x = [i for i in x if not np.isnan(i)] 

    value_max = x[0]

    if len(x) == 1:
        return value_max
    else:
        recursive_max = _max(*x[1:])
        if value_max > recursive_max:
            return value_max
        else:
            return recursive_max

greatest = PysparkMockFunctionWithCols(lambda *x: f'GREATEST({",".join(x)})', lambda *x: _max(*x))

when = PysparkMockFunctionWithCols(lambda x, y: f'WHEN({x})', lambda x, y: y if x else None)

sin = PysparkMockFunctionWithCols(lambda x: f'SIN({x})', lambda x: math.sin(x))

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

