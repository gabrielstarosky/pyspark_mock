import math
from typing import List

import numpy as np
from pyspark_mock.sql import Column 
from pyspark_mock.sql.column import AggregatedColumn, PureWindowColumn
from pyspark_mock.sql import DataFrame

from .callable_pyspark_functions import PysparkMockFunctionWithColsAndParams
from .callable_pyspark_functions import PysparkMockFunctionWithCols
from .callable_pyspark_functions import PysparkMockFunctionWithParams

from ._utils import _f_in_df

lit = PysparkMockFunctionWithParams(lambda x: x, lambda x: x)
col = PysparkMockFunctionWithCols(lambda x: x, lambda x: x)
sqrt = PysparkMockFunctionWithCols(lambda x: f'SQRT({x})', lambda x: math.sqrt(x))
abs = PysparkMockFunctionWithCols(lambda x: f'ABS({x})', lambda x: x if x >= 0 else -x)
when = PysparkMockFunctionWithCols(lambda x, y: f'WHEN({x})', lambda x, y: y if x else None)
sin = PysparkMockFunctionWithCols(lambda x: f'SIN({x})', lambda x: math.sin(x))

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

