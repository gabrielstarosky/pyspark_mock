import math
from typing import List

from pyspark_mock.sql import Column, AggregatedColumn

from ._utils import _f_in_df

def lit(literal_value):
    imp_f_in_df = _f_in_df(lambda pd_df : literal_value)
    return Column(str(literal_value), imp_f_in_df)

def col(column_name):
    imp_f_in_df = _f_in_df(lambda pd_df : pd_df[column_name])
    return Column(column_name, imp_f_in_df)

def sqrt(column_name):
    imp_f_in_df = _f_in_df(lambda pd_df : pd_df[column_name].apply(lambda x : math.sqrt(x)))
    return Column(f'SQRT({column_name})', imp_f_in_df)

def abs(column_name):
    imp_f_in_df = _f_in_df(lambda pd_df : pd_df[column_name].apply(lambda x : x if x >= 0 else -x))
    return Column(f'ABS({column_name})', imp_f_in_df)

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
