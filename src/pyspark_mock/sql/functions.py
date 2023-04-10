import math

from pyspark_mock.sql import Column
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

