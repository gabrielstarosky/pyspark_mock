import math

from pyspark_mock.sql import DataFrame
from pyspark_mock.sql import Column

def lit(literal_value):
    
    def eq_f_in_pandas(df : DataFrame, column_name : str):
        pd_df_copy = df.pd_df.copy()
        pd_df_copy[column_name] = literal_value
        return DataFrame(pd_df_copy)

    return Column(str(literal_value), eq_f_in_pandas)

def col(column_name):

    def eq_f_in_pandas(df : DataFrame, other_column : str):
        pd_df_copy = df.pd_df.copy()
        pd_df_copy[other_column] = pd_df_copy[column_name]
        return DataFrame(pd_df_copy)
    
    return Column(column_name, eq_f_in_pandas)

def sqrt(column_name):

    def eq_f_in_pandas(df : DataFrame, other_column : str):
        pd_df_copy = df.pd_df.copy()
        pd_df_copy[other_column] = pd_df_copy[column_name].apply(lambda x : math.sqrt(x))
        return DataFrame(pd_df_copy)

    return Column(f'SQRT({column_name})', eq_f_in_pandas)

def abs(column_name):

    def eq_f_in_pandas(df : DataFrame, other_column : str):
        pd_df_copy = df.pd_df.copy()
        pd_df_copy[other_column] = pd_df_copy[column_name].apply(lambda x : x if x >= 0 else -x)
        return DataFrame(pd_df_copy)

    return Column(f'ABS({column_name})', eq_f_in_pandas)

def greatest(*cols):

    def eq_f_in_pandas(df : DataFrame, other_column : str):
        pd_df_copy = df.pd_df.copy()
        pd_df_copy[other_column] = pd_df_copy[list(cols)].max(axis=1)
        return DataFrame(pd_df_copy)
    
    str_list_of_cols = ','.join(cols)

    return Column(f'GREATEST({str_list_of_cols})', eq_f_in_pandas)