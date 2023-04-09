import math

from pyspark_mock.sql import DataFrame
from pyspark_mock.sql import Column

def lit(literal_value):
    
    def create_column_with_literal(df : DataFrame, column_name : str):
        pd_df_copy = df.pd_df.copy()
        pd_df_copy[column_name] = literal_value
        return DataFrame(pd_df_copy)

    return Column(str(literal_value), create_column_with_literal)

def col(column_name):

    def create_column_with_other_column(df : DataFrame, other_column : str):
        pd_df_copy = df.pd_df.copy()
        pd_df_copy[other_column] = pd_df_copy[column_name]
        return DataFrame(pd_df_copy)
    
    return Column(column_name, create_column_with_other_column)

def sqrt(column_name):

    def create_column_with_sqrt(df : DataFrame, other_column : str):
        pd_df_copy = df.pd_df.copy()
        pd_df_copy[other_column] = pd_df_copy[column_name].apply(lambda x : math.sqrt(x))
        return DataFrame(pd_df_copy)

    return Column(f'SQRT({column_name})', create_column_with_sqrt)

def abs(column_name):

    def create_column_with_sqrt(df : DataFrame, other_column : str):
        pd_df_copy = df.pd_df.copy()
        pd_df_copy[other_column] = pd_df_copy[column_name].apply(lambda x : x if x >= 0 else -x)
        return DataFrame(pd_df_copy)

    return Column(f'ABS({column_name})', create_column_with_sqrt)

def greatest(*cols):

    def create_column_with_greatest(df : DataFrame, other_column : str):
        pd_df_copy = df.pd_df.copy()
        pd_df_copy[other_column] = pd_df_copy[list(cols)].max(axis=1)
        return DataFrame(pd_df_copy)
    
    str_list_of_cols = ','.join(cols)

    return Column(f'GREATEST({str_list_of_cols})', create_column_with_greatest)