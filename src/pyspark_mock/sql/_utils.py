
from pyspark_mock.sql import DataFrame

def _f_in_df(func):

    def wrapper(df, other_column, *args, **kwargs):
        pd_df_copy = df.pd_df.copy()
        pd_df_copy[other_column] = func(pd_df_copy, *args, **kwargs)
        return DataFrame(pd_df_copy)
    
    return wrapper