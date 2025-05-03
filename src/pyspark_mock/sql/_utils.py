import pandas as pd
from pyspark_mock.sql import DataFrame
 
from typing import Callable, Any, TypeVar

PandasSeriesType = TypeVar('PandasSeriesType', bound=pd.Series)
PandasDataFrameType = TypeVar('PandasDataFrameType', bound=pd.DataFrame)

def f_in_df(func: Callable[[PandasDataFrameType, Any, Any], PandasDataFrameType]) -> Callable[[DataFrame, str, Any, Any], DataFrame]:

    def wrapper(df: DataFrame, other_column: str, *args: Any, **kwargs: Any):
        pd_df_copy: PandasDataFrameType = df.pd_df.copy()
        pd_df_copy[other_column] = func(pd_df_copy, *args, **kwargs)
        return DataFrame(pd_df_copy)
    
    return wrapper
