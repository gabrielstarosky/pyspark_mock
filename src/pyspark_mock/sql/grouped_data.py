from typing import List

import pandas as pd

from pyspark_mock.sql import DataFrame
from pyspark_mock.sql.column import AggregatedColumn

def _mocked_dataframe(f):

    def wrap(*args, **kwargs) -> DataFrame:

        return DataFrame(f(*args, **kwargs))

    return wrap

class GroupedData:

    def __init__(self, cols: str | List[str], df: DataFrame):
        self.df = df
        self.pd_df = self.df.pd_df

        if isinstance(cols, str):
            self.cols = [cols]
        else:
            self.cols = cols

    @_mocked_dataframe
    def agg(self, *aggregations: List[AggregatedColumn]) -> pd.DataFrame:
        copied_pd_df = self.pd_df.copy()

        return copied_pd_df.groupby(self.cols).agg(
           **dict([(ag.col_name, (ag.value, ag.aggregation)) for ag in aggregations]) 
        )

    @_mocked_dataframe
    def count(self) -> pd.DataFrame:
        
        copied_pd_df = self.pd_df.copy()
        copied_pd_df['dummy'] = 1

        if self.cols is not None:
            return copied_pd_df[[*self.cols, 'dummy']].groupby(self.cols).count()
        else:
            return copied_pd_df[['dummy']].count()
    
    
    @_mocked_dataframe
    def min(self, col: str):

        if self.cols is not None:
            return self.pd_df.groupby(self.cols).min(col)
        else:

            copied_pd_df = self.pd_df.copy()
            copied_pd_df['dummy'] = 1
            return copied_pd_df.groupby(['dummy']).min(col)


    @_mocked_dataframe
    def max(self, col: str):

        if self.cols is not None:
            return self.pd_df.groupby(self.cols).max(col)
        else:

            copied_pd_df = self.pd_df.copy()
            copied_pd_df['dummy'] = 1
            return copied_pd_df.groupby(['dummy']).max(col)

