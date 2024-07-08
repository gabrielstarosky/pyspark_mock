from typing import List
import pandas as pd



class DataFrame:

    def __init__(self, pd_df : pd.DataFrame):
        self.pd_df = pd_df

    def count(self):
        return len(self.pd_df.index)
    
    @property
    def columns(self):
        return list(self.pd_df.columns)
    
    def withColumn(self, column_name, col):
        return col.alias(column_name).apply(self)

    def groupby(self, cols: str | List[str] = None):
        from pyspark_mock.sql import GroupedData
        return GroupedData(cols, self)
