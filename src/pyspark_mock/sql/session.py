import pandas as pd

from .data_frame import DataFrame

__all__ = ['SparkSession']

class SparkSession:

    class Builder:

        def __init__(self):
            self.confs = {}

        def conf(self, key, value):
            self.confs[key] = value
            return self

        def getOrCreate(self):
            spark_session = SparkSession(self.confs)
            return spark_session
    
    builder = Builder()

    def __init__(self, confs):
        self.confs = confs

    def createDataFrame(self, observations, columns):

        pd_df = pd.DataFrame(observations, columns=columns)

        return DataFrame(pd_df)

