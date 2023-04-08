import sys
sys.path.append('src')
import unittest

import pandas as pd

from pyspark_mock.sql import DataFrame as MockedDataFrame


class TestDataFrame(unittest.TestCase):

    def test_count_method(self):

        mocked_spark_session = (MockedSparkSession
            .builder
            .conf("spark.driver.cores", "4")
            .conf("spark.executors.cores", "4")
            .conf("spark.ui.killEnabled", "False")
            .getOrCreate()
        )

        expected_confs = {
            "spark.driver.cores" : "4",
            "spark.executors.cores" : "4",
            "spark.ui.killEnabled" : "False",
        }

        self.assertDictEqual(mocked_spark_session.confs, expected_confs)

    def test_data_frame_creation_returns_data_frame_object(self):

        observations = [
            ('A', 1),
            ('B', 2),
            ('C', 3)
        ]
        columns = ['col1', 'col2']

        df = self.spark.createDataFrame(observations, columns)

        self.assertIsInstance(df, MockedDataFrame)