# Welcome to Pyspark Mock
> A library that mocks pyspark by making operations on pandas dataframes instead of spark dataframes and without needing of open a spark session, making possible doing fast unit testing of `pyspark` codes.

## Demo

Let's write a function that takes a `dataframe` (object of type `pyspark.sql.dataframe`) and creates a column called `col3` with the sum of `col1` and `col2`.

```python
# sum_cols.py

from pyspark.sql import DataFrame

def sum_cols(df : DataFrame):
    return df.withColumn('col3', df.col1 + df.col2)
```

In this example, we are using native python unit test module, `unittest`. Without using `pyspark_mock` library, we must open a spark session and create a spark dataframe using this session. After running the test function, we must stop the spark session.

```python
# test_sum_cols.py
import unittest

from pyspark.sql import SparkSession

from sum_cols import sum_cols

class TestSum(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.Builder()\
            .master('local[*]')\
            .appName('unittest')\
            .getOrCreate()
            
    @classmethod
    def teardowmClass(cls):
        cls.spark.stop()
        
    def test_sum_cols(self):
        
        observations = [
            (0, 2, 2),
            (9, 2, 11),
            (2, 1, 3), 
            (2, 6, 8),
            (2, 2, 4) 
        ]

        columns = ["col1", "col2", "col3_expected"]
        df_input = self.spark.createDataFrame(observations, columns)
        df_output = sum_cols(df_input)
        
        # Test for each row if value produced by `sum_cols` function is as expected
        with self.subTest():
            for row in df_output.collect():
                self.assertEqual(row['col3'], row['col3_expected'], msg = row)
```

Running the test takes a time of **12.4 seconds!**

[comment]: <> (TODO: write log of `unittest` without using mocked spark dataframe)

If we use `pyspark_mock`, we do not use `setUpClass` and `teardownClass` because we do not need to open a spark session and we can create a `DataFrame` object from `pyspark_mock` that is initialized with a `pandas DataFrame`. This `pandas DataFrame` is what mocks `spark DataFrame` under the hood.

```python
# test_sum_cols.py
import unittest

import pandas as pd
from pyspark_mock.sql import DataFrame

from sum_cols import sum_cols

class TestSum(unittest.TestCase):
        
    def test_sum_cols(self):
        
        mapping_columns = {
            'col1' : [0, 9, 2, 2, 2]
            'col2' : [2, 2, 1, 6, 2]
        }
        pd_df_input = pd.DataFrame(mapping_columns)
        df_input = DataFrame(pd_df_input)
        df_output = sum_cols(df_input)
        
        # Pandas dataframe is an accessible property of the mocked spark dataframe
        self.assertEqual(list(df_output.pd_df.col3), [2, 11, 3, 8, 40)]
```

Running the tests with mocked spark dataframe takes only **0.01 seconds!**

[comment]: <> (TODO: write log of `unittest` with using mocked spark dataframe)


# Instalation

It's only to use `pip`

```bash
pip install pyspark_mock
```
