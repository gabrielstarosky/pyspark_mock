from typing import List

from pyspark_mock.sql import DataFrame, Column
from ._utils import _f_in_df

def convert_columns_to_pandas_columns(cols: List[str | Column], pd_df):
    return [pd_df[col] if isinstance(col, str) else col.apply(DataFrame(pd_df)).pd_df[col.column_name] for col in cols]

class PysparkMockFunctionWithColsAndParams:

    def __init__(self, label_lambda: callable, calculus: callable):
        self.label_lambda = label_lambda
        self.calculus = calculus

    def __call__(self, cols: List[str | Column], params: List[any]):
        return self._calculate_column_by_lambda(cols, params)

    def _calculate_column_by_lambda(self, cols: List[str | Column], params: List[any]): 

        calculation = lambda cols, params: (lambda pd_df: pd_df.apply(lambda x: self.calculus(*convert_columns_to_pandas_columns(cols, x), *params), axis=1))

        imp_f_in_df = _f_in_df(calculation(cols, params))
        return Column(self.label_lambda(*cols, *params), imp_f_in_df)

class PysparkMockFunctionWithCols(PysparkMockFunctionWithColsAndParams):

    def __call__(self, *cols: List[str | Column]):
        return self._calculate_column_by_lambda(cols, [])

class PysparkMockFunctionWithParams(PysparkMockFunctionWithColsAndParams):

    def __call__(self, *params: List[str | Column]):
        return self._calculate_column_by_lambda([], params)

