

from pyspark_mock.sql.window import Window
from ._utils import _f_in_df

class Column:

    def __init__(self, column_name, rule_function, ascending=True):
        self.column_name = column_name
        self.rule_function = rule_function
        self.ascending = ascending
    
    def alias(self, new_column_name):
        return Column(new_column_name, self.rule_function)
    
    def apply(self, df):
        return self.rule_function(df, self.column_name)
    
    def __str__(self):
        return self.column_name

    def asc(self):
        return Column(self.column_name, self.rule_function, True)

    def desc(self):
        return Column(self.column_name, self.rule_function, False)
    
    def __add__(self, other):
        imp_f_in_df = _f_in_df(lambda pd_df : pd_df[self.column_name] + pd_df[other.column_name])
        return Column(f'{self.column_name} + {other.column_name}', imp_f_in_df)
    
    def __sub__(self, other):
        imp_f_in_df = _f_in_df(lambda pd_df : pd_df[self.column_name] - pd_df[other.column_name])
        return Column(f'{self.column_name} - {other.column_name}', imp_f_in_df)
    
    def __mul__(self, other):
        imp_f_in_df = _f_in_df(lambda pd_df : pd_df[self.column_name] * pd_df[other.column_name])
        return Column(f'{self.column_name} - {other.column_name}', imp_f_in_df)
    
    def __truediv__(self, other):
        imp_f_in_df = _f_in_df(lambda pd_df : pd_df[self.column_name] / pd_df[other.column_name])
        return Column(f'{self.column_name} / {other.column_name}', imp_f_in_df)
    
    def __lt__(self, other):
        imp_f_in_df = _f_in_df(lambda pd_df : pd_df[self.column_name] < pd_df[other.column_name])
        return Column(f'{self.column_name} < {other.column_name}', imp_f_in_df)
    
    def __le__(self, other):
        imp_f_in_df = _f_in_df(lambda pd_df : pd_df[self.column_name] <= pd_df[other.column_name])
        return Column(f'{self.column_name} <= {other.column_name}', imp_f_in_df)
    
    def __eq__(self, other):
        imp_f_in_df = _f_in_df(lambda pd_df : pd_df[self.column_name] == pd_df[other.column_name])
        return Column(f'{self.column_name} = {other.column_name}', imp_f_in_df)
    
    def __ne__(self, other):
        imp_f_in_df = _f_in_df(lambda pd_df : pd_df[self.column_name] != pd_df[other.column_name])
        return Column(f'{self.column_name} != {other.column_name}', imp_f_in_df)

    def __gt__(self, other):
        imp_f_in_df = _f_in_df(lambda pd_df : pd_df[self.column_name] > pd_df[other.column_name])
        return Column(f'{self.column_name} > {other.column_name}', imp_f_in_df)

    def __ge__(self, other):
        imp_f_in_df = _f_in_df(lambda pd_df : pd_df[self.column_name] >= pd_df[other.column_name])
        return Column(f'{self.column_name} > {other.column_name}', imp_f_in_df)


class AggregatedColumn:


    def __init__(self, col_name: str, value: str, aggregation: str, windowing_function):
        self.col_name = col_name
        self.value = value
        self.aggregation = aggregation
        self.windowing_function = windowing_function

    def alias(self, new_col_name):
        return AggregatedColumn(new_col_name, self.value, self.aggregation, self.windowing_function)

    def over(self, window: Window):
        imp_f_in_df = _f_in_df(lambda pd_df: self.windowing_function(pd_df, window._partitionBy, window._orderBy)) 
        return Column(self.col_name, imp_f_in_df) 

class PureWindowColumn:

    def __init__(self, col_name, windowing_function):
        self.col_name = col_name
        self.windowing_function = windowing_function

    def alias(self, new_col_name):
        return PureWindowColumn(new_col_name, self.windowing_function)

    def over(self, window: Window):
        imp_f_in_df = _f_in_df(lambda pd_df: self.windowing_function(pd_df, window._partitionBy, window._orderBy)) 
        return Column(self.col_name, imp_f_in_df) 
