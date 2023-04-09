
from pyspark_mock.sql import DataFrame

class Column:

    def __init__(self, column_name, rule_function):
        self.column_name = column_name
        self.rule_function = rule_function
    
    def alias(self, new_column_name):
        return Column(new_column_name, self.rule_function)
    
    def apply(self, df):
        return self.rule_function(df, self.column_name)
    
    def __add__(self, other):

        def eq_f_in_pandas(df, other_column : str):
            pd_df_copy = df.pd_df.copy()
            pd_df_copy[other_column] = pd_df_copy[self.column_name] + pd_df_copy[other.column_name]
            return DataFrame(pd_df_copy)
        
        return Column(f'{self.column_name} + {other.column_name}', eq_f_in_pandas)
    
    def __sub__(self, other):

        def eq_f_in_pandas(df, other_column : str):
            pd_df_copy = df.pd_df.copy()
            pd_df_copy[other_column] = pd_df_copy[self.column_name] - pd_df_copy[other.column_name]
            return DataFrame(pd_df_copy)
        
        return Column(f'{self.column_name} - {other.column_name}', eq_f_in_pandas)
    
    def __mul__(self, other):

        def eq_f_in_pandas(df, other_column : str):
            pd_df_copy = df.pd_df.copy()
            pd_df_copy[other_column] = pd_df_copy[self.column_name] * pd_df_copy[other.column_name]
            return DataFrame(pd_df_copy)
        
        return Column(f'{self.column_name} - {other.column_name}', eq_f_in_pandas)
    
    def __truediv__(self, other):

        def eq_f_in_pandas(df, other_column : str):
            pd_df_copy = df.pd_df.copy()
            pd_df_copy[other_column] = pd_df_copy[self.column_name] / pd_df_copy[other.column_name]
            return DataFrame(pd_df_copy)
        
        return Column(f'{self.column_name} / {other.column_name}', eq_f_in_pandas)
    
    def __lt__(self, other):

        def eq_f_in_pandas(df, other_column : str):
            pd_df_copy = df.pd_df.copy()
            pd_df_copy[other_column] = pd_df_copy[self.column_name] < pd_df_copy[other.column_name]
            return DataFrame(pd_df_copy)
        
        return Column(f'{self.column_name} < {other.column_name}', eq_f_in_pandas)
    
    def __le__(self, other):

        def eq_f_in_pandas(df, other_column : str):
            pd_df_copy = df.pd_df.copy()
            pd_df_copy[other_column] = pd_df_copy[self.column_name] <= pd_df_copy[other.column_name]
            return DataFrame(pd_df_copy)
        
        return Column(f'{self.column_name} <= {other.column_name}', eq_f_in_pandas)
    
    def __eq__(self, other):

        def eq_f_in_pandas(df, other_column : str):
            pd_df_copy = df.pd_df.copy()
            pd_df_copy[other_column] = pd_df_copy[self.column_name] == pd_df_copy[other.column_name]
            return DataFrame(pd_df_copy)
        
        return Column(f'{self.column_name} = {other.column_name}', eq_f_in_pandas)
