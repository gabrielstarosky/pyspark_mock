

from ._utils import _f_in_df

class Column:

    def __init__(self, column_name, rule_function):
        self.column_name = column_name
        self.rule_function = rule_function
    
    def alias(self, new_column_name):
        return Column(new_column_name, self.rule_function)
    
    def apply(self, df):
        return self.rule_function(df, self.column_name)
    
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
