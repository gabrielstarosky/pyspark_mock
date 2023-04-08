import pandas as pd

class DataFrame:

    def __init__(self, pd_df : pd.DataFrame):
        self.pd_df = pd_df