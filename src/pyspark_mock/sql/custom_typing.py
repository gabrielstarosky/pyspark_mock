from typing import List


from pyspark.sql import Column


strings_or_columns = str | Column | List[str] | List[Column]
