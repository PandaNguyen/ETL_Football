import pandas as pd
import numpy as np
import os
from pathlib import Path


df=pd.read_csv(r"h:\ETL_FOOTBALL\data_raw\team_point.csv")
print(df.dtypes)
print(df["Rank"].dtype)
