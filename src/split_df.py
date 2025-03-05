"""
Simple fuction so split measurementData csv files into smaller csv
files. Used for data collection progression mapping. The split files
are sorted chronologically, as the input measurementData files are 
as well.
"""

import pandas as pd
import numpy as np

df = pd.read_csv(r'data\private\app_test_data_3007\measurementData.csv',sep='\t',dtype=object)


# Split into x parts
x=7
split_dfs = np.array_split(df, 7)

# Each element in split_dfs is a DataFrame
for i, part in enumerate(split_dfs):
    print(f"Part {i+1}:\n", part, "\n")

count = 0
for df in split_dfs:
    count+=1
    df.to_csv(f'data/private/app_test_data_3007/split/measurementData{count}.csv',sep='\t')