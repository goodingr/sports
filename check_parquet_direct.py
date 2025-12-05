import pandas as pd

df = pd.read_parquet('data/forward_test/ensemble/predictions_master.parquet')
gb = df[(df['home_team'].str.contains('Green Bay Phoenix', case=False, na=False)) | 
        (df['away_team'].str.contains('Green Bay Phoenix', case=False, na=False))]

print(f'Result: {gb.iloc[0]["result"]}')
print(f'Home Score: {gb.iloc[0]["home_score"]}')
print(f'Away Score: {gb.iloc[0]["away_score"]}')
print(f'File modification time: {pd.Timestamp.fromtimestamp(pd.io.common.file_path_to_url("data/forward_test/ensemble/predictions_master.parquet"))}')

import os
stat = os.stat('data/forward_test/ensemble/predictions_master.parquet')
print(f'Last modified: {pd.Timestamp.fromtimestamp(stat.st_mtime)}')
