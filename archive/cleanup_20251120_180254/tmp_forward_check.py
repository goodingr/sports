import pandas as pd
preds = pd.read_parquet('data/forward_test/predictions_master.parquet')
settled = pd.read_parquet('data/forward_test/settled_bets.parquet')
merged = preds.merge(settled[['bet_id','settled_flag']], on='bet_id', how='left')
discrepancy = merged[(merged['settled_flag'] == True) & (merged['prediction_status'] != 'completed')]
print(discrepancy[['bet_id','game_id','team','stake','settled_flag','prediction_status']])
