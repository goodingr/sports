from nba_api.stats.endpoints import leaguegamelog
import pandas as pd

try:
    endpoint = leaguegamelog.LeagueGameLog(season="2023-24", season_type_all_star="Regular Season")
    df = endpoint.get_data_frames()[0]
    print(df.columns.tolist())
    print(df.head(1).T)
except Exception as e:
    print(e)
