import requests
import pandas as pd

# Check API response
print("=" * 80)
print("API RESPONSE CHECK")
print("=" * 80)
response = requests.get("http://localhost:8000/api/bets/upcoming")
data = response.json()

# Filter for Cremonese games
cremonese_games = [bet for bet in data.get('data', []) if 'Cremonese' in bet.get('home_team', '') or 'Cremonese' in bet.get('away_team', '')]

print(f"\nTotal Cremonese games in API: {len(cremonese_games)}\n")
for i, game in enumerate(cremonese_games, 1):
    print(f"Game {i}:")
    print(f"  game_id: {game.get('game_id')}")
    print(f"  Matchup: {game.get('away_team')} @ {game.get('home_team')}")
    print(f"  League: {game.get('league')}")
    print(f"  Prediction: {game.get('prediction')}")
    print(f"  Line: {game.get('line')}")
    print()

# Check parquet file
print("=" * 80)
print("PARQUET FILE CHECK")
print("=" * 80)
df = pd.read_parquet('data/forward_test/predictions_master.parquet')

print(f"\nColumns: {df.columns.tolist()}\n")

# Filter for Cremonese games
cremonese_df = df[(df['home_team'].str.contains('Cremonese', na=False)) | 
                   (df['away_team'].str.contains('Cremonese', na=False))]

print(f"\nTotal Cremonese predictions in parquet: {len(cremonese_df)}\n")
if len(cremonese_df) > 0:
    print(cremonese_df[['game_id', 'home_team', 'away_team', 'league', 'over_edge', 'under_edge']].to_string())

# Check for duplicate game_ids
print("\n" + "=" * 80)
print("DUPLICATE CHECK")
print("=" * 80)
duplicates = cremonese_df[cremonese_df.duplicated(subset=['game_id'], keep=False)]
if len(duplicates) > 0:
    print(f"\n⚠️  Found {len(duplicates)} duplicate game_id records!")
    print(duplicates[['game_id', 'home_team', 'away_team', 'over_edge', 'under_edge']].to_string())
else:
    print("\n✓ No duplicate game_ids found in parquet file")
