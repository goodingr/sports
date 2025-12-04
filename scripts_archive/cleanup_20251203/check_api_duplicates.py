import requests
import json

# Check API response
print("Fetching API response...")
response = requests.get("http://localhost:8000/api/bets/upcoming")
data = response.json()

all_games = data.get('data', [])
print(f"\nTotal games in API: {len(all_games)}")

# Filter for Cremonese or Bologna
target_games = [bet for bet in all_games if 'Cremonese' in str(bet.get('home_team', '')) + str(bet.get('away_team', '')) or 
                                             'Bologna' in str(bet.get('home_team', '')) + str(bet.get('away_team', ''))]

print(f"\nCremonese/Bologna games found: {len(target_games)}\n")

for i, game in enumerate(target_games, 1):
    print(f"=== Game {i} ===")
    print(json.dumps(game, indent=2))
    print()

# Check for exact duplicates
if len(target_games) >= 2:
    print("=" * 80)
    print("CHECKING FOR DUPLICATE GAME_IDS")
    print("=" * 80)
    game_ids = [g.get('game_id') for g in target_games]
    print(f"Game IDs: {game_ids}")
    
    if len(game_ids) != len(set(game_ids)):
        print("\n⚠️  FOUND DUPLICATE GAME_IDS!")
        from collections import Counter
        duplicates = [id for id, count in Counter(game_ids).items() if count > 1]
        print(f"Duplicate game_ids: {duplicates}")
