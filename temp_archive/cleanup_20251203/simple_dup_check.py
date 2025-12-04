import requests

response = requests.get("http://localhost:8000/api/bets/upcoming")
data = response.json()

all_games = data.get('data', [])

# Find Cremonese/Bologna games
target_games = [bet for bet in all_games if ('Cremonese' in str(bet.get('home_team', '')) or
                                               'Cremonese' in str(bet.get('away_team', ''))) and
                                              ('Bologna' in str(bet.get('home_team', '')) or
                                               'Bologna' in str(bet.get('away_team', '')))]

print(f"Found {len(target_games)} Cremonese vs Bologna games\n")

for i, game in enumerate(target_games, 1):
    print(f"{i}. game_id: {game.get('game_id')}")
    print(f"   {game.get('away_team')} @ {game.get('home_team')}")
    print(f"   Prediction: {game.get('prediction')}")
    print(f"   League: {game.get('league')}")
    print()

# Show unique game IDs
game_ids = [g.get('game_id') for g in target_games]
unique_ids = set(game_ids)
print(f"Total game records: {len(game_ids)}")
print(f"Unique game_ids: {len(unique_ids)}")

if len(game_ids) > len(unique_ids):
    from collections import Counter
    counts = Counter(game_ids)
    print("\n⚠️  DUPLICATES FOUND:")
    for gid, count in counts.items():
        if count > 1:
            print(f"  {gid}: appears {count} times")
