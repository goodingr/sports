import requests

response = requests.get("http://localhost:8000/api/bets/upcoming")
data = response.json()
bets = data.get('data', [])

# Find all Serie A games
seriea = [b for b in bets if b.get('league') == 'SERIEA']

print(f"Total Serie A games: {len(seriea)}")

# Find duplicates
from collections import Counter
game_tuples = [(b['home_team'], b['away_team'], b['commence_time']) for b in seriea]
counts = Counter(game_tuples)

duplicates = [(game, count) for game, count in counts.items() if count > 1]

print(f"\nDuplicates found: {len(duplicates)}\n")

for (home, away, time), count in duplicates:
    print(f"{away} @ {home} ({time})")
    print(f"  Appears {count} times")
    
    # Find the actual records
    matching = [b for b in seriea 
               if b['home_team'] == home 
               and b['away_team'] == away
               and b['commence_time'] == time]
    
    for m in matching:
        print(f"    - game_id: {m['game_id']}")
    print()
