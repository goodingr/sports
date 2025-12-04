import requests
import json

try:
    response = requests.get("http://localhost:8000/api/bets/upcoming")
    data = response.json()
    
    print(f"Total bets: {data.get('count', 0)}")
    
    for bet in data.get('data', [])[:5]:
        print(f"Game ID: {bet.get('game_id')}")
        print(f"Matchup: {bet.get('away_team')} @ {bet.get('home_team')}")
        print("-" * 20)
        
except Exception as e:
    print(f"Error: {e}")
