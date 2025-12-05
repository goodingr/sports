import requests
import json

url = "http://localhost:8000/api/bets/history?limit=100&page=1"
try:
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    
    bets = data.get("data", [])
    print(f"Total bets in history: {data.get('total')}")
    
    target_game_id = "NBA_0022400744"
    found = False
    for bet in bets:
        if bet.get("game_id") == target_game_id:
            print(f"Found target game: {bet['home_team']} vs {bet['away_team']}")
            print(f"Status: {bet.get('status')}")
            print(f"Commence Time: {bet.get('commence_time')}")
            found = True
            break
            
    if not found:
        print(f"Target game {target_game_id} NOT found in history.")
        
except Exception as e:
    print(f"Error calling API: {e}")
