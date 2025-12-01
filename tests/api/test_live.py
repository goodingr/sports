import requests
import json

game_id = "2824b0eee01ab1fc4155185a69980d39"
url = f"http://localhost:8000/api/bets/game/{game_id}/odds"

try:
    print(f"Fetching from {url}...")
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        records = data.get('data', [])
        if records:
            print("First record keys:", records[0].keys())
            print("First record sample:", records[0])
            
            has_url = any('book_url' in r for r in records)
            print(f"Has 'book_url' in any record: {has_url}")
        else:
            print("No records returned.")
    else:
        print(f"Error: {response.status_code} - {response.text}")
except Exception as e:
    print(f"Exception: {e}")
