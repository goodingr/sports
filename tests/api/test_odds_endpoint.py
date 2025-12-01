import requests
import json
import pandas as pd

game_id = "2824b0eee01ab1fc4155185a69980d39"
url = f"http://localhost:8000/api/bets/game/{game_id}/odds"

print(f"Testing URL: {url}")

try:
    response = requests.get(url)
    print(f"Status Code: {response.status_code}")
    
    if response.status_code == 200:
        data = response.json()
        print(f"Count: {data.get('count')}")
        records = data.get('data', [])
        if records:
            df = pd.DataFrame(records)
            print("\nColumns:", df.columns.tolist())
            print("\nSample Data:")
            print(df[['book', 'market', 'outcome', 'line', 'moneyline']].head(10))
        else:
            print("No data returned.")
    else:
        print(f"Error: {response.text}")

except Exception as e:
    print(f"Exception: {e}")
