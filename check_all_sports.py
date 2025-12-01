import requests
import json
from collections import Counter

def check_all_sports():
    try:
        response = requests.get("http://localhost:8000/api/bets/upcoming")
        if response.status_code == 200:
            data = response.json()
            bets = data.get("data", [])
            
            print(f"Total upcoming bets: {len(bets)}")
            
            league_counts = Counter(b.get("league") for b in bets)
            print("\nBets by League:")
            for league, count in league_counts.items():
                print(f"  - {league}: {count}")
                
            if not bets:
                print("\nNo bets found in API response.")
        else:
            print(f"API failed with status {response.status_code}")
    except Exception as e:
        print(f"API request failed: {e}")

if __name__ == "__main__":
    check_all_sports()
