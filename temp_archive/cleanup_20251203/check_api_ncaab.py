import requests
import json

def check_api_ncaab():
    try:
        response = requests.get("http://localhost:8000/api/bets/upcoming")
        if response.status_code == 200:
            data = response.json()
            bets = data.get("data", [])
            ncaab_bets = [b for b in bets if b.get("league") == "NCAAB"]
            
            print(f"Total upcoming bets: {len(bets)}")
            print(f"Total NCAAB bets: {len(ncaab_bets)}")
            
            if ncaab_bets:
                print("\nNCAAB Bets found in API:")
                for bet in ncaab_bets[:10]:  # Limit to 10
                    print(f"  - {bet.get('home_team')} vs {bet.get('away_team')} ({bet.get('description')}) Edge: {bet.get('edge')}")
            else:
                print("\nNo NCAAB bets found in API response.")
        else:
            print(f"API failed with status {response.status_code}")
    except Exception as e:
        print(f"API request failed: {e}")

if __name__ == "__main__":
    check_api_ncaab()
