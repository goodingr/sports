import json
from pathlib import Path
from datetime import datetime, timezone

def check_game_times():
    now = datetime.now(timezone.utc)
    print(f"Current UTC time: {now}")
    print(f"Current EST time: {now.astimezone()}\n")
    
    # Check NBA games
    print("=== NBA Games in Raw Odds ===")
    nba_file = sorted(Path("data/raw/odds/basketball_nba").glob("*.json"))[-1]
    print(f"File: {nba_file.name}\n")
    
    with open(nba_file, "r") as f:
        nba_data = json.load(f)
    
    nba_results = nba_data if isinstance(nba_data, list) else nba_data.get("results", [])
    
    for game in nba_results:
        commence_time = game.get("commence_time", "")
        try:
            game_time = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
            time_diff = (game_time - now).total_seconds() / 3600  # hours
            status = "PAST" if time_diff < 0 else "UPCOMING"
            print(f"{status}: {game.get('away_team')} @ {game.get('home_team')}")
            print(f"  Time: {commence_time} ({time_diff:.1f} hours from now)")
        except:
            print(f"ERROR parsing: {game.get('away_team')} @ {game.get('home_team')}")
    
    # Check NHL games
    print("\n=== NHL Games in Raw Odds ===")
    nhl_file = sorted(Path("data/raw/odds/icehockey_nhl").glob("*.json"))[-1]
    print(f"File: {nhl_file.name}\n")
    
    with open(nhl_file, "r") as f:
        nhl_data = json.load(f)
    
    nhl_results = nhl_data if isinstance(nhl_data, list) else nhl_data.get("results", [])
    
    for game in nhl_results:
        commence_time = game.get("commence_time", "")
        try:
            game_time = datetime.fromisoformat(commence_time.replace("Z", "+00:00"))
            time_diff = (game_time - now).total_seconds() / 3600  # hours
            status = "PAST" if time_diff < 0 else "UPCOMING"
            print(f"{status}: {game.get('away_team')} @ {game.get('home_team')}")
            print(f"  Time: {commence_time} ({time_diff:.1f} hours from now)")
        except:
            print(f"ERROR parsing: {game.get('away_team')} @ {game.get('home_team')}")

if __name__ == "__main__":
    check_game_times()
