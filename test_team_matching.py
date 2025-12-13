import pandas as pd
from src.dashboard.data import _match_games_to_db

# Create a fake recommended dataframe that simulates the Ohio State game
test_df = pd.DataFrame([{
    "game_id": "CFB_401752897",
    "league": "CFB",
    "home_team": "Purdue Boilermakers",
    "away_team": "Ohio State Buckeyes",
    "commence_time": pd.Timestamp("2024-12-07 12:00:00", tz="UTC"),
    "side": "over",
    "edge": 0.10,
}])

print("Testing _match_games_to_db with Purdue vs Ohio State...")
print(f"Input game_id: {test_df.iloc[0]['game_id']}")
print(f"Teams: {test_df.iloc[0]['home_team']} vs {test_df.iloc[0]['away_team']}")
print()

result = _match_games_to_db(test_df)

if not result.empty:
    print("SUCCESS! Mapping found:")
    print(result)
else:
    print("FAILURE: No mapping found")
    print("\nDebugging - let's try step by step...")
    
    from src.data.team_mappings import normalize_team_code, get_full_team_name
    from src.db.core import connect
    
    # Step 1: Get sport_id
    with connect() as conn:
        sport_row = conn.execute("SELECT sport_id FROM sports WHERE UPPER(league) = 'CFB'").fetchone()
        sport_id = sport_row[0] if sport_row else None
        print(f"1. Sport ID for CFB: {sport_id}")
        
        if sport_id:
            # Step 2: Normalize team names
            home_code = normalize_team_code("CFB", "Purdue Boilermakers")
            away_code = normalize_team_code("CFB", "Ohio State Buckeyes")
            print(f"2. Normalized codes: home={home_code}, away={away_code}")
            
            # Step 3: Look up team IDs
            home_row = conn.execute("SELECT team_id FROM teams WHERE sport_id = ? AND code = ?", (sport_id, home_code.upper())).fetchone()
            away_row = conn.execute("SELECT team_id FROM teams WHERE sport_id = ? AND code = ?", (sport_id, away_code.upper())).fetchone()
            home_team_id = home_row[0] if home_row else None
            away_team_id = away_row[0] if away_row else None
            print(f"3. Team IDs: home={home_team_id}, away={away_team_id}")
            
            if home_team_id and away_team_id:
                # Step 4: Look for matching game
                game_row = conn.execute("""
                    SELECT game_id, start_time_utc
                    FROM games
                    WHERE sport_id = ?
                      AND home_team_id = ?
                      AND away_team_id = ?
                    LIMIT 5
                """, (sport_id, home_team_id, away_team_id)).fetchall()
                print(f"4. Matching games found: {len(game_row)}")
                for row in game_row:
                    print(f"   - {row[0]}: {row[1]}")
