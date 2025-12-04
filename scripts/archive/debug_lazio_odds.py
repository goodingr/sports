import pandas as pd
import sys
import os
from pathlib import Path

# Add project root to path
project_root = Path(__file__).resolve().parents[1]
sys.path.append(str(project_root))

from src.api.routes.bets import get_totals_data
from src.dashboard.data import get_totals_odds_for_recommended
from src.db.core import connect

def debug_lazio_odds():
    print("Fetching totals data...")
    df = get_totals_data()
    
    # Filter for Lazio vs Lecce
    # Note: Team names might be normalized, so checking for substring or specific ID if known
    # Based on previous debug: Lazio (395) vs Lecce (380)
    # Game ID from previous debug: SERIEA_736901
    
    lazio_games = df[
        (df['home_team'].str.contains('Lazio', case=False, na=False)) | 
        (df['away_team'].str.contains('Lazio', case=False, na=False))
    ]
    
    if lazio_games.empty:
        print("❌ No Lazio games found in predictions!")
        return

    print(f"\nFound {len(lazio_games)} Lazio games in predictions.")
    
    for _, row in lazio_games.iterrows():
        print(f"\n--- Game: {row['home_team']} vs {row['away_team']} ---")
        print(f"Prediction Game ID: {row['game_id']}")
        print(f"Side: {row['side']}")
        print(f"Predicted Line: {row['total_line']}")
        print(f"Displayed Odds (Moneyline): {row['moneyline']}")
        print(f"Displayed Book: {row['book']}")
        print(f"Displayed Book URL: {row['book_url']}")
        
        # Now let's check what the DB has for this game
        # We need to manually call the matching logic to see what's happening under the hood
        # Create a mini dataframe for just this row to pass to get_totals_odds_for_recommended
        mini_df = pd.DataFrame([row])
        
        print("\nChecking DB matching for this game...")
        odds_df = get_totals_odds_for_recommended(mini_df)
        
        if odds_df.empty:
            print("❌ No matching odds found in DB for this game!")
            
            # Manually query DB to see if we can find it
            with connect() as conn:
                # Look for Lazio team ID
                lazio_id = conn.execute("SELECT team_id FROM teams WHERE code = 'LAZ'").fetchone()
                lecce_id = conn.execute("SELECT team_id FROM teams WHERE code = 'LEC'").fetchone()
                
                if lazio_id and lecce_id:
                    print(f"DB Team IDs: Lazio={lazio_id[0]}, Lecce={lecce_id[0]}")
                    # Find game
                    game = conn.execute(
                        "SELECT game_id, start_time_utc FROM games WHERE (home_team_id = ? AND away_team_id = ?) OR (home_team_id = ? AND away_team_id = ?)",
                        (lazio_id[0], lecce_id[0], lecce_id[0], lazio_id[0])
                    ).fetchall()
                    print(f"Found {len(game)} games in DB between these teams:")
                    for g in game:
                        print(f"  - {g['game_id']} at {g['start_time_utc']}")
                else:
                    print("Could not find team IDs in DB")
        else:
            print(f"✅ Found {len(odds_df)} odds records in DB.")
            print(odds_df[['book', 'outcome', 'line', 'moneyline']].to_string())
            
            # Check if the displayed odds match any of the DB odds
            displayed_ml = row['moneyline']
            displayed_book = row['book']
            
            match = odds_df[
                (odds_df['book'] == displayed_book) & 
                (odds_df['outcome'].str.lower() == row['side'].lower())
            ]
            
            if not match.empty:
                db_ml = match.iloc[0]['moneyline']
                db_line = match.iloc[0]['line']
                
                print(f"\nVerification:")
                print(f"  Displayed: {displayed_book} | Line: {row['total_line']} | Odds: {displayed_ml}")
                print(f"  DB Actual: {displayed_book} | Line: {db_line} | Odds: {db_ml}")
                
                if abs(displayed_ml - db_ml) < 0.1:
                    print("✅ Odds match!")
                else:
                    print(f"❌ Odds MISMATCH! Displayed {displayed_ml} but DB has {db_ml}")
                
                if abs(row['total_line'] - db_line) < 0.1:
                    print("✅ Lines match!")
                else:
                    print(f"❌ Line MISMATCH! Displayed {row['total_line']} but DB has {db_line}")
            else:
                print(f"❌ Could not find DB record for book '{displayed_book}' and side '{row['side']}'")

if __name__ == "__main__":
    debug_lazio_odds()
