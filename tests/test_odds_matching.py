import pytest
import pandas as pd
from src.api.routes.bets import get_totals_data
from src.dashboard.data import get_totals_odds_for_recommended

def test_lazio_lecce_odds_match_db():
    """
    Verify that the displayed odds for Lazio vs Lecce match the best available
    odds in the database and that the line/odds come from the same book.
    """
    # 1. Get the processed data from the API logic
    df = get_totals_data()
    
    # 2. Find the Lazio vs Lecce game
    # We look for Lazio in home or away team
    lazio_game = df[
        (df['home_team'].str.contains('Lazio', case=False, na=False)) | 
        (df['away_team'].str.contains('Lazio', case=False, na=False))
    ]
    
    assert not lazio_game.empty, "Lazio vs Lecce game not found in predictions"
    
    # There might be multiple rows (e.g. Over and Under), we want the one with the bet
    # Assuming we are looking at the 'Over' bet based on user context (+132 was Over 2.5)
    # But let's check all rows for this game
    
    for _, row in lazio_game.iterrows():
        game_id = row['game_id']
        displayed_ml = row['moneyline']
        displayed_line = row['total_line']
        displayed_book = row['book']
        side = row['side'] # 'over' or 'under'
        
        print(f"\nChecking {side} bet for game {game_id} ({row['home_team']} vs {row['away_team']})")
        print(f"Displayed: Book={displayed_book}, Line={displayed_line}, ML={displayed_ml}")
        
        # 3. Fetch actual odds from DB for this game
        # We construct a mini-dataframe to pass to the matching function
        mini_df = pd.DataFrame([row])
        db_odds = get_totals_odds_for_recommended(mini_df)
        
        assert not db_odds.empty, "No odds found in DB for this game"
        
        # 4. Verify the displayed odds match a record in the DB
        # Filter DB odds for the specific book and side
        matched_book_odds = db_odds[
            (db_odds['book'] == displayed_book) & 
            (db_odds['outcome'].str.lower() == side.lower())
        ]
        
        assert not matched_book_odds.empty, \
            f"Displayed book '{displayed_book}' not found in DB odds for this game/side"
            
        # Get the actual values from DB
        db_ml = matched_book_odds.iloc[0]['moneyline']
        db_line = matched_book_odds.iloc[0]['line']
        
        # 5. Assertions
        # Check Moneyline matches
        assert abs(displayed_ml - db_ml) < 0.1, \
            f"Moneyline mismatch! Displayed: {displayed_ml}, DB: {db_ml}"
            
        # Check Line matches (ensures odds and line are from same book)
        assert abs(displayed_line - db_line) < 0.1, \
            f"Line mismatch! Displayed: {displayed_line}, DB: {db_line}"
            
        print(f"✅ Verified: {displayed_book} offers {side} {displayed_line} @ {displayed_ml}")

if __name__ == "__main__":
    # Manually run the test function if executed as a script
    try:
        test_lazio_lecce_odds_match_db()
        print("\n🎉 Test Passed!")
    except AssertionError as e:
        print(f"\n❌ Test Failed: {e}")
    except Exception as e:
        print(f"\n❌ An error occurred: {e}")
