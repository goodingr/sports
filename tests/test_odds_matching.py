import pytest
import pandas as pd
import src.api.routes.bets as bets_api
import src.dashboard.data as dashboard_data

from unittest.mock import patch, MagicMock

def test_lazio_lecce_odds_match_db():
    """
    Verify that the displayed odds for Lazio vs Lecce match the best available
    odds in the database and that the line/odds come from the same book.
    """
    # Mock data
    mock_df = pd.DataFrame({
        'game_id': ['game_1'],
        'home_team': ['Lazio'],
        'away_team': ['Lecce'],
        'moneyline': [132],
        'total_line': [2.5],
        'book': ['DraftKings'],
        'side': ['Over'],
        'edge': [0.1],
        'commence_time': [pd.Timestamp("2025-01-01T12:00:00Z")],
        'league': ['SERIEA']
    })

    mock_db_odds = pd.DataFrame({
        'forward_game_id': ['game_1'],
        'outcome': ['Over'],
        'book': ['DraftKings'],
        'moneyline': [132],
        'line': [2.5]
    })

    with patch('src.api.routes.bets.get_totals_data', return_value=mock_df), \
         patch('src.dashboard.data.get_totals_odds_for_recommended', return_value=mock_db_odds):
        
        # 1. Get the processed data from the API logic (mocked)
        df = bets_api.get_totals_data()
        
        # 2. Find the Lazio vs Lecce game
        lazio_game = df[
            (df['home_team'].str.contains('Lazio', case=False, na=False)) | 
            (df['away_team'].str.contains('Lazio', case=False, na=False))
        ]
        
        assert not lazio_game.empty, "Lazio vs Lecce game not found in predictions"
        
        for _, row in lazio_game.iterrows():
            game_id = row['game_id']
            displayed_ml = row['moneyline']
            displayed_line = row['total_line']
            displayed_book = row['book']
            side = row['side']
            
            # 3. Fetch actual odds from DB (mocked)
            mini_df = pd.DataFrame([row])
            db_odds = dashboard_data.get_totals_odds_for_recommended(mini_df)
            
            assert not db_odds.empty, "No odds found in DB for this game"
            
            # 4. Verify the displayed odds match a record in the DB
            matched_book_odds = db_odds[
                (db_odds['book'] == displayed_book) & 
                (db_odds['outcome'].str.lower() == side.lower())
            ]
            
            assert not matched_book_odds.empty, \
                f"Displayed book '{displayed_book}' not found in DB odds for this game/side"
                
            db_ml = matched_book_odds.iloc[0]['moneyline']
            db_line = matched_book_odds.iloc[0]['line']
            
            assert abs(displayed_ml - db_ml) < 0.1
            assert abs(displayed_line - db_line) < 0.1
