"""
Debug restoration for NFL.
"""
import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models.train import (
    CalibratedModel,
    EnsembleModel,
    ProbabilityCalibrator,
)
from src.models.forward_test import load_model, make_predictions

# Fix for unpickling if class was saved as __main__.CalibratedModel
import sys
sys.modules['__main__'].CalibratedModel = CalibratedModel
sys.modules['__main__'].EnsembleModel = EnsembleModel
sys.modules['__main__'].ProbabilityCalibrator = ProbabilityCalibrator

def debug_restore_nfl():
    print("Debugging NFL restoration...")
    
    conn = sqlite3.connect("data/betting.db")
    
    # Query NFL games
    query = """
    SELECT 
        gr.game_id,
        s.league,
        g.start_time_utc as commence_time,
        ht.name as home_team,
        at.name as away_team,
        gr.home_score,
        gr.away_score,
        gr.home_moneyline_close as home_moneyline,
        gr.away_moneyline_close as away_moneyline
    FROM game_results gr
    JOIN games g ON gr.game_id = g.game_id
    JOIN sports s ON g.sport_id = s.sport_id
    JOIN teams ht ON g.home_team_id = ht.team_id
    JOIN teams at ON g.away_team_id = at.team_id
    WHERE s.league = 'NFL'
    AND gr.home_score IS NOT NULL
    AND g.start_time_utc >= datetime('now', '-30 days')
    """
    
    games_df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"Found {len(games_df)} NFL games in DB")
    
    if len(games_df) == 0:
        return

    try:
        print("Loading model...")
        model = load_model(None, league='NFL', model_type='gradient_boosting')
        print("Model loaded successfully")
        
        games_list = []
        for _, game in games_df.iterrows():
            game_dict = {
                'id': game['game_id'],
                'sport_key': 'nfl',
                'commence_time': game['commence_time'],
                'home_team': game['home_team'],
                'away_team': game['away_team'],
                'bookmakers': [{
                    'key': 'historical',
                    'title': 'Closing Line',
                    'markets': [
                        {
                            'key': 'h2h',
                            'outcomes': [
                                {'name': game['home_team'], 'price': game['home_moneyline']},
                                {'name': game['away_team'], 'price': game['away_moneyline']}
                            ]
                        }
                    ]
                }]
            }
            games_list.append(game_dict)
        
        print(f"Prepared {len(games_list)} games for prediction")
        
        # Run predictions
        print("Running make_predictions...")
        predictions_df = make_predictions(games_list, model, league='NFL')
        print(f"Generated {len(predictions_df)} predictions")
        
        if not predictions_df.empty:
            print("Sample prediction:")
            print(predictions_df.iloc[0])
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    debug_restore_nfl()
