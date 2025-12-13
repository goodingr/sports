import time
from src.db.core import connect

def add_indexes():
    print("Adding performance indexes...")
    
    indexes = [
        # Games
        "CREATE INDEX IF NOT EXISTS idx_games_start_time ON games(start_time_utc)",
        
        # Predictions
        "CREATE INDEX IF NOT EXISTS idx_predictions_model_type ON predictions(model_type)",
        "CREATE INDEX IF NOT EXISTS idx_predictions_game_id ON predictions(game_id)",
        "CREATE INDEX IF NOT EXISTS idx_predictions_predicted_at ON predictions(predicted_at)",
        
        # Odds
        "CREATE INDEX IF NOT EXISTS idx_odds_game_id ON odds(game_id)",
        "CREATE INDEX IF NOT EXISTS idx_odds_last_modified ON odds_snapshots(fetched_at_utc)",
        
        # Results
        "CREATE INDEX IF NOT EXISTS idx_game_results_game_id ON game_results(game_id)"
    ]
    
    with connect() as conn:
        for idx_sql in indexes:
            try:
                print(f"Executing: {idx_sql}")
                start = time.time()
                conn.execute(idx_sql)
                conn.commit()
                print(f"  Done in {time.time() - start:.2f}s")
            except Exception as e:
                print(f"  Error: {e}")
                
    print("Indexes added successfully.")

if __name__ == "__main__":
    add_indexes()
