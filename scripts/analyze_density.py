import pandas as pd
from src.db.core import connect

def analyze_density():
    with connect() as conn:
        print("Analyzing Predictions Density...")
        
        # 1. Total count
        valid_models = ['ensemble', 'random_forest', 'gradient_boosting']
        query_overview = """
            SELECT model_type, count(*) as count 
            FROM predictions 
            GROUP BY model_type
        """
        df_models = pd.read_sql_query(query_overview, conn)
        print("\nBreakdown by Model:")
        print(df_models.to_string())
        
        # 2. Rows per game
        query_per_game = """
            SELECT game_id, count(*) as update_count 
            FROM predictions 
            WHERE model_type = 'ensemble'
            GROUP BY game_id 
            ORDER BY update_count DESC 
            LIMIT 10
        """
        df_games = pd.read_sql_query(query_per_game, conn)
        print("\nTop 10 Games by Update Frequency (Ensemble):")
        print(df_games.to_string())
        
        # 3. Time range for a specific high-volume game
        if not df_games.empty:
            top_game = df_games.iloc[0]['game_id']
            query_history = f"""
                SELECT predicted_at, home_prob, home_moneyline 
                FROM predictions 
                WHERE game_id = '{top_game}' AND model_type = 'ensemble'
                ORDER BY predicted_at
            """
            df_hist = pd.read_sql_query(query_history, conn)
            print(f"\nHistory sample for {top_game} ({len(df_hist)} rows):")
            print(df_hist.head(3).to_string())
            print("...")
            print(df_hist.tail(3).to_string())

if __name__ == "__main__":
    analyze_density()
