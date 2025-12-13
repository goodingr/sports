import pandas as pd
from src.db.core import connect

def analyze():
    print("Analyzing database...")
    with connect() as conn:
        query = """
                SELECT 
                    s.league,
                    g.season,
                    count(*) as count
                FROM games g
                JOIN sports s ON g.sport_id = s.sport_id
                LEFT JOIN game_results gr ON g.game_id = gr.game_id
                WHERE 
                    (g.odds_api_id IS NULL OR g.odds_api_id = '')
                    AND
                    (
                        gr.game_id IS NULL
                        OR
                        (
                            gr.home_moneyline_close IS NULL AND
                            gr.away_moneyline_close IS NULL AND
                            gr.spread_close IS NULL AND
                            gr.total_close IS NULL
                        )
                    )
                GROUP BY s.league, g.season
                ORDER BY count DESC
        """
        
        df = pd.read_sql_query(query, conn)
        print(f"\nTotal potential deletions: {df['count'].sum()}")
        print("\nDetailed Breakdown of Orphans:")
        print(df.to_string())

if __name__ == "__main__":
    analyze()
