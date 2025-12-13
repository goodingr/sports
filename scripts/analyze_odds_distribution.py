import pandas as pd
from src.db.core import connect

def analyze_odds():
    print("Analyzing Odds Table Distribution...")
    with connect() as conn:
        # 1. Rows per Game stats
        query_per_game = """
            SELECT game_id, count(*) as row_count 
            FROM odds 
            GROUP BY game_id
        """
        df_games = pd.read_sql_query(query_per_game, conn)
        
        print("\n--- Rows Per Game ---")
        print(df_games['row_count'].describe().to_string())
        print(f"\nTop 5 Games with most rows:")
        print(df_games.nlargest(5, 'row_count').to_string(index=False))

        # 2. Rows per Book
        print("\n--- Rows Per Book (Top 10) ---")
        query_books = """
            SELECT b.name, count(*) as count 
            FROM odds o 
            JOIN books b ON o.book_id = b.book_id 
            GROUP BY b.name 
            ORDER BY count DESC 
            LIMIT 10
        """
        print(pd.read_sql_query(query_books, conn).to_string())

        # 3. Duplicate Check (approximate)
        # Check if we have multiple entries for same game+book+market+snapshot (should be impossible due to PK/Unique?)
        # Let's check same game+book+market+outcome adjacent in time? Too complex for simple query.
        # Let's check distinct snapshots count
        n_snapshots = conn.execute("SELECT count(*) FROM odds_snapshots").fetchone()[0]
        print(f"\nTotal Snapshots: {n_snapshots}")
        print(f"Avg rows per snapshot: {223830 / n_snapshots if n_snapshots else 0:.1f}")

if __name__ == "__main__":
    analyze_odds()
