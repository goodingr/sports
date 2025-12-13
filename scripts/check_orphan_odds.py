import pandas as pd
from src.db.core import connect

def check_orphans():
    print("Checking for orphan odds records...")
    with connect() as conn:
        query = """
            SELECT count(*) 
            FROM odds o 
            LEFT JOIN games g ON o.game_id = g.game_id 
            WHERE g.game_id IS NULL
        """
        count = conn.execute(query).fetchone()[0]
        
        total_odds = conn.execute("SELECT count(*) FROM odds").fetchone()[0]
        
        print(f"Total Odds Rows: {total_odds:,.0f}")
        print(f"Orphan Odds Rows (No matching Game): {count:,.0f}")
        
        if count > 0:
            print("\nSample Orphan Game IDs from Odds table:")
            sample_query = """
                SELECT DISTINCT o.game_id 
                FROM odds o 
                LEFT JOIN games g ON o.game_id = g.game_id 
                WHERE g.game_id IS NULL
                LIMIT 10
            """
            samples = pd.read_sql_query(sample_query, conn)
            print(samples.to_string(index=False))

if __name__ == "__main__":
    check_orphans()
