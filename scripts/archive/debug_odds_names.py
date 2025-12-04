import sqlite3
import pandas as pd

def check_odds_names():
    conn = sqlite3.connect('data/betting.db')
    
    # Query recent odds snapshots
    query = """
    SELECT 
        game_id, 
        book, 
        home_team, 
        away_team, 
        commence_time 
    FROM odds_snapshots 
    WHERE book LIKE '%LowVig%' 
    ORDER BY commence_time DESC 
    LIMIT 20
    """
    
    try:
        df = pd.read_sql_query(query, conn)
        print("Recent LowVig Odds Data:")
        print(df[['home_team', 'away_team', 'commence_time']].to_string())
        
        # Check for specific codes
        print("\nChecking for specific codes (TTI, TRO, etc):")
        codes = ['TTI', 'TRO', 'RRA', 'TTR', 'UAG']
        for code in codes:
            match = df[
                (df['home_team'] == code) | 
                (df['away_team'] == code)
            ]
            if not match.empty:
                print(f"Found code {code} in DB!")
            else:
                print(f"Code {code} NOT found in DB (likely has full name)")
                
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    check_odds_names()
