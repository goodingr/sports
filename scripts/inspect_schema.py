import pandas as pd
from src.db.core import connect

def inspect_odds_schema():
    with connect() as conn:
        print("Columns in 'odds' table:")
        print("-" * 30)
        # using PRAGMA table_info for cleaner output than .schema
        cursor = conn.execute("PRAGMA table_info(odds)")
        columns = cursor.fetchall()
        
        # format: cid, name, type, notnull, dflt_value, pk
        for col in columns:
            print(f"- {col[1]} ({col[2]})")

if __name__ == "__main__":
    inspect_odds_schema()
