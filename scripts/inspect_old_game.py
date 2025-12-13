import pandas as pd
from src.db.core import connect

def inspect():
    with connect() as conn:
        # Use simple cursor fetch to avoid pandas/connector issues
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM games WHERE game_id = 'NBA_0021200250'")
        columns = [description[0] for description in cursor.description]
        row = cursor.fetchone()
        
        if row:
            print("Row found:")
            data = dict(zip(columns, row))
            for k, v in data.items():
                print(f"{k}: {v}")
        else:
            print("Game not found.")

if __name__ == "__main__":
    inspect()
