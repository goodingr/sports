import sqlite3
import pandas as pd

conn = sqlite3.connect("data/betting.db")

# Get schema
schema = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='game_results'").fetchone()
print("game_results table schema:")
print(schema[0])

# Get sample data
print("\n\nSample game_results data:")
df = pd.read_sql_query('SELECT * FROM game_results LIMIT 5', conn)
print(df.to_string())

# Check how many have model predictions
print("\n\nChecking for model predictions in game_results...")
with_predictions = conn.execute("SELECT COUNT(*) FROM game_results WHERE home_moneyline_prediction IS NOT NULL").fetchone()[0]
print(f"Rows with home_moneyline_prediction: {with_predictions}")

conn.close()
