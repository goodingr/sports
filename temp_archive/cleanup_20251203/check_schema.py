import sqlite3

conn = sqlite3.connect("data/betting.db")

# Get the games table schema
schema = conn.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name='games'").fetchone()
print("Games table schema:")
print(schema[0])

conn.close()
