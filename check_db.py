import sqlite3
from pathlib import Path

DB_PATH = Path("data/betting.db")
print(f"Checking {DB_PATH.resolve()}")

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print("Tables:", [t[0] for t in tables])

if "predictions" in [t[0] for t in tables]:
    print("predictions table FOUND")
    cursor.execute("PRAGMA table_info(predictions)")
    print(cursor.fetchall())
else:
    print("predictions table NOT FOUND")

conn.close()
