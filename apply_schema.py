import sqlite3
from pathlib import Path

DB_PATH = Path("data/betting.db")
SCHEMA_PATH = Path("src/db/schema.sql")

print(f"Applying schema to {DB_PATH.resolve()}")

with open(SCHEMA_PATH, "r") as f:
    schema_sql = f.read()

try:
    conn = sqlite3.connect(DB_PATH)
    conn.executescript(schema_sql)
    conn.commit()
    print("Schema applied successfully.")
    
    # Verify
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='predictions';")
    if cursor.fetchone():
        print("VERIFIED: predictions table exists.")
    else:
        print("ERROR: predictions table still NOT found.")
        
    conn.close()
except Exception as e:
    print(f"Error applying schema: {e}")
