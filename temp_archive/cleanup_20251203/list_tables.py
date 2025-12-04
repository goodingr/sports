import sqlite3

conn = sqlite3.connect("data/betting.db")

# List all tables
tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
print("All tables in database:")
for table in tables:
    print(f"  - {table[0]}")

# Check for game_results table
print("\n\nChecking for game_results table...")
result_tables = [t[0] for t in tables if 'result' in t[0].lower()]
if result_tables:
    print(f"Found: {result_tables}")
    for table in result_tables:
        schema = conn.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'").fetchone()
        print(f"\n{table} schema:")
        print(schema[0])
        
        # Count rows
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        print(f"\nRows in {table}: {count}")
else:
    print("No result tables found")

conn.close()
