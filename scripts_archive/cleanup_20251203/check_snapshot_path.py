import sqlite3

def check_raw_path():
    conn = sqlite3.connect("data/betting.db")
    cursor = conn.execute("SELECT raw_path FROM odds_snapshots WHERE snapshot_id = 'f6fde5bf962b4c23b13a189c6240fec5'")
    row = cursor.fetchone()
    if row:
        print(row[0])
    else:
        print("Snapshot not found")

if __name__ == "__main__":
    check_raw_path()
