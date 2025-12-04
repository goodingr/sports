import sqlite3

def check_odds_schema():
    conn = sqlite3.connect('data/betting.db')
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(odds)")
    columns = cursor.fetchall()
    print("Columns in odds:")
    for col in columns:
        print(col)
    conn.close()

if __name__ == "__main__":
    check_odds_schema()
