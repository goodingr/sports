import sqlite3

def check_games_schema():
    conn = sqlite3.connect('data/betting.db')
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(games)")
    columns = cursor.fetchall()
    print("Columns in games:")
    for col in columns:
        print(col)
    conn.close()

if __name__ == "__main__":
    check_games_schema()
