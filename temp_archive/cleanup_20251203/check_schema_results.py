import sqlite3

def check_schema():
    conn = sqlite3.connect('data/betting.db')
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(game_results)")
    columns = cursor.fetchall()
    print("Columns in game_results:")
    for col in columns:
        print(col)
    conn.close()

if __name__ == "__main__":
    check_schema()
