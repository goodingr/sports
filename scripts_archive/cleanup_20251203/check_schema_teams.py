import sqlite3

def check_teams_schema():
    conn = sqlite3.connect('data/betting.db')
    cursor = conn.cursor()
    cursor.execute("PRAGMA table_info(teams)")
    columns = cursor.fetchall()
    print("Columns in teams:")
    for col in columns:
        print(col)
    conn.close()

if __name__ == "__main__":
    check_teams_schema()
