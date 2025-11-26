import sqlite3

def list_tables():
    conn = sqlite3.connect('data/betting.db')
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print("Tables in database:")
    for table in tables:
        print(table[0])
    conn.close()

if __name__ == "__main__":
    list_tables()
