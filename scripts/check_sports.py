
import sqlite3
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).resolve().parents[1]
sys.path.append(str(project_root))

from src.db.core import connect

def list_sports():
    with connect() as conn:
        rows = conn.execute("SELECT * FROM sports").fetchall()
        print(f"{'ID':<5} {'LEAGUE':<10} {'NAME'}")
        print("-" * 30)
        for row in rows:
            print(f"{row['sport_id']:<5} {row['league']:<10} {row['name']}")

if __name__ == "__main__":
    list_sports()
