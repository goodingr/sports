
import sqlite3
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).resolve().parents[1]
sys.path.append(str(project_root))

from src.db.core import connect

def list_teams():
    with connect() as conn:
        rows = conn.execute("""
            SELECT s.league, t.code, t.name 
            FROM teams t
            JOIN sports s ON t.sport_id = s.sport_id
            ORDER BY s.league, t.name
        """).fetchall()
        
        print(f"{'LEAGUE':<10} {'CODE':<10} {'NAME'}")
        print("-" * 50)
        for row in rows:
            print(f"{row['league']:<10} {row['code']:<10} {row['name']}")

if __name__ == "__main__":
    list_teams()
