import sys
from pathlib import Path
import sqlite3

# Add project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.append(str(PROJECT_ROOT))

from src.data.team_mappings import normalize_team_code

def test_mappings():
    print("--- MAPPINGS ---")
    teams = [
        ("SERIEA", "Internazionale"),
        ("SERIEA", "Bologna"),
        ("EPL", "Nottm Forest"),
        ("EPL", "Nottingham Forest"),
        ("LALIGA", "Girona"),
        ("LIGUE1", "Stade de Reims"),
        ("LIGUE1", "Le Havre"),
        ("LIGUE1", "Clermont Foot"),
        ("LIGUE1", "Metz"),
    ]
    for league, name in teams:
        code = normalize_team_code(league, name)
        print(f"{league} {name} -> '{code}'")

def test_dates():
    print("\n--- DATES ---")
    db_date = '2026-01-04T19:45:00+00:00'
    espn_date = '2026-01-04T19:45Z'
    
    with sqlite3.connect(":memory:") as conn:
        try:
            row = conn.execute("SELECT ABS(julianDay(?) - julianDay(?))", (db_date, espn_date)).fetchone()
            print(f"Diff '{db_date}' vs '{espn_date}': {row[0]}")
        except Exception as e:
            print(f"Error comparing dates: {e}")

if __name__ == "__main__":
    test_mappings()
    test_dates()
