
import sys
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.data.team_mappings import get_full_team_name

def debug_fix():
    print("Testing get_full_team_name...")
    
    test_cases = [
        ("NCAAB", "SCO"),
        ("NCAAB", "AHO"),
        ("NCAAB", "MRE"),
        ("NCAAB", "ALI"),
        ("NCAAB", "CGR"),
        ("NCAAB", "BBE"),
        ("NBA", "LAL"), # Should work normally
    ]
    
    for league, code in test_cases:
        name = get_full_team_name(league, code)
        print(f"League: {league}, Code: {code} -> Name: {name}")
        
        if league == "NCAAB" and code == "SCO" and name != "SIU Edwardsville":
            print("  FAIL: Expected SIU Edwardsville")
        elif league == "NCAAB" and code == "AHO" and name != "Alabama State":
            print("  FAIL: Expected Alabama State")

if __name__ == "__main__":
    debug_fix()
