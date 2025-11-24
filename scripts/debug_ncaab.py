
import sys
import csv
from pathlib import Path

# Add src to path
sys.path.append(str(Path(__file__).resolve().parents[1]))

from src.data.team_mappings import _load_ncaab_team_mappings, normalize_team_code, NCAAB_TEAM_NAMES

def debug_mappings():
    print("Loading mappings...")
    codes, aliases, names = _load_ncaab_team_mappings()
    print(f"Loaded {len(codes)} codes, {len(aliases)} aliases, {len(names)} names")

    targets = ["AHO", "SCO", "ALI", "MRE", "BBE", "CGR", "NOS", "PPA", "CLI", "LLA", "DDR", "OMO", "GJA", "WWO", "SBU", "MBE", "ABR", "OSO", "CTI", "NBE", "VCA", "BBU", "MHA", "RCO", "SSA", "DHE", "XMU", "WMO", "WCO", "NSP", "PPI", "STO", "DDE", "HBI", "EBU", "UWA", "MGR", "LCA", "AFA", "IJA", "IRE", "CCH", "FPA", "QRO", "QBO", "SGA", "NWI", "CCO", "YBU", "BEA", "TWA", "MRA", "JTI", "WEA", "EAC", "AZI", "UMA", "GBU", "ATI", "MWO", "SAZ", "MTE", "URE", "SST"]
    
    # Load spellings manually to inspect them
    base_dir = Path(__file__).resolve().parents[1] / "data" / "external" / "mm2025"
    spellings_path = base_dir / "MTeamSpellings.csv"
    
    team_nicknames = {} # TeamID -> Nickname
    
    print(f"Reading spellings from {spellings_path}...")
    try:
        with open(spellings_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                if count < 50:
                    print(f"Row {count}: {row}")
                    count += 1
                
                team_id = row.get("TeamID")
                spelling = row.get("TeamNameSpelling", "").strip()
                
                # Find the team name for this ID
                if team_id in names:
                    team_name = names[team_id]
                    # Check if spelling starts with team name
                    # Relaxed check: just print some examples where spelling != team_name
                    # if spelling.lower() != team_name.lower():
                    #     print(f"  {team_name} -> {spelling}")
                        
                    if spelling.lower().startswith(team_name.lower()):
                        suffix = spelling[len(team_name):].strip()
                        if suffix:
                            if team_id not in team_nicknames:
                                team_nicknames[team_id] = set()
                            team_nicknames[team_id].add(suffix)
    except Exception as e:
        print(f"Error reading spellings: {e}")
        return

    print(f"Found potential nicknames for {len(team_nicknames)} teams.")
    
    # Try to generate codes
    matches = {}
    for team_id, potential_nicks in team_nicknames.items():
        team_name = names[team_id]
        for nick in potential_nicks:
            # Pattern: School[0] + Nick[0:2] (Upper)
            if not team_name or not nick:
                continue
                
            # Clean nickname (remove "Univ", etc if needed, but usually it's just the nickname)
            # nick might be "Hornets"
            
            code = (team_name[0] + nick[:2]).upper()
            
            if code in targets:
                if code not in matches:
                    matches[code] = []
                matches[code].append(f"{team_name} {nick}")

    print("\nMatches found:")
    for code, candidates in matches.items():
        print(f"{code}: {candidates}")
        
    print(f"\nTotal matched codes: {len(matches)}/{len(targets)}")

if __name__ == "__main__":
    debug_mappings()
