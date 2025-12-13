
import logging
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import difflib

# Add project root to path
sys.path.append(str(Path.cwd()))

from src.db.core import connect

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

REPORT_PATH = Path(r"C:\Users\Bobby\.gemini\antigravity\brain\c240361c-af3f-4bda-bf92-79e7b5adc3a7\duplicate_report.md")

def normalize_name(name):
    """Normalize team name for comparison."""
    if not name: return ""
    # Lowercase
    n = name.lower()
    # Remove common suffixes/prefixes
    remove_words = ["fc", "cf", "sc", "united", "city", "state", "university", "st", "univ"]
    tokens = n.split()
    tokens = [t for t in tokens if t not in remove_words]
    return " ".join(tokens)

def are_teams_similar(name1, name2):
    """
    Check if two team names are similar enough to be the same team.
    Uses SequenceMatcher and token set logic.
    """
    n1 = normalize_name(name1)
    n2 = normalize_name(name2)
    
    if not n1 or not n2: return False
    
    # Exact match after normalization
    if n1 == n2: return True
    
    # Ratcliff-Obershelp similarity
    similarity = difflib.SequenceMatcher(None, n1, n2).ratio()
    if similarity > 0.85: # High threshold
        return True
        
    # Token subset check (e.g. "Man Utd" in "Manchester United" - roughly)
    # Actually "man utd" -> "man utd", "manchester" -> "manchester"
    # Better: Is one a substring of the other?
    if n1 in n2 or n2 in n1:
        # Prevent "Man City" matching "Man Utd" via "Man"
        # Since we stripped "City" and "Utd" (United), we might be left with "Man" vs "Man".
        # Danger! "Manchester City" -> "Manchester", "Manchester United" -> "Manchester".
        # So we MUST NOT strip "City" or "United" blindly if it leads to ambiguity.
        pass
        
    # Let's revert the aggressive stripping for the similarity check, 
    # but use it for the "key" generation perhaps?
    # Actually, simpler: Use the full name for difflib, but maybe handle specific known abbreviations.
    
    # Re-comparing full names lowercased
    s1 = name1.lower()
    s2 = name2.lower()
    
    # Special cases: "Utd" vs "United"
    s1 = s1.replace("utd", "united")
    s2 = s2.replace("utd", "united")
    
    sim = difflib.SequenceMatcher(None, s1, s2).ratio()
    
    # "Manchester United" vs "Manchester City" -> 0.76 similarity. 
    # Threshold 0.85 should be safe? 
    # "Chelsea" vs "AFC Bournemouth" -> 0 (no match). 
    # "Bournemouth" vs "AFC Bournemouth" -> Match.
    
    if sim > 0.85: return True
    
    # Substring check for reliability
    # "Bournemouth" in "AFC Bournemouth" -> True
    if s1 in s2 or s2 in s1:
        # Guard against short matches like "Man" in "Manchester"
        if len(s1) > 4 and len(s2) > 4:
            return True
            
    return False

def generate_report():
    LOGGER.info("Fetching games...")
    
    with connect() as conn:
        # Fetch all games from last 30 days + future
        # We need extensive columns to debug
        query = """
            SELECT 
                g.game_id, 
                g.start_time_utc, 
                g.home_team_id, 
                g.away_team_id,
                ht.name as home_team, 
                at.name as away_team,
                g.sport_id,
                s.league,
                s.name as sport_name,
                (SELECT count(*) FROM odds o WHERE o.game_id = g.game_id) as odds_count,
                (SELECT count(*) FROM predictions p WHERE p.game_id = g.game_id) as pred_count
            FROM games g
            JOIN teams ht ON g.home_team_id = ht.team_id
            JOIN teams at ON g.away_team_id = at.team_id
            JOIN sports s ON g.sport_id = s.sport_id
            WHERE g.start_time_utc > datetime('now', '-30 days')
            ORDER BY g.start_time_utc DESC
        """
        df = pd.read_sql_query(query, conn)
        
    if df.empty:
        LOGGER.info("No games found.")
        return

    # Group by (League, Date)
    # Date = YYYY-MM-DD
    df['date_str'] = df['start_time_utc'].apply(lambda x: str(x)[:10])
    
    groups = df.groupby(['league', 'date_str'])
    
    duplicates = []
    
    LOGGER.info(f"Scanning {len(df)} games for duplicates...")
    
    for (league, date), group in groups:
        if len(group) < 2: continue
        
        # O(N^2) comparison within daily league group (usually small, < 20 games)
        existing_matches = set()
        
        games = group.to_dict('records')
        
        for i in range(len(games)):
            for j in range(i + 1, len(games)):
                g1 = games[i]
                g2 = games[j]
                
                # Check timestamps (allow small diff)
                # But duplicates often have slightly different times, e.g. 15:00 vs 15:05
                # The grouping by date handles the coarse filter.
                
                # Check Similarity
                home_sim = are_teams_similar(g1['home_team'], g2['home_team'])
                away_sim = are_teams_similar(g1['away_team'], g2['away_team'])
                
                # Swapped check (Home vs Away) ?
                # "Chelsea vs Bournemouth" vs "Bournemouth vs Chelsea"
                home_away_sim = are_teams_similar(g1['home_team'], g2['away_team'])
                away_home_sim = are_teams_similar(g1['away_team'], g2['home_team'])
                
                is_direct_match = home_sim and away_sim
                is_swapped_match = home_away_sim and away_home_sim
                
                if is_direct_match or is_swapped_match:
                    # Found a pair
                    pair_id = tuple(sorted([g1['game_id'], g2['game_id']]))
                    if pair_id in existing_matches: continue
                    existing_matches.add(pair_id)
                    
                    duplicates.append({
                        'league': league,
                        'date': date,
                        'type': 'Swapped' if is_swapped_match else 'Direct',
                        'game1': g1,
                        'game2': g2
                    })

    # Generate Text Report
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    txt_path = Path(r"C:\Users\Bobby\Desktop\sports\duplicate_report.txt")
    
    with open(txt_path, 'w', encoding='utf-8') as f:
        f.write(f"Duplicate Games Report\nGenerated: {timestamp}\n\n")
        f.write(f"Found {len(duplicates)} duplicate pairs.\n")
        f.write("="*80 + "\n\n")
        
        if not duplicates:
            f.write("No duplicates found based on current similarity logic.\n")
        else:
            for idx, d in enumerate(duplicates, 1):
                g1 = d['game1']
                g2 = d['game2']
                
                f.write(f"{idx}. {d['league']} - {d['date']} ({d['type']})\n")
                f.write("-" * 80 + "\n")
                f.write(f"{'Game ID':<35} | {'Home Team':<25} | {'Away Team':<25} | {'Odds':<5} | {'Preds'}\n")
                f.write("-" * 80 + "\n")
                
                def write_row(g):
                    return f"{g['game_id']:<35} | {g['home_team']:<25} | {g['away_team']:<25} | {str(g['odds_count']):<5} | {g['pred_count']}\n"
                
                f.write(write_row(g1))
                f.write(write_row(g2))
                f.write("\n")
        
    LOGGER.info(f"Report generated at {txt_path}")

if __name__ == "__main__":
    generate_report()
