import sqlite3
from src.data.team_mappings import normalize_team_code

def debug_load_games():
    db_path = "data/betting.db"
    game_id = "SERIEA_4c9f3ce33da277b2e8830f22af05b7cd" # Como vs Inter Milan
    league = "SERIEA"
    
    print(f"Debugging game: {game_id}")
    
    with sqlite3.connect(db_path) as conn:
        # Get game details
        game_row = conn.execute(
            """
            SELECT g.game_id, ht.name as home_team, at.name as away_team
            FROM games g
            JOIN teams ht ON g.home_team_id = ht.team_id
            JOIN teams at ON g.away_team_id = at.team_id
            WHERE g.game_id = ?
            """,
            (game_id,)
        ).fetchone()
        
        if not game_row:
            print("Game not found in DB")
            return
            
        _, home_team_db, away_team_db = game_row
        print(f"DB Home Team: {home_team_db}")
        print(f"DB Away Team: {away_team_db}")
        
        # Get odds
        odds_rows = conn.execute(
            """
            SELECT b.name as book, o.market, o.outcome, o.price_american
            FROM odds o
            JOIN books b ON o.book_id = b.book_id
            WHERE o.game_id = ?
            """,
            (game_id,)
        ).fetchall()
        
        print(f"Found {len(odds_rows)} odds rows")
        
        # Simulate load_games_from_database mapping
        mapped_outcomes = []
        for book, market, outcome, price in odds_rows:
            if market != "h2h": continue
            
            outcome_name = outcome
            if outcome == "home":
                outcome_name = home_team_db
            elif outcome == "away":
                outcome_name = away_team_db
            
            mapped_outcomes.append((book, outcome_name, price))
            
        print(f"Mapped {len(mapped_outcomes)} h2h outcomes")
        
        # Simulate _extract_moneyline_prices
        home_team_norm = normalize_team_code(league, home_team_db)
        away_team_norm = normalize_team_code(league, away_team_db)
        print(f"Normalized Home: {home_team_norm}")
        print(f"Normalized Away: {away_team_norm}")
        
        for book, outcome_name, price in mapped_outcomes:
            name_raw = outcome_name.strip()
            name_lower = name_raw.lower()
            
            if name_lower in ("draw", "tie", "x"):
                print(f"  Book {book}: Found Draw ({price})")
                continue
                
            outcome_team = normalize_team_code(league, name_raw)
            if outcome_team == home_team_norm:
                print(f"  Book {book}: Found Home ({price})")
            elif outcome_team == away_team_norm:
                print(f"  Book {book}: Found Away ({price})")
            else:
                print(f"  Book {book}: Mismatch! '{outcome_name}' -> '{outcome_team}' != '{home_team_norm}' or '{away_team_norm}'")

if __name__ == "__main__":
    debug_load_games()
