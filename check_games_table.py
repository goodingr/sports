import sqlite3
import pandas as pd
from src.db.core import connect

def check_games():
    with connect() as conn:
        # Check sports table
        sports = pd.read_sql("SELECT * FROM sports", conn)
        print("Sports in DB:")
        print(sports)
        
        # Check games count by sport
        query = """
        SELECT s.league, COUNT(g.game_id) as game_count, 
               MIN(g.start_time_utc) as first_game, 
               MAX(g.start_time_utc) as last_game
        FROM games g
        JOIN sports s ON g.sport_id = s.sport_id
        GROUP BY s.league
        """
        counts = pd.read_sql(query, conn)
        print("\nGame counts by league:")
        print(counts)
        
        # Check upcoming games
        query_upcoming = """
        SELECT s.league, COUNT(g.game_id) as upcoming_count
        FROM games g
        JOIN sports s ON g.sport_id = s.sport_id
        WHERE g.start_time_utc > datetime('now')
        GROUP BY s.league
        """
        upcoming = pd.read_sql(query_upcoming, conn)
        print("\nUpcoming games by league:")
        print(upcoming)

if __name__ == "__main__":
    check_games()
