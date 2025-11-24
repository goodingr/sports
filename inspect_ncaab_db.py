import pandas as pd
from src.db.core import connect

def inspect_ncaab_data():
    with connect() as conn:
        # Check if we have any NCAAB games
        games = pd.read_sql_query(
            """
            SELECT g.game_id, g.season, ht.code as home_team, at.code as away_team
            FROM games g
            JOIN teams ht ON ht.team_id = g.home_team_id
            JOIN teams at ON at.team_id = g.away_team_id
            JOIN sports s ON s.sport_id = g.sport_id
            WHERE s.league = 'NCAAB'
            LIMIT 5
            """,
            conn
        )
        print("NCAAB Games Sample:")
        print(games)

        if not games.empty:
            # Check results for these games
            game_ids = tuple(games['game_id'].tolist())
            results = pd.read_sql_query(
                f"""
                SELECT * FROM game_results WHERE game_id IN {game_ids}
                """,
                conn
            )
            print("\nGame Results Sample:")
            print(results)

if __name__ == "__main__":
    inspect_ncaab_data()
