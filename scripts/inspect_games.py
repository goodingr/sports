import sqlite3
import pandas as pd
import sys
from pathlib import Path

# Add project root to path
sys.path.append(str(Path.cwd()))
from src.db.core import connect

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows', 100)

def inspect_games():
    matchups = [
        ("Miss Valley", "Kentucky"),
        ("North Florida", "Gonzaga"),
        ("Central Michigan", "Saint Louis")
    ]
    
    output_lines = []
    output_lines.append("# Data Inspection Report")
    
    with connect() as conn:
        for home, away in matchups:
            print(f"Processing {home} vs {away}...")
            
            # Using parameters for safer queries
            query_games = """
            SELECT * FROM games 
            WHERE (home_team_id IN (SELECT team_id FROM teams WHERE name LIKE ?) 
                   OR away_team_id IN (SELECT team_id FROM teams WHERE name LIKE ?))
              AND (home_team_id IN (SELECT team_id FROM teams WHERE name LIKE ?) 
                   OR away_team_id IN (SELECT team_id FROM teams WHERE name LIKE ?))
              AND start_time_utc > '2025-11-01'
            """
            params = (f'%{home}%', f'%{home}%', f'%{away}%', f'%{away}%')
            
            try:
                games_df = pd.read_sql_query(query_games, conn, params=params)
                
                if games_df.empty:
                    output_lines.append(f"\n# {home} vs {away}")
                    output_lines.append("No games found matching these teams.")
                    continue

                # Fetch Game Results First for Summary
                results_df = pd.read_sql_query("SELECT * FROM game_results WHERE game_id = ?", conn, params=(games_df.iloc[0]['game_id'],))
                
                winner_text = "Unknown/Not Played"
                if not results_df.empty:
                    hs = results_df.iloc[0]['home_score']
                    as_ = results_df.iloc[0]['away_score']
                    if hs is not None and as_ is not None:
                        if hs > as_:
                            winner_text = f"Home ({hs}-{as_})"
                        elif as_ > hs:
                            winner_text = f"Away ({as_}-{hs})"
                        else:
                            winner_text = f"Draw ({hs}-{as_})"

                output_lines.append(f"\n# {home} vs {away}")
                output_lines.append(f"**Game ID**: `{games_df.iloc[0]['game_id']}`")
                output_lines.append(f"**Winner**: {winner_text}")

                for _, game in games_df.iterrows():
                    game_id = game['game_id']
                    
                    # Games Table
                    output_lines.append("\n## Games Table")
                    output_lines.append(games_df[games_df['game_id'] == game_id].to_markdown(index=False))

                    # Predictions
                    output_lines.append("\n## Predictions Table")
                    preds_df = pd.read_sql_query("SELECT * FROM predictions WHERE game_id = ?", conn, params=(game_id,))
                    if preds_df.empty:
                        output_lines.append("No predictions found.")
                    else:
                        output_lines.append(preds_df.to_markdown(index=False))

                    # Odds
                    output_lines.append("\n## Odds Table")
                    odds_df = pd.read_sql_query("SELECT * FROM odds WHERE game_id = ?", conn, params=(game_id,))
                    if odds_df.empty:
                        output_lines.append("No odds found.")
                    else:
                        output_lines.append(odds_df.head(20).to_markdown(index=False))

                    # Results
                    output_lines.append("\n## Game Results Table")
                    results_df = pd.read_sql_query("SELECT * FROM game_results WHERE game_id = ?", conn, params=(game_id,))
                    if results_df.empty:
                        output_lines.append("No results found.")
                    else:
                        output_lines.append(results_df.to_markdown(index=False))
                        output_lines.append("\n**Raw Result Row:**")
                        for col, val in results_df.iloc[0].items():
                            output_lines.append(f"- **{col}**: `{repr(val)}`")

            except Exception as e:
                output_lines.append(f"Error querying for {home} vs {away}: {e}")

    with open("data.md", "w", encoding="utf-8") as f:
        f.write("\n".join(output_lines))
    print("Report written to data.md")

if __name__ == "__main__":
    inspect_games()
