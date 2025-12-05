import pandas as pd
import numpy as np

# Mock recommended dataframe (from parquet)
recommended = pd.DataFrame([{
    "game_id": "EPL_740735",
    "side": "over",
    "total_line": 3.0, # Stale line
    "predicted_total_points": 3.1,
    "edge": 0.1,
    "league": "EPL"
}])

# Mock best_odds dataframe (from DB)
best_odds = pd.DataFrame([{
    "forward_game_id": "EPL_740735",
    "outcome": "over",
    "book": "BetRivers",
    "moneyline": 128.0,
    "line": 3.5, # Fresh line
    "home_team_full": "Man Utd",
    "away_team_full": "West Ham"
}])

print("Before merge:")
print(recommended[["total_line"]])

# Merge logic from app.py
recommended = recommended.merge(
    best_odds[['forward_game_id', 'outcome', 'book', 'moneyline', 'line', 'home_team_full', 'away_team_full']],
    left_on=['game_id', 'side'],
    right_on=['forward_game_id', 'outcome'],
    how='left',
    suffixes=('', '_sportsbook')
)

print("\nAfter merge columns:", recommended.columns.tolist())
print("Line column values:", recommended["line"].tolist())

# Handle line column
line_col = 'line_sportsbook' if 'line_sportsbook' in recommended.columns else 'line'
print("Line col:", line_col)

if line_col in recommended.columns:
    has_sportsbook_data = recommended[line_col].notna()
    recommended.loc[has_sportsbook_data, 'total_line'] = recommended.loc[has_sportsbook_data, line_col]
    
    # Update description
    recommended.loc[has_sportsbook_data, 'description'] = recommended.loc[has_sportsbook_data].apply(
        lambda row: f"{row['side'].title()} {row['total_line']:.1f}" if pd.notna(row['total_line']) else row['side'].title(),
        axis=1
    )

print("\nFinal total_line:", recommended["total_line"].tolist())
print("Final description:", recommended["description"].tolist())
