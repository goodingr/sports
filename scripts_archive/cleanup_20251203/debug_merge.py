import pandas as pd
import numpy as np

def test_merge():
    # Simulate 'completed' dataframe
    completed = pd.DataFrame([
        {
            "game_id": "game1",
            "side": "over",
            "total_line": 145.5,
            "moneyline": -110,
            "description": "Over 145.5"
        }
    ])

    # Simulate 'best_completed_odds' dataframe
    # User case: LowVig.ag Over 146.0 -108
    best_completed_odds = pd.DataFrame([
        {
            "forward_game_id": "game1",
            "outcome": "over",
            "book": "LowVig.ag",
            "moneyline": -108.0,
            "line": 146.0,
            "home_team_full": "UMass",
            "away_team_full": "Georgia Tech"
        }
    ])

    print("Before merge:")
    print(completed[["total_line", "moneyline"]])

    # Perform merge exactly as in app.py
    completed = completed.merge(
        best_completed_odds[['forward_game_id', 'outcome', 'book', 'moneyline', 'line', 'home_team_full', 'away_team_full']],
        left_on=['game_id', 'side'],
        right_on=['forward_game_id', 'outcome'],
        how='left',
        suffixes=('', '_sportsbook')
    )

    print("\nAfter merge columns:", completed.columns.tolist())
    if 'line' in completed.columns:
        print("Line column values:", completed['line'].values)

    # Handle book column
    if 'book' not in completed.columns:
        completed['book'] = ""
    if 'book_sportsbook' in completed.columns:
        completed['book'] = completed['book_sportsbook'].fillna(completed['book'])
        completed = completed.drop(columns=['book_sportsbook'], errors='ignore')

    # Handle moneyline column
    if 'moneyline_sportsbook' in completed.columns:
        completed['moneyline'] = completed['moneyline_sportsbook'].fillna(completed['moneyline'])
        completed = completed.drop(columns=['moneyline_sportsbook'], errors='ignore')

    # Handle line column
    line_col = 'line_sportsbook' if 'line_sportsbook' in completed.columns else 'line'
    print(f"\nUsing line_col: {line_col}")
    
    if line_col in completed.columns:
        has_sportsbook_data = completed[line_col].notna()
        print("Has sportsbook data:", has_sportsbook_data.values)
        
        completed.loc[has_sportsbook_data, 'total_line'] = completed.loc[has_sportsbook_data, line_col]
        
        # Regenerate description
        if 'description' in completed.columns and 'side' in completed.columns:
            completed.loc[has_sportsbook_data, 'description'] = completed.loc[has_sportsbook_data].apply(
                lambda row: f"{row['side'].title()} {row['total_line']:.1f}" if pd.notna(row['total_line']) else row['side'].title(),
                axis=1
            )

        # Drop the line column after using it
        completed = completed.drop(columns=[line_col, 'home_team_full', 'away_team_full'], errors='ignore')
    
    # Clean up merge columns
    completed = completed.drop(columns=['forward_game_id', 'outcome'], errors='ignore')

    print("\nFinal result:")
    print(completed[["total_line", "moneyline", "description"]])

if __name__ == "__main__":
    test_merge()
