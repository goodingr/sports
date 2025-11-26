"""
Restore historical predictions for completed games.
Uses game results and closing lines from database to recreate prediction records.
"""
import pandas as pd
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.models.forward_test import load_model, make_predictions
from src.models.train import (
    CalibratedModel,  # noqa: F401 - needed for joblib unpickling
    EnsembleModel,  # noqa: F401 - needed for joblib unpickling
    ProbabilityCalibrator,  # noqa: F401 - needed for joblib unpickling
)

def restore_completed_predictions(lookback_days=30):
    """
    Restore predictions for completed games by:
    1. Loading completed games from database
    2. Running model predictions with historical odds
    3. Adding results to predictions
    """
    
    print(f"Restoring completed predictions from last {lookback_days} days...")
    print("=" * 60)
    
    conn = sqlite3.connect("data/betting.db")
    
    # Query completed games with results and odds
    query = """
    SELECT 
        gr.game_id,
        s.league,
        g.start_time_utc as commence_time,
        ht.name as home_team,
        at.name as away_team,
        gr.home_score,
        gr.away_score,
        gr.home_moneyline_close as home_moneyline,
        gr.away_moneyline_close as away_moneyline,
        gr.spread_close,
        gr.total_close
    FROM game_results gr
    JOIN games g ON gr.game_id = g.game_id
    JOIN sports s ON g.sport_id = s.sport_id
    JOIN teams ht ON g.home_team_id = ht.team_id
    JOIN teams at ON g.away_team_id = at.team_id
    WHERE gr.home_score IS NOT NULL
    AND gr.away_score IS NOT NULL
    AND gr.home_moneyline_close IS NOT NULL
    AND gr.away_moneyline_close IS NOT NULL
    AND g.start_time_utc >= datetime('now', ?)
    ORDER BY g.start_time_utc DESC
    """
    
    completed_games = pd.read_sql_query(query, conn, params=(f'-{lookback_days} days',))
    conn.close()
    
    print(f"\nFound {len(completed_games)} completed games in database")
    
    if len(completed_games) == 0:
        print("No completed games to restore")
        return
    
    # Group by league
    by_league = completed_games.groupby('league')
    print(f"\nGames by league:")
    for league, games in by_league:
        print(f"  {league}: {len(games)}")
    
    # Process each league
    all_predictions = []
    
    for league, games_df in by_league:
        print(f"\n\nProcessing {league}...")
        
        try:
            # Load the gradient_boosting model for this league (more reliable than ensemble)
            model = load_model(None, league=league, model_type='gradient_boosting')
            
            # Convert games to the format expected by make_predictions
            games_list = []
            for _, game in games_df.iterrows():
                # Create a minimal game dict with the data we have
                game_dict = {
                    'id': game['game_id'],
                    'sport_key': league.lower(),
                    'commence_time': game['commence_time'],
                    'home_team': game['home_team'],
                    'away_team': game['away_team'],
                    'bookmakers': [{
                        'key': 'historical',
                        'title': 'Closing Line',
                        'markets': [
                            {
                                'key': 'h2h',
                                'outcomes': [
                                    {'name': game['home_team'], 'price': game['home_moneyline']},
                                    {'name': game['away_team'], 'price': game['away_moneyline']}
                                ]
                            }
                        ]
                    }]
                }
                
                # For soccer, we need a Draw price for the model to accept the odds
                # Since game_results doesn't store draw odds, we inject a dummy value
                # so that home/away odds are preserved.
                if league in ['EPL', 'LALIGA', 'BUNDESLIGA', 'SERIEA', 'LIGUE1']:
                    game_dict['bookmakers'][0]['markets'][0]['outcomes'].append(
                        {'name': 'Draw', 'price': 250}  # Dummy +250
                    )
                
                # Add totals if available
                if pd.notna(game['total_close']):
                    game_dict['bookmakers'][0]['markets'].append({
                        'key': 'totals',
                        'outcomes': [
                            {'name': 'Over', 'price': -110, 'point': game['total_close']},
                            {'name': 'Under', 'price': -110, 'point': game['total_close']}
                        ]
                    })
                
                # Add spreads if available
                if pd.notna(game['spread_close']):
                    game_dict['bookmakers'][0]['markets'].append({
                        'key': 'spreads',
                        'outcomes': [
                            {'name': game['home_team'], 'price': -110, 'point': game['spread_close']},
                            {'name': game['away_team'], 'price': -110, 'point': -game['spread_close']}
                        ]
                    })
                
                games_list.append(game_dict)
            
            # Run predictions
            predictions_df = make_predictions(games_list, model, league=league)
            
            # Add results to predictions
            for idx, pred in predictions_df.iterrows():
                game_result = games_df[games_df['game_id'] == pred['game_id']].iloc[0]
                
                # Determine winner
                if game_result['home_score'] > game_result['away_score']:
                    result = 'home'
                elif game_result['away_score'] > game_result['home_score']:
                    result = 'away'
                else:
                    result = 'draw'
                
                predictions_df.at[idx, 'home_score'] = int(game_result['home_score'])
                predictions_df.at[idx, 'away_score'] = int(game_result['away_score'])
                predictions_df.at[idx, 'result'] = result
                predictions_df.at[idx, 'result_updated_at'] = datetime.now(timezone.utc).isoformat()
            
            all_predictions.append(predictions_df)
            print(f"  ✓ Generated {len(predictions_df)} predictions with results")
            
        except Exception as e:
            print(f"  ✗ Error processing {league}: {e}")
            continue
    
    if not all_predictions:
        print("\n\nNo predictions generated")
        return
    
    # Combine all predictions
    combined = pd.concat(all_predictions, ignore_index=True)
    print(f"\n\nTotal restored predictions: {len(combined)}")
    
    # Load existing predictions and merge
    ensemble_path = Path("data/forward_test/ensemble/predictions_master.parquet")
    if ensemble_path.exists():
        existing = pd.read_parquet(ensemble_path)
        print(f"Existing predictions: {len(existing)}")
        
        # Remove any existing entries for these game_ids (avoid duplicates)
        restored_game_ids = set(combined['game_id'].values)
        existing = existing[~existing['game_id'].isin(restored_game_ids)]
        
        # Combine
        final = pd.concat([existing, combined], ignore_index=True)
        print(f"Final total: {len(final)}")
    else:
        final = combined
    
    # Normalize datetime columns for parquet compatibility
    datetime_cols = ["commence_time", "result_updated_at"]
    for column in datetime_cols:
        if column in final.columns:
            final[column] = pd.to_datetime(final[column], errors="coerce", utc=True)
            
    # Drop predicted_at if it exists to ensure dashboard uses commence_time for versioning
    if "predicted_at" in final.columns:
        final = final.drop(columns=["predicted_at"])
    
    # Save
    final.to_parquet(ensemble_path, index=False)
    print(f"\n✓ Saved to {ensemble_path}")
    
    # Summary
    completed_count = final['result'].notna().sum()
    print(f"\n\nFinal Summary:")
    print(f"  Total predictions: {len(final)}")
    print(f"  Completed games: {completed_count}")
    print(f"  Restored: {len(combined)}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--days', type=int, default=30, help='Number of days to look back')
    args = parser.parse_args()
    
    restore_completed_predictions(lookback_days=args.days)
