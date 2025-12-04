import pandas as pd

# Load predictions
df = pd.read_parquet("data/forward_test/predictions_master.parquet")
nba = df[df['league'] == 'NBA'].copy()
nba['predicted_at'] = pd.to_datetime(nba['predicted_at'])

# Split by model version
cutoff = pd.to_datetime('2025-11-20 02:00:00+00:00')
old = nba[nba['predicted_at'] < cutoff].copy()
new = nba[nba['predicted_at'] >= cutoff].copy()

print("CHECKING FOR OVERLAPPING GAMES")
print("=" * 70)

# Create game identifiers
old['game_key'] = old['home_team'] + '_vs_' + old['away_team']
new['game_key'] = new['home_team'] + '_vs_' + new['away_team']

# Find overlaps
overlapping_games = set(old['game_key']) & set(new['game_key'])

if overlapping_games:
    print(f"\nFound {len(overlapping_games)} games predicted by BOTH models:")
    print("-" * 70)
    
    for game_key in sorted(overlapping_games):
        old_pred = old[old['game_key'] == game_key].iloc[0]
        new_pred = new[new['game_key'] == game_key].iloc[0]
        
        print(f"\n{game_key.replace('_vs_', ' vs ')}")
        print(f"  Old Model:")
        print(f"    Home: {old_pred['home_predicted_prob']:.1%} prob, {old_pred['home_edge']*100:.1f}% edge")
        print(f"    Away: {old_pred['away_predicted_prob']:.1%} prob, {old_pred['away_edge']*100:.1f}% edge")
        print(f"  New Model:")
        print(f"    Home: {new_pred['home_predicted_prob']:.1%} prob, {new_pred['home_edge']*100:.1f}% edge")
        print(f"    Away: {new_pred['away_predicted_prob']:.1%} prob, {new_pred['away_edge']*100:.1f}% edge")
        print(f"  Changes:")
        home_prob_change = (new_pred['home_predicted_prob'] - old_pred['home_predicted_prob']) * 100
        home_edge_change = (new_pred['home_edge'] - old_pred['home_edge']) * 100
        print(f"    Home Prob: {home_prob_change:+.1f} percentage points")
        print(f"    Home Edge: {home_edge_change:+.1f} percentage points")
else:
    print("\nNo overlapping games found.")
    print("\nThis is expected because:")
    print("  - The old model made predictions on games from earlier dates")
    print("  - The new model is making predictions on NEW upcoming games")
    print("  - These are different sets of games")
    
    print("\n" + "=" * 70)
    print("WHAT THIS MEANS:")
    print("=" * 70)
    print("The enhanced model is being used for FUTURE predictions.")
    print("To compare probabilities, we would need to:")
    print("  1. Wait for games to complete")
    print("  2. Compare actual outcomes vs predicted probabilities")
    print("  3. Measure which model was more accurate")
    
    print("\nOr we could:")
    print("  - Retrain BOTH models on the same historical data")
    print("  - Compare their predictions on a holdout test set")
    print("  - This would show probability calibration differences")
    
    print("\n" + "=" * 70)
    print("CURRENT COMPARISON:")
    print("=" * 70)
    print(f"Old Model: {len(old)} predictions on games from {old['commence_time'].min()} to {old['commence_time'].max()}")
    print(f"New Model: {len(new)} predictions on games from {new['commence_time'].min()} to {new['commence_time'].max()}")
    
    # Show sample from each
    print("\nSample Old Model Prediction:")
    sample_old = old.iloc[0]
    print(f"  {sample_old['home_team']} vs {sample_old['away_team']}")
    print(f"  Home: {sample_old['home_predicted_prob']:.1%} prob")
    
    print("\nSample New Model Prediction:")
    sample_new = new.iloc[0]
    print(f"  {sample_new['home_team']} vs {sample_new['away_team']}")
    print(f"  Home: {sample_new['home_predicted_prob']:.1%} prob")
