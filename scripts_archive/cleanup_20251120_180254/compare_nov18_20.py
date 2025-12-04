import pandas as pd

# Load predictions
df = pd.read_parquet("data/forward_test/predictions_master.parquet")
nba = df[df['league'] == 'NBA'].copy()
nba['commence_time'] = pd.to_datetime(nba['commence_time'])
nba['predicted_at'] = pd.to_datetime(nba['predicted_at'])

# Filter for games that COMMENCED on Nov 18-20
start_date = pd.to_datetime('2025-11-18 00:00:00')
end_date = pd.to_datetime('2025-11-21 00:00:00')
target_games = nba[(nba['commence_time'] >= start_date) & (nba['commence_time'] < end_date)].copy()

print("NBA GAMES FROM NOV 18-20, 2025")
print("=" * 80)
print(f"Found {len(target_games)} games in this timeframe\n")

if len(target_games) > 0:
    # Sort by commence time
    target_games = target_games.sort_values('commence_time')
    
    # Model cutoff
    model_cutoff = pd.to_datetime('2025-11-20 02:00:00+00:00')
    
    for idx, row in target_games.iterrows():
        model_version = "NEW (Enhanced)" if row['predicted_at'] >= model_cutoff else "OLD (Basic)"
        
        print(f"Game: {row['home_team']} vs {row['away_team']}")
        print(f"  Commence: {row['commence_time']}")
        print(f"  Predicted: {row['predicted_at']} ({model_version})")
        print(f"  Home: {row['home_predicted_prob']:.1%} prob, {row['home_edge']*100:+.1f}% edge, ML {row['home_moneyline']:.0f}")
        print(f"  Away: {row['away_predicted_prob']:.1%} prob, {row['away_edge']*100:+.1f}% edge, ML {row['away_moneyline']:.0f}")
        
        # Show result if available
        if pd.notna(row.get('result')):
            print(f"  Result: {row['result']}")
            if pd.notna(row.get('home_score')) and pd.notna(row.get('away_score')):
                print(f"  Score: {row['home_team']} {row['home_score']:.0f} - {row['away_score']:.0f} {row['away_team']}")
        
        print()
    
    # Summary by model
    old_games = target_games[target_games['predicted_at'] < model_cutoff]
    new_games = target_games[target_games['predicted_at'] >= model_cutoff]
    
    print("=" * 80)
    print("SUMMARY")
    print("=" * 80)
    print(f"Games predicted by OLD model: {len(old_games)}")
    print(f"Games predicted by NEW model: {len(new_games)}")
    
    if len(old_games) > 0:
        old_edges = pd.concat([old_games['home_edge'], old_games['away_edge']]) * 100
        print(f"\nOLD Model Stats:")
        print(f"  Avg Edge: {old_edges.mean():.2f}%")
        print(f"  Max Edge: {old_edges.max():.2f}%")
        print(f"  High-value bets (>=6%): {(old_edges >= 6).sum()}")
    
    if len(new_games) > 0:
        new_edges = pd.concat([new_games['home_edge'], new_games['away_edge']]) * 100
        print(f"\nNEW Model Stats:")
        print(f"  Avg Edge: {new_edges.mean():.2f}%")
        print(f"  Max Edge: {new_edges.max():.2f}%")
        print(f"  High-value bets (>=6%): {(new_edges >= 6).sum()}")
    
else:
    print("No games found in Nov 18-20 timeframe")
    print(f"\nAvailable date range: {nba['commence_time'].min()} to {nba['commence_time'].max()}")
