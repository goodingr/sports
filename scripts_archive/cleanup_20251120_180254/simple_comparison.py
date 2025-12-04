import pandas as pd

# Load predictions
df = pd.read_parquet("data/forward_test/predictions_master.parquet")
nba = df[df['league'] == 'NBA'].copy()
nba['predicted_at'] = pd.to_datetime(nba['predicted_at'])

# Split by model version (cutoff at Nov 20, 2025 02:00 UTC)
cutoff = pd.to_datetime('2025-11-20 02:00:00+00:00')
old = nba[nba['predicted_at'] < cutoff]
new = nba[nba['predicted_at'] >= cutoff]

print("NBA MODEL COMPARISON")
print("=" * 60)
print(f"Old Model: {len(old)} predictions (before Nov 20, 2025)")
print(f"New Model: {len(new)} predictions (after Nov 20, 2025)")
print()

if len(new) > 0:
    print("NEW MODEL PREDICTIONS:")
    print("-" * 60)
    for idx, row in new.iterrows():
        print(f"{row['home_team']} vs {row['away_team']}")
        print(f"  Home: {row['home_predicted_prob']:.1%} prob, {row['home_edge']*100:.1f}% edge")
        print(f"  Away: {row['away_predicted_prob']:.1%} prob, {row['away_edge']*100:.1f}% edge")
        
        if row['home_edge'] >= 0.06:
            print(f"  >> RECOMMEND: {row['home_team']} (Home)")
        elif row['away_edge'] >= 0.06:
            print(f"  >> RECOMMEND: {row['away_team']} (Away)")
        print()
    
    # Statistics
    new_edges = pd.concat([new['home_edge'], new['away_edge']]) * 100
    print("NEW MODEL STATISTICS:")
    print(f"  Avg Edge: {new_edges.mean():.2f}%")
    print(f"  Max Edge: {new_edges.max():.2f}%")
    print(f"  Bets with edge >= 6%: {(new_edges >= 6).sum()}")
    print()

if len(old) > 0 and len(new) > 0:
    old_edges = pd.concat([old['home_edge'], old['away_edge']]) * 100
    new_edges = pd.concat([new['home_edge'], new['away_edge']]) * 100
    
    print("COMPARISON:")
    print(f"  Old Avg Edge: {old_edges.mean():.2f}%")
    print(f"  New Avg Edge: {new_edges.mean():.2f}%")
    print(f"  Change: {new_edges.mean() - old_edges.mean():+.2f}%")

print()
print("Enhanced features used by new model:")
print("  - Rolling Offensive Rating (3, 5, 10 games)")
print("  - Rolling Defensive Rating (3, 5, 10 games)")
print("  - Rolling Net Rating (3, 5, 10 games)")
print("  - Rolling Pace (3, 5, 10 games)")
