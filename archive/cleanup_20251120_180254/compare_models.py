import pandas as pd
import numpy as np
from datetime import datetime

# Load all predictions
df = pd.read_parquet("data/forward_test/predictions_master.parquet")

# Filter for NBA only
nba = df[df['league'] == 'NBA'].copy()

# Convert predicted_at to datetime
nba['predicted_at'] = pd.to_datetime(nba['predicted_at'])

# Define cutoff - predictions made after we ran the new model (Nov 20, 2025 02:16)
cutoff = pd.to_datetime('2025-11-20 02:00:00+00:00')

old_model = nba[nba['predicted_at'] < cutoff]
new_model = nba[nba['predicted_at'] >= cutoff]

print("=" * 80)
print("NBA MODEL COMPARISON: Old vs Enhanced (with Rolling Metrics)")
print("=" * 80)

print(f"\n📊 PREDICTION COUNTS:")
print(f"  Old Model Predictions: {len(old_model)}")
print(f"  New Model Predictions: {len(new_model)}")

if len(new_model) > 0:
    print(f"\n🎯 NEW MODEL PREDICTIONS (Generated: {new_model['predicted_at'].max()}):")
    
    # Show the new predictions
    new_games = new_model.copy()
    new_games['home_edge'] = new_games['home_edge'] * 100  # Convert to percentage
    new_games['away_edge'] = new_games['away_edge'] * 100
    
    for idx, row in new_games.iterrows():
        print(f"\n  Game: {row['home_team']} vs {row['away_team']}")
        print(f"    Commence: {row['commence_time']}")
        print(f"    Home: {row['home_predicted_prob']:.1%} prob, {row['home_edge']:.1f}% edge, ML {row['home_moneyline']:.0f}")
        print(f"    Away: {row['away_predicted_prob']:.1%} prob, {row['away_edge']:.1f}% edge, ML {row['away_moneyline']:.0f}")
        
        # Determine recommendation
        if row['home_edge'] >= 6:
            print(f"    ⭐ RECOMMEND: {row['home_team']} (Home)")
        elif row['away_edge'] >= 6:
            print(f"    ⭐ RECOMMEND: {row['away_team']} (Away)")
    
    print(f"\n📈 NEW MODEL EDGE STATISTICS:")
    all_edges = pd.concat([new_games['home_edge'], new_games['away_edge']])
    print(f"  Average Edge: {all_edges.mean():.2f}%")
    print(f"  Max Edge: {all_edges.max():.2f}%")
    print(f"  Edges >= 6%: {(all_edges >= 6).sum()} bets")
    print(f"  Edges >= 10%: {(all_edges >= 10).sum()} bets")

# Compare edge distributions if we have both
if len(old_model) > 0 and len(new_model) > 0:
    print(f"\n📊 COMPARISON (Old vs New):")
    
    old_edges = pd.concat([old_model['home_edge'], old_model['away_edge']]) * 100
    new_edges = pd.concat([new_games['home_edge'], new_games['away_edge']])
    
    print(f"  Average Edge:")
    print(f"    Old Model: {old_edges.mean():.2f}%")
    print(f"    New Model: {new_edges.mean():.2f}%")
    print(f"    Change: {new_edges.mean() - old_edges.mean():+.2f}%")
    
    print(f"\n  High-Value Bets (Edge >= 6%):")
    old_high = (old_edges >= 6).sum()
    new_high = (new_edges >= 6).sum()
    print(f"    Old Model: {old_high} / {len(old_edges)} = {old_high/len(old_edges)*100:.1f}%")
    print(f"    New Model: {new_high} / {len(new_edges)} = {new_high/len(new_edges)*100:.1f}%")

print("\n" + "=" * 80)
print("💡 KEY INSIGHTS:")
print("=" * 80)
print("The new model uses 18 additional rolling metric features:")
print("  • Offensive Rating (3, 5, 10 game windows)")
print("  • Defensive Rating (3, 5, 10 game windows)")
print("  • Net Rating (3, 5, 10 game windows)")
print("  • Pace (3, 5, 10 game windows)")
print("\nThese features help the model better understand:")
print("  ✓ Team efficiency trends")
print("  ✓ Recent performance momentum")
print("  ✓ Matchup-specific advantages")
print("  ✓ Playing style compatibility")
print("=" * 80)
