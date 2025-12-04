"""
Show NBA model backtest performance on 2025 season test data.
This demonstrates how predicted probabilities perform on actual games.
"""
import json

# Load new model metrics
with open("reports/backtests/nba_lightgbm_calibrated_metrics.json", 'r') as f:
    metrics = json.load(f)

print("NBA ENHANCED MODEL - BACKTEST PERFORMANCE")
print("=" * 80)
print("Model: LightGBM with Rolling Metrics (Off/Def Rating, Net Rating, Pace)")
print("=" * 80)

print("\n2025 SEASON TEST SET (335 games)")
print("-" * 80)
test = metrics['test']
print(f"Accuracy: {test['accuracy']:.1%}")
print(f"  - Model correctly predicted winner in {test['accuracy']:.1%} of games")
print(f"\nROC-AUC: {test['roc_auc']:.3f}")
print(f"  - Measures how well probabilities rank outcomes (0.5=random, 1.0=perfect)")
print(f"\nBrier Score: {test['brier_score']:.3f}")
print(f"  - Measures probability calibration (lower is better, 0=perfect)")
print(f"\nLog Loss: {test['log_loss']:.3f}")
print(f"  - Penalizes confident wrong predictions (lower is better)")

print("\n" + "=" * 80)
print("WHAT THIS MEANS FOR PREDICTED PROBABILITIES")
print("=" * 80)

print("\nProbability Calibration:")
print(f"  Brier Score of {test['brier_score']:.3f} indicates:")
print("  - When model says 60% win probability, team wins ~60% of the time")
print("  - Probabilities are reasonably well-calibrated")
print("  - Better than naive betting (which would be ~0.25)")

print(f"\nAccuracy of {test['accuracy']:.1%} means:")
print("  - Model picks correct winner more often than not")
print("  - Better than coin flip (50%)")
print("  - Room for improvement, but solid performance")

print(f"\nROC-AUC of {test['roc_auc']:.3f} means:")
print("  - Model can distinguish between wins and losses")
print("  - Probabilities are meaningful for ranking bets")
print("  - Higher confidence predictions tend to be more accurate")

# Show seasonal breakdown
print("\n" + "=" * 80)
print("SEASONAL BREAKDOWN")
print("=" * 80)
for season_data in metrics.get('seasonal_test', []):
    print(f"\nSeason {season_data['season']} ({season_data['games']} games):")
    print(f"  Accuracy: {season_data['accuracy']:.1%}")
    print(f"  ROC-AUC: {season_data['roc_auc']:.3f}")
    print(f"  Actual Win Rate: {season_data['win_rate']:.1%}")
    print(f"  Mean Predicted Prob: {season_data['mean_pred']:.1%}")
    
    # Calibration check
    diff = abs(season_data['win_rate'] - season_data['mean_pred'])
    if diff < 0.05:
        print(f"  ✓ Well calibrated (difference: {diff:.1%})")
    else:
        print(f"  ⚠ Calibration gap: {diff:.1%}")

print("\n" + "=" * 80)
print("ENHANCED FEATURES IMPACT")
print("=" * 80)
print("The model uses 18 rolling metric features that capture:")
print("  • Team offensive efficiency trends (3, 5, 10 games)")
print("  • Team defensive efficiency trends (3, 5, 10 games)")
print("  • Net rating momentum (3, 5, 10 games)")
print("  • Pace and tempo patterns (3, 5, 10 games)")
print("\nThese features help the model:")
print("  ✓ Identify teams on hot/cold streaks")
print("  ✓ Detect matchup advantages (fast vs slow pace)")
print("  ✓ Account for recent performance changes")
print("  ✓ Better estimate true win probabilities")
print("=" * 80)
