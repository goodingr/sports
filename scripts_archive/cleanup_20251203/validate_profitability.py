"""Comprehensive validation of model profitability to build confidence."""
import pandas as pd
import numpy as np
from pathlib import Path
from scipy import stats
from src.models.bet_selector import enrich_predictions
import json

def simulate_betting(df, stake_per_bet=100):
    """Simulate flat betting with given stake."""
    bankroll = 10000
    total_wagered = 0
    bets = []
    
    for _, bet in df.iterrows():
        ml = float(bet['moneyline'])
        if pd.isna(ml) or ml == 0:
            continue
        
        total_wagered += stake_per_bet
        if bet['win'] == 1:
            if ml > 0:
                profit = stake_per_bet * (ml / 100.0)
            else:
                profit = stake_per_bet * (100.0 / (-ml))
        else:
            profit = -stake_per_bet
        
        bankroll += profit
        bets.append({
            'profit': profit,
            'bankroll': bankroll,
            'edge': bet['edge'],
        })
    
    return {
        'starting_bankroll': 10000,
        'ending_bankroll': bankroll,
        'total_wagered': total_wagered,
        'net_profit': bankroll - 10000,
        'roi': (bankroll - 10000) / 10000,
        'bets': pd.DataFrame(bets),
    }

print("Loading predictions...")
df = pd.read_parquet('reports/backtests/nba_gradient_boosting_calibrated_test_predictions.parquet')
enriched = enrich_predictions(df)

print(f"Total predictions: {len(enriched)}")
print(f"\n{'='*60}")
print("VALIDATION 1: STATISTICAL SIGNIFICANCE")
print('='*60)

# Test edge threshold
edge_threshold = 0.06
recs = enriched[enriched['edge'] >= edge_threshold].copy()

if len(recs) > 0:
    # Calculate expected vs actual
    expected_wins = (recs['predicted_prob'] * 100).sum()
    actual_wins = recs['win'].sum() * 100
    total_bets = len(recs) * 100
    
    # Binomial test: is actual win rate significantly different from 50%?
    # (null hypothesis: model is no better than coin flip)
    p_value_coin = stats.binomtest(actual_wins, total_bets, 0.5, alternative='greater').pvalue
    
    # Is actual win rate significantly better than market implied?
    market_win_rate = (recs['implied_prob'] * 100).sum()
    p_value_market = stats.binomtest(actual_wins, total_bets, market_win_rate / total_bets, alternative='greater').pvalue
    
    print(f"\nEdge >= {edge_threshold}:")
    print(f"  Bets: {len(recs)}")
    print(f"  Expected wins (predicted): {expected_wins/100:.1f}")
    print(f"  Actual wins: {actual_wins/100:.1f}")
    print(f"  Market expected wins: {market_win_rate/100:.1f}")
    print(f"\nStatistical Tests:")
    print(f"  vs Coin Flip (50%): p-value = {p_value_coin:.6f} {'*** SIGNIFICANT' if p_value_coin < 0.001 else '** SIGNIFICANT' if p_value_coin < 0.01 else '* SIGNIFICANT' if p_value_coin < 0.05 else 'NOT SIGNIFICANT'}")
    print(f"  vs Market Implied: p-value = {p_value_market:.6f} {'*** SIGNIFICANT' if p_value_market < 0.001 else '** SIGNIFICANT' if p_value_market < 0.01 else '* SIGNIFICANT' if p_value_market < 0.05 else 'NOT SIGNIFICANT'}")

print(f"\n{'='*60}")
print("VALIDATION 2: WALK-FORWARD VALIDATION BY SEASON")
print('='*60)

# Performance by season - try to extract from game_id if season column missing
if 'season' not in enriched.columns and 'game_id' in enriched.columns:
    # Extract season from game_id (NBA_0020900001 -> 2009)
    enriched['season'] = enriched['game_id'].str.extract(r'NBA_00(\d{2})')[0].astype(float)
    enriched['season'] = enriched['season'].apply(lambda x: int(2000 + x) if pd.notna(x) else None)

# Also add season to recs if it exists in enriched
if 'season' in enriched.columns and len(recs) > 0:
    recs = recs.copy()
    recs['season'] = enriched.loc[recs.index, 'season'].values if len(recs) > 0 else None

seasons = sorted([s for s in enriched['season'].unique() if pd.notna(s)]) if 'season' in enriched.columns else []
if len(seasons) > 0:
    seasonal_results = []
    for season in seasons:
        season_recs = recs[recs['season'] == season].copy() if len(recs) > 0 and 'season' in recs.columns else pd.DataFrame()
        if len(season_recs) > 0:
            sim = simulate_betting(season_recs)
            wins = season_recs['win'].sum()
            total = len(season_recs)
            seasonal_results.append({
                'season': season,
                'bets': total,
                'wins': wins,
                'win_rate': wins/total,
                'roi': sim['roi'],
                'profit': sim['net_profit'],
            })
    
    if seasonal_results:
        season_df = pd.DataFrame(seasonal_results)
        print("\nPerformance by Season:")
        print(season_df.to_string(index=False))
        
        # Check consistency
        profitable_seasons = (season_df['roi'] > 0).sum()
        print(f"\nProfitable seasons: {profitable_seasons}/{len(season_df)} ({profitable_seasons/len(season_df):.1%})")
        print(f"Mean ROI: {season_df['roi'].mean():.1%}")
        print(f"Std ROI: {season_df['roi'].std():.1%}")
        
        # Check for trend
        if len(season_df) > 2:
            correlation = season_df['season'].corr(season_df['roi'])
            print(f"Trend (season vs ROI): {correlation:.3f} ({'DECLINING' if correlation < -0.3 else 'IMPROVING' if correlation > 0.3 else 'STABLE'})")

print(f"\n{'='*60}")
print("VALIDATION 3: ROBUSTNESS TO EDGE THRESHOLD")
print('='*60)

# Test different edge thresholds
thresholds = [0.02, 0.03, 0.04, 0.05, 0.06, 0.08, 0.10]
threshold_results = []

for threshold in thresholds:
    subset = enriched[enriched['edge'] >= threshold].copy()
    if len(subset) > 0:
        sim = simulate_betting(subset)
        wins = subset['win'].sum()
        threshold_results.append({
            'threshold': threshold,
            'bets': len(subset),
            'win_rate': wins/len(subset),
            'roi': sim['roi'],
            'profit': sim['net_profit'],
            'total_wagered': sim['total_wagered'],
        })

if threshold_results:
    thresh_df = pd.DataFrame(threshold_results)
    print("\nPerformance by Edge Threshold:")
    print(thresh_df.to_string(index=False))
    
    # Check if profitability is consistent
    all_profitable = (thresh_df['roi'] > 0).all()
    print(f"\nAll thresholds profitable: {all_profitable}")

print(f"\n{'='*60}")
print("VALIDATION 4: OVERFITTING DETECTION")
print('='*60)

# Compare train vs test performance
metrics_path = Path('reports/backtests/nba_gradient_boosting_calibrated_metrics.json')
if metrics_path.exists():
    metrics = json.loads(metrics_path.read_text())
    
    train_acc = metrics['train']['accuracy']
    test_acc = metrics['test']['accuracy']
    train_auc = metrics['train']['roc_auc']
    test_auc = metrics['test']['roc_auc']
    
    print(f"\nTrain vs Test Performance:")
    print(f"  Accuracy: Train={train_acc:.3f}, Test={test_acc:.3f}, Diff={train_acc-test_acc:.3f}")
    print(f"  ROC AUC: Train={train_auc:.3f}, Test={test_auc:.3f}, Diff={train_auc-test_auc:.3f}")
    
    # Check for overfitting (large gap indicates overfitting)
    acc_gap = train_acc - test_acc
    auc_gap = train_auc - test_auc
    
    if acc_gap < 0.05 and auc_gap < 0.05:
        print(f"  Overfitting risk: LOW (gaps < 5%)")
    elif acc_gap < 0.10 and auc_gap < 0.10:
        print(f"  Overfitting risk: MODERATE (gaps 5-10%)")
    else:
        print(f"  Overfitting risk: HIGH (gaps > 10%)")

print(f"\n{'='*60}")
print("VALIDATION 5: CONFIDENCE INTERVALS")
print('='*60)

if len(recs) > 0:
    # Bootstrap confidence intervals for ROI
    n_bootstrap = 1000
    bootstrap_rois = []
    
    for _ in range(n_bootstrap):
        # Resample with replacement
        sample = recs.sample(n=len(recs), replace=True)
        sim = simulate_betting(sample)
        bootstrap_rois.append(sim['roi'])
    
    bootstrap_rois = np.array(bootstrap_rois)
    ci_lower = np.percentile(bootstrap_rois, 2.5)
    ci_upper = np.percentile(bootstrap_rois, 97.5)
    
    print(f"\nBootstrap ROI Confidence Interval (95%):")
    print(f"  Mean ROI: {bootstrap_rois.mean():.1%}")
    print(f"  95% CI: [{ci_lower:.1%}, {ci_upper:.1%}]")
    
    # Check if CI includes zero
    if ci_lower > 0:
        print(f"  Confidence: HIGH (CI > 0)")
    elif ci_upper > 0:
        print(f"  Confidence: MODERATE (CI includes 0)")
    else:
        print(f"  Confidence: LOW (CI < 0)")

print(f"\n{'='*60}")
print("VALIDATION 6: DRAWDOWN & RISK ANALYSIS")
print('='*60)

if len(recs) > 0:
    sim = simulate_betting(recs)
    bets_df = sim['bets']
    
    if len(bets_df) > 0:
        # Calculate running bankroll
        bets_df['cumulative_profit'] = bets_df['profit'].cumsum()
        bets_df['running_max'] = bets_df['cumulative_profit'].cummax()
        bets_df['drawdown'] = bets_df['cumulative_profit'] - bets_df['running_max']
        
        max_drawdown = bets_df['drawdown'].min()
        max_drawdown_pct = max_drawdown / 10000
        
        # Calculate win streaks and loss streaks
        bets_df['is_win'] = bets_df['profit'] > 0
        bets_df['streak'] = (bets_df['is_win'] != bets_df['is_win'].shift()).cumsum()
        win_streaks = bets_df[bets_df['is_win']].groupby('streak').size()
        loss_streaks = bets_df[~bets_df['is_win']].groupby('streak').size()
        
        print(f"\nRisk Metrics:")
        print(f"  Max Drawdown: ${abs(max_drawdown):,.0f} ({abs(max_drawdown_pct):.1%})")
        print(f"  Max Win Streak: {win_streaks.max() if len(win_streaks) > 0 else 0} bets")
        print(f"  Max Loss Streak: {loss_streaks.max() if len(loss_streaks) > 0 else 0} bets")
        print(f"  Profit Factor: {bets_df[bets_df['profit']>0]['profit'].sum() / abs(bets_df[bets_df['profit']<0]['profit'].sum()):.2f}")

print(f"\n{'='*60}")
print("SUMMARY & RECOMMENDATIONS")
print('='*60)

print("""
To gain confidence in these results:

1. STATISTICAL SIGNIFICANCE: Check if p-values show significant edge vs market
2. SEASONAL CONSISTENCY: Verify profitability across multiple seasons
3. THRESHOLD ROBUSTNESS: Ensure profitability holds at different edge thresholds
4. OVERFITTING CHECK: Compare train vs test performance (should be similar)
5. CONFIDENCE INTERVALS: Bootstrap to understand uncertainty
6. RISK ANALYSIS: Understand drawdowns and worst-case scenarios

Next Steps for Higher Confidence:
- Paper trade on live games (forward testing)
- Get historical odds for 2018-2024 for true out-of-sample testing
- Monitor performance over time and adjust if degradation occurs
- Consider using a portion of bankroll initially (risk management)
""")

