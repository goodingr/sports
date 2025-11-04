# Forward Testing Guide

## Overview

Forward testing allows you to validate your model's performance on live games without actually betting. This is the best way to gain confidence in your model before risking real money.

## Quick Start

### 1. Make Predictions on Live Games

Fetch current/upcoming games and make predictions (choose NBA/NFL/CFB):

```bash
poetry run python -m src.models.forward_test predict --league NBA
poetry run python -m src.models.forward_test predict --league NFL
poetry run python -m src.models.forward_test predict --league CFB
```

This will:
- Fetch live odds from The Odds API (`basketball_nba`, `americanfootball_nfl`, or `americanfootball_ncaaf`)
- Make predictions using the corresponding trained model
- Show recommendations (bets with edge >= 0.06)
- Save predictions to `data/forward_test/predictions_master.parquet`

### 2. Update Results After Games Finish

After games complete, update the predictions with actual results:

```bash
poetry run python -m src.models.forward_test update --league NBA
poetry run python -m src.models.forward_test update --league NFL
poetry run python -m src.models.forward_test update --league CFB
```

This queries the database for game results and updates your predictions for the selected league.

### 3. View Performance Report

Generate a performance report showing how your forward testing bets performed:

```bash
poetry run python -m src.models.forward_test report --league NBA
poetry run python -m src.models.forward_test report --league NFL
poetry run python -m src.models.forward_test report --league CFB
```

## Workflow

### Daily Routine

1. **Morning/Before Games**: Run `predict --league <NBA|NFL|CFB>` to get recommendations for the day
2. **After Games Finish**: Run `update --league <NBA|NFL|CFB>` to record results
3. **Weekly**: Run `report --league <NBA|NFL|CFB>` to track performance

### Example Schedule

```bash
# Before games (e.g., 6 PM EST)
poetry run python -m src.models.forward_test predict --league NBA

# After games finish (next morning)
poetry run python -m src.models.forward_test update --league NBA

# Check weekly performance (repeat for NFL / CFB as needed)
poetry run python -m src.models.forward_test report --league NBA
```

## What Gets Tracked

For each game:
- Game ID, teams, commence time
- Predicted probabilities (home/away)
- Market implied probabilities
- Calculated edges
- Actual results (when available)
- Betting simulation results

## Performance Metrics

The report shows:
- Total predictions made
- Completed games tracked
- Recommended bets (edge >= 0.06)
- Win rate
- ROI from simulated betting
- Mean predicted probability vs actual win rate

## Building Confidence

To build confidence in your model:

1. **Track 50-100 games minimum** before considering live betting
2. **Monitor win rate** - should match or exceed predicted probabilities
3. **Check ROI consistency** - should remain positive
4. **Compare to backtest** - forward test should match backtest performance

## Troubleshooting

### No Games Found
- Check if games are scheduled today for the league you selected
- Verify The Odds API key is set in `.env`
- Check API rate limits

### No Results Found
- Ensure game results are loaded into the database
- NBA: `poetry run python -m src.data.ingest_results_nba --seasons 2025`
- NFL: `poetry run python -m src.data.ingest_results --seasons 2024`
- CFB: `poetry run python -m src.data.ingest_results_cfb --seasons 2024 --season-type regular` (requires `CFBD_API_KEY`)
- Wait a few minutes after games finish for results to be published (especially for CFB box scores)

### Predictions Seem Wrong
- Verify model file exists for the selected league (`models/nba_*.pkl`, `models/nfl_*.pkl`, `models/cfb_*.pkl`)
- Check that required features are available (moneylines, spreads, totals)
- Review prediction logs for errors and ensure odds API results map to known team codes

## Next Steps

Once you have 50-100 forward tested games with positive results:
- Start with small stakes (1-5% of bankroll)
- Gradually scale up as confidence builds
- Continue monitoring performance


