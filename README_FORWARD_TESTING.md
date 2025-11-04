# Forward Testing Guide

## Overview

Forward testing allows you to validate your model's performance on live games without actually betting. This is the best way to gain confidence in your model before risking real money.

## Quick Start

### 1. Make Predictions on Live Games

Fetch current/upcoming NBA games and make predictions:

```bash
poetry run python -m src.models.forward_test predict
```

This will:
- Fetch current NBA games with odds from The Odds API
- Make predictions using your trained model
- Show recommendations (bets with edge >= 0.06)
- Save predictions to `data/forward_test/predictions_master.parquet`

### 2. Update Results After Games Finish

After games complete, update the predictions with actual results:

```bash
poetry run python -m src.models.forward_test update
```

This queries the database for game results and updates your predictions.

### 3. View Performance Report

Generate a performance report showing how your forward testing bets performed:

```bash
poetry run python -m src.models.forward_test report
```

## Workflow

### Daily Routine

1. **Morning/Before Games**: Run `predict` to get recommendations for the day
2. **After Games Finish**: Run `update` to record results
3. **Weekly**: Run `report` to track performance

### Example Schedule

```bash
# Before games (e.g., 6 PM EST)
poetry run python -m src.models.forward_test predict

# After games finish (next morning)
poetry run python -m src.models.forward_test update

# Check weekly performance
poetry run python -m src.models.forward_test report
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
- Check if NBA games are scheduled today
- Verify The Odds API key is set in `.env`
- Check API rate limits

### No Results Found
- Ensure game results are loaded into database
- Run `src.data.ingest_results_nba` to load recent games
- Wait a few hours after games finish for results to be available

### Predictions Seem Wrong
- Verify model file exists: `models/nba_gradient_boosting_calibrated_moneyline.pkl`
- Check that features are available (moneylines, spreads, etc.)
- Review prediction logs for errors

## Next Steps

Once you have 50-100 forward tested games with positive results:
- Start with small stakes (1-5% of bankroll)
- Gradually scale up as confidence builds
- Continue monitoring performance


