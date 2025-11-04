# Forward Testing Quick Start Guide

## ✅ Setup Complete!

Your forward testing system is ready. All prerequisites are in place:
- ✅ Model trained and ready
- ✅ API key configured
- ✅ Directories created
- ✅ Dependencies installed

## 🚀 Daily Workflow

### Option 1: All-in-One (Recommended)

Run everything at once:
```powershell
poetry run python scripts/setup_forward_test.py
```

Or use the PowerShell script:
```powershell
.\scripts\forward_test_daily.ps1
```

### Option 2: Step-by-Step

#### 1. Before Games (Make Predictions)
```bash
poetry run python -m src.models.forward_test predict --dotenv .env
```

This will:
- Fetch current NBA games with odds
- Make predictions using your trained model
- Show recommendations (bets with edge >= 0.06)
- Save predictions to `data/forward_test/predictions_master.parquet`

**When to run:** Before games start (typically 6-7 PM EST for NBA)

#### 2. After Games (Update Results)
```bash
poetry run python -m src.models.forward_test update
```

This will:
- Query database for completed game results
- Update predictions with actual outcomes
- Mark games as completed

**When to run:** After games finish (next morning)

#### 3. View Performance (Generate Report)
```bash
poetry run python -m src.models.forward_test report
```

This shows:
- Total predictions made
- Completed games tracked
- Win rate and ROI
- Performance metrics

**When to run:** Weekly or whenever you want to check performance

## 📊 Understanding the Output

### Predictions Output
When you run `predict`, you'll see:
```
=== RECOMMENDATIONS (Edge >= 0.06) ===
  LAL vs GSW: Home edge=12.3%, Pred=68.5%, ML=-150
  BOS vs MIA: Away edge=8.1%, Pred=58.2%, ML=+120
```

This means:
- **Edge**: How much better your model thinks the bet is vs market
- **Pred**: Your model's predicted win probability
- **ML**: The moneyline (betting odds)

### Report Output
When you run `report`, you'll see:
```
=== FORWARD TESTING REPORT ===
Total Predictions: 45
Completed Games: 30
Recommended Bets: 18
Wins: 12
Losses: 6
Win Rate: 66.7%
ROI: 15.2%
```

This shows how your forward testing bets performed.

## 🎯 Building Confidence

To build confidence before live betting:

1. **Track 50-100 games minimum**
   - This gives you statistical significance
   - Run `predict` and `update` daily

2. **Monitor win rate**
   - Should match or exceed your predicted probabilities
   - If consistently lower, investigate

3. **Check ROI consistency**
   - Should remain positive
   - Compare to backtest performance

4. **Compare to backtest**
   - Forward test should match backtest (~66% win rate, positive ROI)
   - If significantly worse, something may be wrong

## 📝 Example Weekly Schedule

### Monday-Friday
- **6 PM EST**: Run `predict` (before games)
- **Next morning**: Run `update` (after games finish)

### Sunday
- Run `report` to review weekly performance

## 🔧 Troubleshooting

### "No live games found"
- Check if NBA games are scheduled today
- Verify your API key is valid
- Check The Odds API rate limits

### "No results found" when updating
- Ensure game results are loaded into database
- Run: `poetry run python -m src.data.ingest_results_nba --seasons 2024`
- Wait a few hours after games finish

### "Model not found"
- Run: `poetry run python -m src.models.train --league NBA --model-type gradient_boosting --calibration sigmoid --seasons 2009 2017`

### Predictions seem wrong
- Check that moneylines are being fetched correctly
- Verify model file exists and is loaded
- Review logs for errors

## 📈 Next Steps

Once you have 50-100 forward tested games with positive results:

1. **Start small**: Begin with 1-5% of intended bankroll
2. **Scale gradually**: Increase stakes only after proven success
3. **Continue monitoring**: Track performance and adjust if needed
4. **Set limits**: Maximum bet size, maximum daily loss

## 📚 Additional Resources

- Full documentation: `README_FORWARD_TESTING.md`
- Validation guide: `docs/confidence_validation.md`
- Setup script: `scripts/setup_forward_test.py`

## 🎉 Ready to Start!

Everything is set up. Start forward testing today:

```bash
poetry run python -m src.models.forward_test predict --dotenv .env
```

Good luck! 🏀


