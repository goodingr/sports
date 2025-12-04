# Hourly Automation Setup

## Overview

The sports betting analytics system now has a comprehensive hourly automation pipeline that handles:

1. **Data Ingestion**: Fetches odds, schedules, results, and advanced stats from all sources
2. **Dataset Building**: Rebuilds datasets with the latest data
3. **Model Training**: Trains models when datasets are updated
4. **Predictions**: Generates predictions for upcoming games across all leagues

## What Was Fixed

### NBA Issues
- **Problem**: NBA dataset had no historical game data, causing rolling metrics to be empty
- **Solution**: 
  - Ingested NBA schedules and results using `nba_api` for seasons 2018-2024
  - Loaded historical moneyline data from Kaggle dataset (125k+ rows)
  - Fixed datetime comparison issues in rolling metrics loader
  - Rebuilt NBA dataset with proper game dates and 99%+ feature coverage
  - Retrained NBA model with enriched dataset

### Dataset Feature Coverage (After Fixes)
- **NBA**: 434 games, 99% rolling metrics, 100% team ratings
- **NFL**: 3,884 games, 59% EPA metrics, 99% rolling metrics, 100% injuries
- **CFB**: 2,620 games, 99% advanced stats (PPA, success rates)
- **EPL**: 6,080 games (2008-2016), 22% xG metrics (2014+ only from Understat)

## Setup Instructions

### 1. Set Up Hourly Scheduled Task

The system includes a single hourly task that runs the entire pipeline.

**Option A: Run Setup Script (Requires Administrator)**
```powershell
# Run PowerShell as Administrator, then:
cd C:\Users\Bobby\Desktop\sports
.\scripts\setup_hourly_task.ps1
```

**Option B: Manual Task Scheduler Setup**
If the script fails due to permissions, you can manually create the task:

1. Open Task Scheduler (`taskschd.msc`)
2. Click "Create Task" (not "Create Basic Task")
3. **General Tab**:
   - Name: `SportsAnalyticsHourly`
   - Description: `Hourly sports betting analytics pipeline`
   - Run whether user is logged on or not
   - Run with highest privileges (if needed)
4. **Triggers Tab**:
   - New trigger
   - Begin: On a schedule
   - Settings: Daily, recur every 1 day
   - Repeat task every: 1 hour
   - Duration: Indefinitely
5. **Actions Tab**:
   - Action: Start a program
   - Program: `powershell.exe`
   - Arguments: `-NoProfile -ExecutionPolicy Bypass -File "C:\Users\Bobby\Desktop\sports\scripts\run_hourly_pipeline.ps1"`
   - Start in: `C:\Users\Bobby\Desktop\sports`
6. **Conditions Tab**:
   - ☑ Start only if the computer is on AC power: **UNCHECK**
   - ☑ Start the task if the computer is on battery power: **CHECK**
   - ☑ Start only if the following network connection is available: Any connection
7. **Settings Tab**:
   - ☑ Allow task to be run on demand
   - ☑ Run task as soon as possible after a scheduled start is missed
   - If the task is already running: Do not start a new instance

### 2. Test the Pipeline

Before scheduling, test the pipeline manually:

```powershell
# Test the full pipeline
.\scripts\run_hourly_pipeline.ps1

# Check the log
Get-Content logs\hourly_pipeline_*.log -Tail 50
```

### 3. Monitor the Task

```powershell
# View task status
Get-ScheduledTask -TaskName "SportsAnalyticsHourly"

# Run immediately (for testing)
Start-ScheduledTask -TaskName "SportsAnalyticsHourly"

# View recent logs
Get-ChildItem logs\hourly_pipeline_*.log | Sort-Object LastWriteTime -Descending | Select-Object -First 5
```

## What the Pipeline Does Each Hour

### Step 1: Ingest Hourly Data (~5-10 minutes)
- Fetches ESPN odds for all leagues (NFL, NBA, CFB, EPL, La Liga, Bundesliga, Serie A, Ligue 1)
- Pulls NCAA and NHL moneyline snapshots from The Odds API so future games always have fresh prices
- Ingests schedules and results for current season (NBA, NFL, CFB, Soccer)
- After syncing soccer archives it backfills historical totals/scores via\
  `poetry run python scripts/run_external_loader.py --source football-data --leagues premier-league la-liga bundesliga serie-a ligue-1`
- Updates TeamRankings over/under picks in `game_results` via\
  `poetry run python scripts/run_external_loader.py --source teamrankings --leagues NFL NBA CFB`

### Step 2: Ingest Advanced Stats (~2-5 minutes)
- NBA rolling metrics (game-by-game data from nba_api)
- CFB advanced stats (PPA, success rates from CollegeFootballData API)
- MLB advanced stats (batting/pitching from pybaseball)
- Soccer advanced stats (xG, shots from Understat/FBRef)

### Step 3: Rebuild Datasets (~5-10 minutes)
- Rebuilds datasets for NBA, NFL, CFB, EPL using last 6-7 seasons
- Merges all advanced features and calculates rolling metrics

### Step 4: Train Models (~10-20 minutes)
- Trains gradient boosting models for NBA, NFL, CFB, EPL
- Only retrains if dataset has changed significantly

### Step 5: Generate Predictions (~1-2 minutes)
- Generates predictions for all upcoming games across all leagues
- Saves predictions to `data/forward_test/predictions_master.parquet`
- Displays recommendations with edge >= 6%

**Total Time**: ~25-50 minutes per run (depending on data volume)

## Logs and Monitoring

- **Pipeline Logs**: `logs/hourly_pipeline_YYYYMMDD_HHMMSS.log`
- **Prediction Files**: `data/forward_test/predictions_*.parquet`
- **Master Predictions**: `data/forward_test/predictions_master.parquet`

## Troubleshooting

### Task Not Running
```powershell
# Check task status
Get-ScheduledTaskInfo -TaskName "SportsAnalyticsHourly"

# Check Windows Event Log
Get-WinEvent -LogName "Microsoft-Windows-TaskScheduler/Operational" -MaxEvents 20 | 
    Where-Object {$_.Message -like "*SportsAnalyticsHourly*"}
```

### Data Ingestion Failures
- Check `logs/hourly_pipeline_*.log` for error messages
- Verify internet connection
- Some sources may require residential proxies (documented in `docs/scraping_blockages.md`)

### Model Training Failures
- Ensure datasets have sufficient data (check `data/processed/model_input/`)
- Verify feature coverage is >50% for key features

### Prediction Failures
- Ensure models exist in `models/` directory
- Check that game odds are available from The Odds API

## Next Steps

1. **Monitor First Few Runs**: Check logs after the first few hourly runs to ensure everything is working
2. **Adjust Frequency**: If hourly is too frequent, modify the trigger in Task Scheduler
3. **Add More Leagues**: Edit `scripts/run_hourly_pipeline.ps1` to add more leagues as needed
4. **Set Up Residential Proxies**: If scraping is blocked, set up residential proxies for blocked sources

## Soccer Data Note

Soccer advanced stats (xG, shots) are only available from 2014 onwards due to Understat data availability. The EPL dataset uses 2008-2016 data from the provided `database.sqlite`, but only games from 2014+ will have xG metrics (~22% coverage).

To improve coverage:
- Use 2014+ seasons only for soccer training
- Or find alternative sources for 2008-2013 xG data

