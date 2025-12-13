# Automation & Pipeline Guide

## Overview

The sports betting analytics system uses a unified pipeline script (`scripts/pipeline.ps1`) to orchestrate data ingestion, model training, and prediction generation. This pipeline is designed to be run periodically (e.g., hourly) to ensure the dashboard always displays fresh data.

## The Pipeline (`pipeline.ps1`)

The master script `scripts/pipeline.ps1` performs the following steps in order:

1.  **Backup**: Creates a backup of critical data (optional, configurable).
2.  **Ingestion (`ingest_all.ps1`)**:
    *   Fetches latest odds from The Odds API.
    *   Updates schedules and scores.
    *   Ingests advanced stats (NBA rolling metrics, etc.).
3.  **Training (`train.ps1`)**:
    *   *Default*: Skipped (via `-SkipTraining`) for fast updates.
    *   *When Active*: Rebuilds datasets and retrains models (GBM, Ensemble).
4.  **Prediction (`predict.ps1`)**:
    *   Generates probabilities for upcoming games.
    *   Calculates edges against current odds.
    *   Updates the `forward_test` database.

### Usage

```powershell
# Run the full pipeline (Ingest -> Train -> Predict)
# Recommended for daily updates or when model retraining is needed.
.\scripts\pipeline.ps1

# Run fast update (Ingest -> Predict)
# Recommended for hourly updates. Skips model training.
.\scripts\pipeline.ps1 -SkipTraining

# Run without fetching new odds (Uses cached odds)
.\scripts\pipeline.ps1 -SkipOdds -SkipTraining
```

## Manual Component Execution

You can run specific parts of the pipeline manually if needed:

**Ingestion Only:**
```powershell
.\scripts\ingest_all.ps1
```

**Training Only:**
```powershell
.\scripts\train.ps1 -SkipIngestion
```

**Prediction Only:**
```powershell
.\scripts\predict.ps1 -SkipHistory -SkipOdds
```

## Key Components

### Data Ingestion
- **`src/data/ingest_manager.py`**: The **Smart Ingestion Controller**. Checks DB history and decides whether to run a full backfill or a partial update.
- **`src/data/ingest_odds.py`**: Fetches upcoming game odds. Handles **API key rotation** automatically.
- **`src/data/ingest_scores.py`**: Fetches final scores from The Odds API for fast live updates.

### Modeling
- **`src/models/forward_test.py`**: The core prediction engine. Loads trained models and applies them to new odds data.
- **`src/models/train_model.py`**: Handles training logic and model serialization.

## Scheduled Task Setup

To fully automate the system, set up a Windows Task Scheduler task to run the pipeline hourly.

### Option 1: Manual Setup (Recommended)

1.  Open **Task Scheduler** (`taskschd.msc`).
2.  Click **Create Task**.
3.  **General Tab**:
    *   Name: `SportsAnalyticsHourly`
    *   Select "Run whether user is logged on or not" (optional, requires password) or "Run only when user is logged on".
    *   Check "Run with highest privileges".
4.  **Triggers Tab**:
    *   New Trigger -> Begin the task: **On a schedule**.
    *   Daily -> Recur every 1 day.
    *   Check "Repeat task every: **1 hour**" for a duration of **Indefinitely**.
5.  **Actions Tab**:
    *   New Action -> Start a program.
    *   Program/script: `powershell.exe`
    *   Add arguments: `-NoProfile -ExecutionPolicy Bypass -File "C:\Users\Bobby\Desktop\sports\scripts\pipeline.ps1" -SkipTraining`
    *   Start in: `C:\Users\Bobby\Desktop\sports`
6.  **Conditions Tab**:
    *   Uncheck "Start only if the computer is on AC power" (if running on laptop).
    *   Check "Start the task only if the following network connection is available" (Any connection).

### Logs & Monitoring

*   **Logs**: Stored in `logs/pipeline_YYYYMMDD_HHMMSS.log`.
*   **Transcript**: The pipeline uses `Start-Transcript` to capture all output.

To check the latest logs:
```powershell
Get-ChildItem logs\pipeline_*.log | Sort-Object LastWriteTime -Descending | Select-Object -First 1 | Get-Content -Tail 50
```

## Troubleshooting

### API Limits
*   **The Odds API**: Has a request limit. If the pipeline runs too frequently (e.g., every 5 mins), you may hit quotas. Hourly is generally safe.
*   **Residential Proxies**: If specific scrapers (e.g., NBA API) are blocked, refer to `docs/scraping_blockages.md`.

### Database Locks
*   SQLite can occasionally lock if multiple processes access it. The pipeline runs sequentially to avoid this, but avoid running the dashboard/API *write* operations while the pipeline is running.
