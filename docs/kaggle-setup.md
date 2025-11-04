# Kaggle Dataset Setup

The Kaggle NBA historical betting dataset provides a comprehensive source of historical NBA odds and game data.

## Setup Instructions

1. **Get Kaggle API Credentials**:
   - Go to https://www.kaggle.com/account
   - Scroll down to "API" section
   - Click "Create New Token" to download `kaggle.json`

2. **Install Credentials**:
   - Place `kaggle.json` in one of these locations:
     - Windows: `C:\Users\<username>\.kaggle\kaggle.json`
     - Linux/Mac: `~/.kaggle/kaggle.json`
   - Set permissions (Linux/Mac): `chmod 600 ~/.kaggle/kaggle.json`

3. **Download Dataset**:
   ```bash
   # Option 1: Download via Kaggle API (requires credentials)
   poetry run python -m src.data.ingest_sources --source kaggle_nba_betting
   
   # Option 2: Download manually from Kaggle and load from local file
   poetry run python -c "from src.data.sources.kaggle_nba import ingest; ingest(csv_path='path/to/downloaded/file.csv')"
   ```

## Dataset Information

- **Dataset**: `ehallmar/nba-historical-stats-and-betting-data`
- **URL**: https://www.kaggle.com/datasets/ehallmar/nba-historical-stats-and-betting-data
- **Content**: Historical NBA game stats and betting lines (moneyline, spread, totals)

## Usage

Once loaded, the data will be automatically integrated into the database and available for:
- Dataset building (`moneyline_dataset.py`)
- Model training
- Bet selection

The loader automatically:
- Normalizes column names
- Generates game_ids if missing
- Extracts seasons from dates
- Loads into SQLite database via `load_schedules()`

