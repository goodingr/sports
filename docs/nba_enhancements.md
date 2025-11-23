# NBA Model Enhancements (November 2025)

This document details the recent enhancements made to the NBA modeling pipeline to improve prediction accuracy and robustness.

## 1. Advanced Rolling Metrics

We have implemented advanced team performance metrics calculated over rolling windows (3, 5, and 10 games) to capture recent form more effectively than season-long averages.

### New Features
- **Offensive Rating**: Points scored per 100 possessions.
- **Defensive Rating**: Points allowed per 100 possessions.
- **Net Rating**: Point differential per 100 possessions.
- **Pace**: Possessions per 48 minutes.
- **Possessions**: Estimated possessions based on field goal attempts, free throws, and turnovers.

### Implementation
- **Source**: `src/data/sources/nba_rolling_metrics.py`
- **Integration**: `src/features/dataset/nba.py` merges these metrics into the training dataset.
- **Windows**: 3-game (short-term), 5-game (medium-term), and 10-game (long-term) averages.

## 2. Robust Injury Data Collection

To address gaps in the official ESPN API, we implemented a web scraper as a fallback mechanism.

### Implementation
- **Primary Source**: ESPN API (often missing injury details).
- **Fallback Source**: `src/data/sources/nba_injuries_espn_scraper.py` scrapes the [ESPN NBA Injuries page](https://www.espn.com/nba/injuries).
- **Logic**: If the API returns no injuries, the system automatically triggers the scraper to ensure the model has up-to-date availability data.

## 3. Modular Dataset Architecture

The feature engineering pipeline has been refactored for better maintainability and extensibility.

- **New Package**: `src/features/dataset/`
- **League-Specific Modules**:
    - `nba.py`: NBA-specific feature logic (rolling metrics, schedule merging).
    - `nfl.py`: NFL-specific feature logic.
    - `soccer.py`: Soccer-specific feature logic.
    - `shared.py`: Common utilities (timezone handling, data loading).

## Usage

To rebuild the NBA dataset with these new features:

```bash
poetry run python -m src.features.moneyline_dataset --league NBA --seasons 2023 2024 2025
```

To retrain the model:

```bash
poetry run python -m src.models.train --league NBA --model-type lightgbm --calibration sigmoid
```
