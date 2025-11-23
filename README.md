# Sports Betting Analytics

End-to-end platform for multi-league moneyline modeling, evaluation, and recommendation delivery. The stack ingests dozens of public feeds (odds, injuries, advanced stats), maintains a local SQLite warehouse, engineers league-specific features, trains calibrated models, and pushes forward-testing output into dashboards.

---

## Current Focus

| Domain | Status | Notes |
| --- | --- | --- |
| **NFL** | Production | Wave‑1 data full history (1999–present), rolling EPA features, injuries, weather |
| **NBA** | Production | ESPN odds, Kaggle historical closes, rolling metrics, injury summaries ([See Enhancements](docs/nba_enhancements.md)) |
| **CFB (FBS)** | Production | CollegeFootballData API for schedules, advanced stats, lines |
| **Soccer (EPL, La Liga, Bundesliga, Serie A, Ligue 1)** | Production | Football-Data odds, ESPN schedules, Understat team/player aggregates + starting XI form |
| MLB / Others | Paused | Data ingestion templates exist but pipelines disabled until parity with above |

Key outcomes:

- Unified feature builder (`src/features/moneyline_dataset.py`) handles all leagues.
- Soccer seasons are clipped to **2021–2025** with auto‑filled odds (Football-Data) and Understat‑driven lineup metrics.
- Understat match payloads (rosters/shots) are cached per league/season; subsequent hourly runs only fetch new matches.
- `scripts/run_hourly_pipeline.ps1` orchestrates ingestion → advanced stats → dataset build → model refresh → forward-test predictions.

---

## Repository Layout

```
data/
  raw/            # Source snapshots, ESPN pulls, etc.
  processed/      # Feature-ready tables, model inputs
docs/             # Source notes, dashboards, automation instructions
scripts/          # PowerShell automation (hourly pipeline, forward tests, schedulers)
src/
  data/           # Ingestion + normalization
  features/       # Feature engineering
  models/         # Training, evaluation, forward testing
  dashboard/      # Dash web app
tests/            # Pytest suites
```

---

## Getting Started

1. **Install dependencies**
   ```bash
   pip install poetry
   poetry install
   ```

2. **Environment variables**
   - Copy `.env.example` → `.env`.
   - Provide API keys (The Odds API, CollegeFootballData, Football-Data, etc.).

3. **Initialize the SQLite warehouse (optional but recommended)**
   ```bash
   poetry run python -m src.db.init_db
   ```

4. **Bootstrap sources**
   - All historical feeds run once automatically and are skipped on later hourly jobs.
   - Manual refresh: `poetry run python -m src.data.ingest_sources --full-refresh`.

---

## Data Acquisition Cheat Sheet

| Task | Command |
| --- | --- |
| List available structured sources | `poetry run python -m src.data.ingest_sources --list` |
| Run all enabled sources | `poetry run python -m src.data.ingest_sources` |
| League-only (e.g., NFL) | `... --league nfl --season-start 2019 --season-end 2023` |
| Odds snapshots (Odds API) | `poetry run python -m src.data.ingest_odds --sport americanfootball_nfl` (swap sport id) |
| ESPN scoreboard odds | `poetry run python -m src.data.ingest_sources --source espn_odds_nba` |
| Historical schedules/results | `poetry run python -m src.data.ingest_results --seasons 1999 2024` |
| NBA schedules | `poetry run python -m src.data.ingest_results_nba --seasons 2015 2024` |
| CFB schedules | `poetry run python -m src.data.ingest_results_cfb --seasons 2024 --season-type regular` |
| Football-Data odds (soccer) | `poetry run python -m src.data.ingest_football_data --leagues premier-league,serie-a` |
| Understat archives | `poetry run python -m src.data.ingest_understat --leagues EPL,La_liga --seasons 2021,2025` |
| Understat match payloads (lineups/shots) | `poetry run python -m src.data.sources.understat_match_payloads --leagues EPL --seasons 2024`<br>*(caches per league/season; add `--force` to re-download)* |

All scrapers are designed to **skip work when cached files exist**, preventing redundant network hits.

---

## Feature Engineering & Modeling

### Dataset Builder

```
poetry run python -m src.features.moneyline_dataset --league NFL --seasons 2018 2024
poetry run python -m src.features.moneyline_dataset --league EPL --seasons 2021 2025
```

- NFL/NBA/CFB pull directly from the SQLite warehouse (game_results, odds, injuries).
- Soccer merges ESPN schedules, Football-Data odds, and Understat team/player aggregates. Missing moneylines are backfilled from Bet365/Pinnacle decimal odds, and Understat features mirror both team and opponent context.

### Training

```
poetry run python -m src.models.train --league NFL --model-type gradient_boosting --calibration sigmoid
poetry run python -m src.models.train --league NBA --model-type ensemble --calibration sigmoid
poetry run python -m src.models.train --league EPL --model-type gradient_boosting --calibration sigmoid
```

Outputs:
- `models/<league>_<model>_moneyline.pkl`
- `reports/backtests/<league>_*_metrics.json`
- `reports/backtests/<league>_*_test_predictions.parquet`

### Bet Recommendations

```
poetry run python -m src.models.bet_selector --league NFL --edge-threshold 0.06
Adjust the edge threshold per league to trade off volume vs. confidence.

---

## Automation

### Pipeline Script

`scripts/pipeline.ps1` orchestrates the complete workflow:

1. **Step 1**: Ingest odds, injuries, and ESPN schedules
2. **Step 2**: Refresh advanced stats (Football-Data, Understat)
3. **Step 3**: Rebuild datasets for all leagues
4. **Step 4**: Train models (ensemble, random_forest, gradient_boosting)
5. **Step 5**: Generate predictions and update results for all models
6. **Step 6**: Sync results across model types

**Usage:**
```powershell
# Full pipeline
.\scripts\pipeline.ps1

# Skip odds API calls (saves credits, still updates results)
.\scripts\pipeline.ps1 -SkipOdds
```

**Multi-Model Support:**
- The pipeline now trains and generates predictions for three model types:
  - **Ensemble**: Voting classifier (Random Forest + Gradient Boosting + Logistic Regression)
  - **Random Forest**: Standalone random forest classifier
  - **Gradient Boosting**: Standalone XGBoost classifier
- Results are automatically synced across all models via `scripts/copy_results.py`

### Forward Testing & Dashboard

**Starting the Dashboard:**
- UNIX: `./run_dashboard --port 8050`
- PowerShell: `.\run_dashboard.ps1 --port 8050 -DashDebug`
- Direct: `poetry run python -m src.dashboard`

**Dashboard Features:**
- **Multi-Model Selection**: Switch between Ensemble, Random Forest, and Gradient Boosting
- **Moneyline Dashboard**: 
  - Summary metrics (ROI, win rate, profit)
  - Cumulative profit charts (overall, by model, by league)
  - **ROI by League**: Track return on investment for each league
  - Performance analysis and edge distribution
  - Recommended and completed bets tables
- **Over/Under Dashboard**:
  - Totals-specific metrics and charts
  - **ROI by League**: Separate tracking for over/under bets
  - Multi-model comparison
  - Edge analysis for totals
- **Model Comparison**: Side-by-side performance comparison at `/compare`

**Updating Results Without API Calls:**
```powershell
# Update results only (no odds API calls)
poetry run python -m src.models.forward_test update --league NBA --model-type ensemble

# Or use the pipeline with skip flag
.\scripts\pipeline.ps1 -SkipOdds
```

The dashboard reads from `data/forward_test/{model_type}/predictions_master.parquet` and displays:
- Real-time profit/loss tracking
- ROI analysis by league and model
- Adjustable edge threshold slider
- Completed bet history with win/loss records

---

## Soccer-Specific Notes

- **Match mapping**: ESPN/Football-Data games are normalized via `src/data/team_mappings.py` and keyed on `(league, match_date, home_code, away_code)`.
- **Odds coverage**: Bet365 & Pinnacle closes (decimal → implied + American) plus draw prices. Missing moneylines in `game_results` are filled before modeling.
- **Understat features**: Rolling xG/xGA, PPDA, deep entries, xPTS, lineup minutes, per‑starter xG/xA/shot form, and starter continuity share. All metrics are opponent-mirrored (prefixed `opponent_`).
- **Caching**: `src/data/sources/understat_match_payloads.py` skips league/seasons already present in `data/raw/sources/soccer/understat_matches/<timestamp>/match_metadata.parquet`. Use `--force` to refresh.

---

## Troubleshooting

| Symptom | Fix |
| --- | --- |
| Understat match payload step “Skipping … (cached)” but you need a refresh | Add `--force` to the CLI or delete the corresponding run directory under `data/raw/sources/soccer/understat_matches/`. |
| Hourly pipeline slow because of soccer scraping | Ensure `$soccerMatchPayloadSeasons` (scripts/run_hourly_pipeline.ps1) targets only the newest seasons; default is the latest two. |
| `poetry run` commands fail to locate modules | Verify you are in the repo root (`c:\Users\Bobby\Desktop\sports`) and that `poetry shell` is activated or use `poetry run …`. |
| DB missing tables | Re-run `poetry run python -m src.db.init_db` to lay down schema migrations. |

---

## Contributing Workflow

1. Make changes + add tests in `tests/`.
2. Run quick sanity check:
   ```
   poetry run pytest
   poetry run python -m src.features.moneyline_dataset --league NFL --seasons 2023
   ```
3. Update docs (`docs/`, `README.md`) when adding sources or automation.
4. Use descriptive commit messages summarizing ingestion + modeling impacts.

For detailed source documentation, see:
- `docs/data-sources.md`
- `docs/storage-layout.md`
- `docs/dashboard.md`
- `docs/scraping_blockages.md` (rate-limit notes)

Happy modeling!
