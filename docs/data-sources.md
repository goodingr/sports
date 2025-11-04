# Data Sources

Centralized scraping targets are defined in `config/sources.yml`. Each entry specifies the source key, handler module, collection frequency, and storage location. Wave 1 focuses on structured feeds from nflfastR, Pro Football Reference, TeamRankings, Covers, Basketball-Reference, nba_api, and related sites.

Run the orchestrator with:

```
poetry run python -m src.data.ingest_sources --list      # inspect sources
poetry run python -m src.data.ingest_sources             # execute all enabled
poetry run python -m src.data.ingest_sources --source covers_nfl --season-start 2023 --season-end 2024
```

## Odds (Free Tier)

- **Provider**: The Odds API (https://theoddsapi.com/)
- **Coverage**: NFL moneyline, spreads, totals, and many global sports. Free tier allows 500 monthly requests and three requests per minute.
- **Access**:
  - Sign up for a free account and create an API key.
  - Store the key in `.env` as `ODDS_API_KEY`.
  - Use the `americanfootball_nfl` market code when requesting NFL odds.
- **Endpoints used**:
  - `/v4/sports/americanfootball_nfl/odds` for upcoming games.
  - `/v4/sports/americanfootball_nfl/odds-history` for historical prices by date/time.
- **Notes**: Respect rate limits, request in UTC, and cache responses under `data/raw/odds/` with ISO date folders.
  - Use `src/data/backfill_odds.py` to pull historical snapshots via the `/odds-history` endpoint (set `--step-days` to thin requests when covering many seasons).
- **Provider**: ESPN Scoreboard API (`https://site.api.espn.com/apis/site/v2/sports/<sport>/<league>/scoreboard`)
  - Scraped via `src.data.sources.espn_odds` for NFL (`espn_odds_nfl`), NBA (`espn_odds_nba`), and FBS college football (`espn_odds_cfb`).
  - Captures open/close moneyline, spread, and total values published by ESPN Bet.
  - Outputs `odds.csv` under `data/raw/sources/<league>/espn_odds/<timestamp>/`.
  - Note: ESPN API only provides current/upcoming odds, not historical data.

- **Provider**: OddsShark (`https://www.oddsshark.com/nba/odds`)
  - Scraped via `src.data.sources.oddsshark` for NBA (`oddsshark_nba`).
  - Uses Selenium for JavaScript rendering.
  - Supports historical odds via date parameter: `ingest(date="2023-10-24")`.
  - Historical URL format: `https://www.oddsshark.com/nba/scores/{YYYY-MM-DD}`.
  - Outputs `odds.csv` under `data/raw/sources/nba/oddsshark/<timestamp>/`.

- **Provider**: VegasInsider (`https://www.vegasinsider.com/nba/odds/las-vegas/`)
  - Scraped via `src.data.sources.vegasinsider` for NBA (`vegasinsider_nba`).
  - Uses Selenium for JavaScript rendering.
  - Supports historical odds via date parameter: `ingest(date="2023-10-24")`.
  - Historical URL format: `https://www.vegasinsider.com/nba/scoreboard/?date={YYYY-MM-DD}`.
  - Outputs `odds.csv` under `data/raw/sources/nba/vegasinsider/<timestamp>/`.

- **Provider**: Action Network (`https://www.actionnetwork.com/nba/odds`)
  - Scraped via `src.data.sources.action_network` for both NFL and NBA (`action_network_nfl`, `action_network_nba`).
  - Uses Selenium to bypass AWS WAF protection.
  - Captures moneyline odds and line movement data.
  - Outputs `moneyline.csv` under `data/raw/sources/<league>/action_network/<timestamp>/`.

- **Provider**: Covers (`https://www.covers.com/sport/basketball/nba/matchups`)
  - Scraped via `src.data.sources.covers` for both NFL and NBA (`covers_nfl`, `covers_nba`).
  - Extracts `__NEXT_DATA__` JSON payload from Next.js pages.
  - Supports historical odds via date parameter: `ingest_nba(date="2023-10-24")`.
  - Historical URL format: `https://www.covers.com/sport/basketball/nba/matchups/{YYYY-MM-DD}`.
  - Outputs JSON and HTML under `data/raw/sources/<league>/covers/<timestamp>/`.

## Game Results & Statistics

- **Provider**: `nfl_data_py` Python library (https://github.com/nflverse/nfl_data_py)
- **Coverage**: NFL play-by-play, schedules, betting lines, and team stats dating back to 1999.
- **Access**:
  - Install `nfl-data-py` (MIT licensed) via Poetry or pip.
  - No API key required; the package reads from the public `nflverse` data releases.
- **Data pulls**:
  - Example usage:

```
import nfl_data_py as nfl
schedules = nfl.import_schedules([2020, 2021, 2022])
betting = nfl.import_betting_data([2020, 2021, 2022])
```

- **Notes**: Normalize timezone to UTC, join with odds snapshots using game IDs.

### NBA Results

- **Provider**: `nba_api` Python package (https://github.com/swar/nba_api)
- **Coverage**: NBA game logs by team; used to reconstruct home/away scores and schedules.
- **Access**:
  - Install `nba_api` via Poetry/pip.
  - Use `leaguegamefinder.LeagueGameFinder` with `season_type_nullable` (`"Regular Season"` or `"Playoffs"`).
- **Pipeline integration**:
  - Script `src/data/ingest_results_nba.py` converts team-level logs into game-level parquet (`data/raw/results/schedules_nba_<start>_<end>.parquet`).
  - Games/results load into SQLite via `load_schedules(..., league="NBA")`.
- **Advanced Team Metrics (NBA)**: `src.data.sources.nba_team_metrics` queries `nba_api.stats` (`TeamEstimatedMetrics`) for league-wide offensive/defensive ratings and pace. Data lands in `data/raw/sources/nba/team_metrics/<timestamp>/team_metrics.parquet` and feeds the dataset builder.

### College Football (FBS) Results

- **Provider**: CollegeFootballData API (https://api.collegefootballdata.com)
- **Coverage**: FBS schedules, scores, and game metadata. Requires a free API key.
- **Access**:
  - Sign up for an API key and store it as `CFBD_API_KEY` in `.env`.
  - Use `src/data/ingest_results_cfb.py` to fetch season schedules and final scores (writes to `data/raw/results/schedules_cfb_<start>_<end>.parquet`).
- **Notes**:
  - The ingestion script only records scores when the API marks games as completed (Win/Loss values populated).
  - Results load into SQLite via `load_schedules(..., league="CFB")`, enabling the generic moneyline dataset builder to service CFB models.

## Contextual Team Metrics (Optional)

- **Provider**: Pro Football Reference scraping via `sportsipy` library (https://github.com/roclark/sportsipy)
- **Usage**: Lagged team stats (EPA, yards per play). Optional for first iteration; include once feature engineering begins.
- **Caveats**: Rate-limit your requests to avoid blocking; cache scraped tables under `data/raw/teams/`.
- **NFL Season Metrics**: `src.data.sources.nfl_team_metrics` aggregates `nflfastR` play-by-play to produce season-level EPA/success metrics per team (stored under `data/raw/sources/nfl/team_metrics/`).

## Injuries & News

- **nflverse injuries** (`src.data.sources.nflverse_injuries:ingest`): downloads the official `injuries.csv` release from the nflverse data repository, archives raw/Parquet copies, and loads normalized rows into the `injury_reports` table.
- **NBA live injuries** (`src.data.sources.nba_injuries:ingest`): consumes `https://cdn.nba.com/static/json/liveData/injuries/injuries_00.json` for league-wide statuses and persists them via the same table.
- Injury counts (`injuries_out`, `injuries_questionable`, etc.) are merged into the moneyline dataset builders for both leagues.
- Additional qualitative feeds (e.g., power rankings, beat writer summaries) can slot into `config/sources.yml` using the same orchestration flow.

