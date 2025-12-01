# Database Schema Documentation

> **Last Updated**: November 30, 2025  
> **Database**: SQLite (`data/betting.db`)  
> **Total Tables**: 20  
> **Total Records**: ~1.5 million

---

## Table of Contents

1. [Overview](#overview)
2. [Entity Relationship Diagram](#entity-relationship-diagram)
3. [Core Reference Tables](#core-reference-tables)
4. [Game & Odds Data](#game--odds-data)
5. [Model & Predictions](#model--predictions)
6. [Data Sources & Ingestion](#data-sources--ingestion)
7. [Operational Metadata](#operational-metadata)
8. [Common Queries](#common-queries)

---

## Overview

The sports betting database is organized into several functional areas:

- **Core Reference**: Sports, teams, and books (sportsbooks)
- **Game Data**: Games, results, player stats
- **Odds Data**: Snapshots and odds across multiple sportsbooks
- **Modeling**: Models, predictions, recommendations
- **Data Management**: Source tracking, ingestion runs, file registry

### Key Statistics

| Table | Row Count | Purpose |
|-------|-----------|---------|
| `game_results` | 246,952 | Historical game scores and closing lines |
| `games` | 248,459 | Scheduled and completed games |
| `odds` | 370,260 | Odds from various sportsbooks |
| `model_predictions` | 578,551 | Model-generated win probabilities |
| `teams` | 1,502 | All teams across all sports |
| `books` | 31 | Sportsbooks providing odds |
| `sports` | 10 | Supported leagues |

---

## Entity Relationship Diagram

```mermaid
erDiagram
    sports ||--o{ teams : contains
    sports ||--o{ games : schedules
    sports ||--o{ odds_snapshots : tracks
    
    teams ||--o{ games : "plays_home"
    teams ||--o{ games : "plays_away"
    teams ||--o{ model_predictions : predicts
    teams ||--o{ recommendations : recommends
    teams ||--o{ player_stats : has
    
    games ||--o| game_results : "has_result"
    games ||--o{ odds : "has_odds"
    games ||--o{ model_predictions : predicts
    games ||--o{ recommendations : recommends
    games ||--o{ player_stats : contains
    
    books ||--o{ odds : provides
    
    odds_snapshots ||--o{ odds : captures
    odds_snapshots ||--o{ recommendations : basis
    
    models ||--o{ model_predictions : generates
    models ||--o{ recommendations : makes
    
    data_sources ||--o{ source_runs : executes
    data_sources ||--o{ source_files : produces
    
    sports {
        int sport_id PK
        text name
        text league UK
        text default_market
    }
    
    teams {
        int team_id PK
        int sport_id FK
        text code
        text name
    }
    
    games {
        text game_id PK
        int sport_id FK
        int home_team_id FK
        int away_team_id FK
        text start_time_utc
        int season
        text status
    }
    
    game_results {
        text game_id PK_FK
        int home_score
        int away_score
        real home_moneyline_close
        real total_close
    }
    
    odds_snapshots {
        text snapshot_id PK
        text fetched_at_utc
        int sport_id FK
    }
    
    odds {
        text snapshot_id PK_FK
        text game_id PK_FK
        int book_id PK_FK
        text market PK
        text outcome PK
        real price_american
        real line
    }
    
    books {
        int book_id PK
        text name UK
        text region
    }
    
    models {
        text model_id PK
        text model_type
        text league
        text trained_at
    }
    
    model_predictions {
        int prediction_id PK
        text model_id FK
        text game_id FK
        int team_id FK
        real probability
        real edge
    }
    
    recommendations {
        int recommendation_id PK
        text model_id FK
        text game_id FK
        int team_id FK
        real edge
        text status
    }
```

---

## Core Reference Tables

### `sports`
**Purpose**: Defines supported leagues/sports

| Column | Type | Description |
|--------|------|-------------|
| `sport_id` | INTEGER | Primary key (auto-increment) |
| `name` | TEXT | Display name (e.g., "National Basketball Association") |
| `league` | TEXT | League code (e.g., "NBA") **[UNIQUE]** |
| `default_market` | TEXT | Default betting market (e.g., "h2h", "totals") |
| `created_at` | TEXT | ISO 8601 timestamp |

**Row Count**: 10  
**Supported Leagues**: NFL, NBA, NCAAB, NHL, CFB, EPL, LALIGA, BUNDESLIGA, SERIEA, LIGUE1

**Example Query**:
```sql
SELECT * FROM sports WHERE league IN ('NFL', 'NBA');
```

---

### `teams`
**Purpose**: Master list of teams across all sports

| Column | Type | Description |
|--------|------|-------------|
| `team_id` | INTEGER | Primary key (auto-increment) |
| `sport_id` | INTEGER | Foreign key to `sports` |
| `code` | TEXT | Short code (e.g., "PHI", "LAL") |
| `name` | TEXT | Full team name (e.g., "Philadelphia Eagles") |
| `active_from` | INTEGER | Season when team became active |
| `active_to` | INTEGER | Season when team became inactive (NULL if still active) |

**Row Count**: 1,502  
**Unique Constraint**: `(sport_id, code)`

**Example Query**:
```sql
-- Get all active NFL teams
SELECT t.code, t.name 
FROM teams t
JOIN sports s ON t.sport_id = s.sport_id
WHERE s.league = 'NFL' AND t.active_to IS NULL;
```

---

### `books`
**Purpose**: Sportsbook/bookmaker registry

| Column | Type | Description |
|--------|------|-------------|
| `book_id` | INTEGER | Primary key (auto-increment) |
| `name` | TEXT | Sportsbook name (e.g., "FanDuel") **[UNIQUE]** |
| `region` | TEXT | Operating region (e.g., "us", "uk") |

**Row Count**: 31  
**Examples**: FanDuel, DraftKings, BetMGM, Pinnacle, Bet365

---

## Game & Odds Data

### `games`
**Purpose**: Core game schedule and metadata

| Column | Type | Description |
|--------|------|-------------|
| `game_id` | TEXT | Primary key (unique game identifier) |
| `sport_id` | INTEGER | Foreign key to `sports` |
| `season` | INTEGER | Season year (e.g., 2024) |
| `game_type` | TEXT | Type (e.g., "REG", "POST", "PRE") |
| `week` | INTEGER | Week number (NULL for soccer) |
| `start_time_utc` | TEXT | Game start time (ISO 8601 UTC) |
| `home_team_id` | INTEGER | Foreign key to `teams` |
| `away_team_id` | INTEGER | Foreign key to `teams` |
| `venue` | TEXT | Venue name or location |
| `status` | TEXT | Status ("scheduled", "final", "in_progress") |
| `gsis_id` | TEXT | NFL: GSIS identifier |
| `pfr_id` | TEXT | Pro Football Reference ID |
| `odds_api_id` | TEXT | The Odds API identifier |
| `is_neutral` | INTEGER | 1 if neutral site, 0 otherwise |

**Row Count**: 248,459  
**Indexes**: 
- `idx_games_sport_season_week` on `(sport_id, season, week)`
- `idx_games_odds_api` on `odds_api_id`

**Example Query**:
```sql
-- Get upcoming NFL games
SELECT 
    g.game_id,
    g.start_time_utc,
    ht.name as home_team,
    at.name as away_team
FROM games g
JOIN teams ht ON g.home_team_id = ht.team_id
JOIN teams at ON g.away_team_id = at.team_id
JOIN sports s ON g.sport_id = s.sport_id
WHERE s.league = 'NFL' 
  AND g.status = 'scheduled'
  AND datetime(g.start_time_utc) > datetime('now')
ORDER BY g.start_time_utc
LIMIT 10;
```

---

### `game_results`
**Purpose**: Final scores and closing lines

| Column | Type | Description |
|--------|------|-------------|
| `game_id` | TEXT | Primary key, foreign key to `games` |
| `home_score` | INTEGER | Final home team score |
| `away_score` | INTEGER | Final away team score |
| `home_moneyline_close` | REAL | Closing moneyline for home (American odds) |
| `away_moneyline_close` | REAL | Closing moneyline for away (American odds) |
| `spread_close` | REAL | Closing spread line |
| `total_close` | REAL | Closing total (over/under) line |
| `source_version` | TEXT | Data source version |
| `tr_pick` | TEXT | TeamRankings pick (if available) |
| `tr_total_line` | REAL | TeamRankings total line |
| `tr_confidence` | REAL | TeamRankings confidence (0-100) |
| `tr_odds` | REAL | TeamRankings odds |
| `tr_model_pick` | TEXT | TeamRankings model pick |
| `tr_model_prob` | REAL | TeamRankings model probability |
| `tr_retrieved_at` | TEXT | When TeamRankings data was fetched |

**Row Count**: 246,952

**Example Query**:
```sql
-- Get results with closing lines for completed games
SELECT 
    gr.*,
    g.start_time_utc,
    ht.name as home_team,
    at.name as away_team
FROM game_results gr
JOIN games g ON gr.game_id = g.game_id
JOIN teams ht ON g.home_team_id = ht.team_id
JOIN teams at ON g.away_team_id = at.team_id
WHERE gr.home_score IS NOT NULL
ORDER BY g.start_time_utc DESC
LIMIT 10;
```

---

### `odds_snapshots`
**Purpose**: Tracks when odds were captured

| Column | Type | Description |
|--------|------|-------------|
| `snapshot_id` | TEXT | Primary key (UUID) |
| `fetched_at_utc` | TEXT | When odds were fetched (ISO 8601 UTC) |
| `sport_id` | INTEGER | Foreign key to `sports` |
| `source` | TEXT | Data source (e.g., "odds_api", "espn") |
| `raw_path` | TEXT | Path to raw data file |

**Row Count**: 410

---

### `odds`
**Purpose**: Point-in-time odds from sportsbooks

| Column | Type | Description |
|--------|------|-------------|
| `snapshot_id` | TEXT | Foreign key to `odds_snapshots` **[PK]** |
| `game_id` | TEXT | Foreign key to `games` **[PK]** |
| `book_id` | INTEGER | Foreign key to `books` **[PK]** |
| `market` | TEXT | Market type ("h2h", "spreads", "totals") **[PK]** |
| `outcome` | TEXT | Outcome type ("home", "away", "Over", "Under") **[PK]** |
| `price_american` | REAL | American odds (e.g., -110, +150) |
| `price_decimal` | REAL | Decimal odds (e.g., 1.91, 2.50) |
| `implied_prob` | REAL | Implied probability (0-1) |
| `line` | REAL | Line value (spread or total) |

**Row Count**: 370,260  
**Composite Primary Key**: `(snapshot_id, book_id, game_id, market, outcome)`  
**Index**: `idx_odds_game_market` on `(game_id, market)`

**Market Types**:
- `h2h`: Head-to-head (moneyline)
- `spreads`: Point spread
- `totals`: Over/Under

**Example Query**:
```sql
-- Get latest odds for a specific game
WITH latest_snapshot AS (
    SELECT snapshot_id, fetched_at_utc
    FROM odds_snapshots
    WHERE sport_id = (SELECT sport_id FROM sports WHERE league = 'NFL')
    ORDER BY fetched_at_utc DESC
    LIMIT 1
)
SELECT 
    b.name as bookmaker,
    o.market,
    o.outcome,
    o.price_american,
    o.line
FROM odds o
JOIN books b ON o.book_id = b.book_id
JOIN latest_snapshot ls ON o.snapshot_id = ls.snapshot_id
WHERE o.game_id = 'specific_game_id_here'
ORDER BY o.market, o.outcome, b.name;
```

---

### `player_stats`
**Purpose**: Player-level statistics (primarily NBA)

| Column | Type | Description |
|--------|------|-------------|
| `stat_id` | INTEGER | Primary key (auto-increment) |
| `game_id` | TEXT | Foreign key to `games` |
| `team_id` | INTEGER | Foreign key to `teams` |
| `player_id` | INTEGER | Player identifier |
| `player_name` | TEXT | Player name |
| `min` | REAL | Minutes played |
| `pts` | INTEGER | Points scored |
| `reb` | INTEGER | Rebounds |
| `ast` | INTEGER | Assists |
| `stl` | INTEGER | Steals |
| `blk` | INTEGER | Blocks |
| `tov` | INTEGER | Turnovers |
| `pf` | INTEGER | Personal fouls |
| `plus_minus` | INTEGER | Plus/minus stat |

**Row Count**: 26,188  
**Indexes**: 
- `idx_player_stats_game` on `game_id`
- `idx_player_stats_player` on `player_id`

---

## Model & Predictions

### `models`
**Purpose**: Tracks trained machine learning models

| Column | Type | Description |
|--------|------|-------------|
| `model_id` | TEXT | Primary key (unique model identifier) |
| `trained_at` | TEXT | Training timestamp (ISO 8601) |
| `model_type` | TEXT | Type ("ensemble", "random_forest", "gradient_boosting") |
| `calibration` | TEXT | Calibration method ("sigmoid", "isotonic") |
| `seasons_start` | INTEGER | First season in training data |
| `seasons_end` | INTEGER | Last season in training data |
| `features` | TEXT | JSON array of feature names |
| `dataset_hash` | TEXT | Hash of training dataset |
| `metrics_json` | TEXT | JSON object of performance metrics |
| `artifact_path` | TEXT | Path to serialized model file |
| `predictions_path` | TEXT | Path to predictions parquet file |
| `league` | TEXT | League code (e.g., "NFL", "NBA") |

**Row Count**: 683

**Example Query**:
```sql
-- Get latest model for each league
SELECT league, model_type, trained_at, metrics_json
FROM models
WHERE model_id IN (
    SELECT model_id
    FROM models m1
    WHERE trained_at = (
        SELECT MAX(trained_at)
        FROM models m2
        WHERE m2.league = m1.league AND m2.model_type = m1.model_type
    )
)
ORDER BY league, model_type;
```

---

### `model_predictions`
**Purpose**: Model-generated win probabilities

| Column | Type | Description |
|--------|------|-------------|
| `prediction_id` | INTEGER | Primary key (auto-increment) |
| `model_id` | TEXT | Foreign key to `models` |
| `game_id` | TEXT | Foreign key to `games` |
| `team_id` | INTEGER | Foreign key to `teams` |
| `probability` | REAL | Predicted win probability (0-1) |
| `market_prob` | REAL | Market-implied probability (from odds) |
| `edge` | REAL | Betting edge (probability - market_prob) |
| `created_at` | TEXT | Prediction timestamp |

**Row Count**: 578,551  
**Unique Constraint**: `(model_id, game_id, team_id)`

**Example Query**:
```sql
-- Get predictions with positive edge
SELECT 
    mp.model_id,
    mp.game_id,
    t.name as team,
    mp.probability,
    mp.market_prob,
    mp.edge
FROM model_predictions mp
JOIN teams t ON mp.team_id = t.team_id
WHERE mp.edge > 0.06
ORDER BY mp.edge DESC
LIMIT 20;
```

---

### `recommendations`
**Purpose**: Recommended bets based on model predictions

| Column | Type | Description |
|--------|------|-------------|
| `recommendation_id` | INTEGER | Primary key (auto-increment) |
| `model_id` | TEXT | Foreign key to `models` |
| `snapshot_id` | TEXT | Foreign key to `odds_snapshots` |
| `game_id` | TEXT | Foreign key to `games` |
| `team_id` | INTEGER | Foreign key to `teams` |
| `recommended_at` | TEXT | Recommendation timestamp |
| `edge` | REAL | Betting edge |
| `kelly_fraction` | REAL | Kelly criterion fraction |
| `stake` | REAL | Recommended stake amount |
| `status` | TEXT | Status ("pending", "won", "lost", "push") |

**Row Count**: 2,745

**Example Query**:
```sql
-- Get pending recommendations
SELECT 
    r.*,
    m.league,
    t.name as team,
    g.start_time_utc
FROM recommendations r
JOIN models m ON r.model_id = m.model_id
JOIN teams t ON r.team_id = t.team_id
JOIN games g ON r.game_id = g.game_id
WHERE r.status = 'pending'
  AND datetime(g.start_time_utc) > datetime('now')
ORDER BY r.edge DESC;
```

---

## Data Sources & Ingestion

### `data_sources`
**Purpose**: Registry of external data sources

| Column | Type | Description |
|--------|------|-------------|
| `source_id` | INTEGER | Primary key (auto-increment) |
| `source_key` | TEXT | Unique key (e.g., "espn_odds_nba") **[UNIQUE]** |
| `name` | TEXT | Display name |
| `league` | TEXT | League code (NULL for multi-league sources) |
| `category` | TEXT | Category ("odds", "schedules", "injuries", etc.) |
| `uri` | TEXT | Base URI or endpoint |
| `enabled` | INTEGER | 1 if enabled, 0 if disabled |
| `default_frequency` | TEXT | Default refresh frequency ("hourly", "daily") |
| `created_at` | TEXT | Creation timestamp |

**Row Count**: 33

---

### `source_runs`
**Purpose**: Tracks data source execution history

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | TEXT | Primary key (UUID) |
| `source_id` | INTEGER | Foreign key to `data_sources` |
| `started_at` | TEXT | Run start timestamp |
| `finished_at` | TEXT | Run completion timestamp |
| `status` | TEXT | Status ("success", "failure", "partial") |
| `message` | TEXT | Status message or error details |
| `records_ingested` | INTEGER | Number of records ingested |
| `raw_path` | TEXT | Path to raw data file |

**Row Count**: 939  
**Index**: `idx_source_runs_source_id` on `(source_id, started_at DESC)`

---

### `source_files`
**Purpose**: Tracks captured data files

| Column | Type | Description |
|--------|------|-------------|
| `file_id` | INTEGER | Primary key (auto-increment) |
| `source_id` | INTEGER | Foreign key to `data_sources` |
| `captured_at` | TEXT | Capture timestamp |
| `storage_path` | TEXT | File path in storage |
| `hash` | TEXT | File hash (for deduplication) |
| `season` | INTEGER | Season year |
| `metadata_json` | TEXT | JSON metadata |

**Row Count**: 2,222  
**Index**: `idx_source_files_source_season` on `(source_id, season, captured_at DESC)`

---

## Operational Metadata

### `injury_reports`
**Purpose**: Player injury status from multiple sources

| Column | Type | Description |
|--------|------|-------------|
| `injury_id` | INTEGER | Primary key (auto-increment) |
| `league` | TEXT | League code |
| `sport_id` | INTEGER | Foreign key to `sports` |
| `team_id` | INTEGER | Foreign key to `teams` |
| `team_code` | TEXT | Team code |
| `season` | INTEGER | Season year |
| `week` | INTEGER | Week number |
| `player_name` | TEXT | Player name |
| `position` | TEXT | Player position |
| `status` | TEXT | Injury status ("Out", "Doubtful", "Questionable", "Probable") |
| `practice_status` | TEXT | Practice participation |
| `report_date` | TEXT | Report date |
| `game_date` | TEXT | Upcoming game date |
| `detail` | TEXT | Injury details |
| `source_key` | TEXT | Data source |
| `created_at` | TEXT | Record creation timestamp |

**Row Count**: 0 (table exists but currently unpopulated)  
**Indexes**: 
- `idx_injury_reports_league_date` on `(league, report_date DESC)`
- `idx_injury_reports_team` on `(league, team_code, report_date DESC)`

---

### `ingestion_runs`
**Purpose**: High-level ingestion process tracking

| Column | Type | Description |
|--------|------|-------------|
| `run_id` | TEXT | Primary key (UUID) |
| `process` | TEXT | Process name (e.g., "pipeline", "odds_fetch") |
| `started_at` | TEXT | Start timestamp |
| `finished_at` | TEXT | Completion timestamp |
| `status` | TEXT | Status ("running", "success", "failure") |
| `notes` | TEXT | Additional notes |

**Row Count**: 0

---

## Common Queries

### Get Upcoming Games with Latest Odds

```sql
WITH latest_snapshot AS (
    SELECT snapshot_id, fetched_at_utc, sport_id
    FROM odds_snapshots
    ORDER BY fetched_at_utc DESC
    LIMIT 1
)
SELECT 
    g.game_id,
    s.league,
    ht.name as home_team,
    at.name as away_team,
    g.start_time_utc,
    GROUP_CONCAT(DISTINCT b.name) as sportsbooks_count
FROM games g
JOIN sports s ON g.sport_id = s.sport_id
JOIN teams ht ON g.home_team_id = ht.team_id
JOIN teams at ON g.away_team_id = at.team_id
JOIN odds o ON g.game_id = o.game_id
JOIN books b ON o.book_id = b.book_id
JOIN latest_snapshot ls ON o.snapshot_id = ls.snapshot_id AND g.sport_id = ls.sport_id
WHERE datetime(g.start_time_utc) > datetime('now')
GROUP BY g.game_id, s.league, ht.name, at.name, g.start_time_utc
ORDER BY g.start_time_utc
LIMIT 50;
```

### Calculate Model Performance

```sql
SELECT 
    m.league,
    m.model_type,
    COUNT(*) as total_predictions,
    AVG(mp.edge) as avg_edge,
    COUNT(CASE WHEN mp.edge > 0 THEN 1 END) as positive_edge_count,
    COUNT(CASE WHEN mp.edge > 0.06 THEN 1 END) as high_edge_count
FROM model_predictions mp
JOIN models m ON mp.model_id = m.model_id
GROUP BY m.league, m.model_type
ORDER BY m.league, m.model_type;
```

### Find Best Betting Opportunities

```sql
SELECT 
    s.league,
    ht.name as home_team,
    at.name as away_team,
    g.start_time_utc,
    t.name as pick,
    mp.probability as model_prob,
    mp.market_prob,
    mp.edge,
    o.price_american as best_odds
FROM model_predictions mp
JOIN models m ON mp.model_id = m.model_id
JOIN games g ON mp.game_id = g.game_id
JOIN sports s ON g.sport_id = s.sport_id
JOIN teams ht ON g.home_team_id = ht.team_id
JOIN teams at ON g.away_team_id = at.team_id
JOIN teams t ON mp.team_id = t.team_id
JOIN odds o ON mp.game_id = o.game_id
WHERE mp.edge >= 0.06
  AND datetime(g.start_time_utc) > datetime('now')
  AND o.market = 'h2h'
  AND (
    (o.outcome = 'home' AND t.team_id = g.home_team_id) OR
    (o.outcome = 'away' AND t.team_id = g.away_team_id)
  )
ORDER BY mp.edge DESC
LIMIT 20;
```

---

## Database Maintenance

### Indexes

The database includes several indexes for query performance:

- `idx_games_sport_season_week`: Fast filtering by league and season
- `idx_games_odds_api`: Quick lookup by Odds API ID
- `idx_odds_game_market`: Efficient odds queries by game and market
- `idx_source_runs_source_id`: Source execution history
- `idx_source_files_source_season`: File registry by season
- `idx_injury_reports_league_date`: Injury report queries
- `idx_player_stats_game`: Player stats by game
- `idx_player_stats_player`: Player stats by player

### Vacuum & Optimize

```sql
-- Rebuild database file to reclaim space
VACUUM;

-- Update internal statistics for query optimizer
ANALYZE;
```

### Foreign Key Constraints

Foreign key enforcement is **enabled** in the database. All relationship integrity is maintained automatically.

```sql
-- Verify FK enforcement
PRAGMA foreign_keys;
-- Returns: 1 (enabled)
```

---

## Schema Evolution

The schema is managed via `src/db/schema.sql`. To apply schema changes:

```bash
# Initialize or update schema
poetry run python -m src.db.init_db
```

Schema migrations are **idempotent** - tables are created with `IF NOT EXISTS`, allowing safe re-runs.

---

## Related Documentation

- [Data Sources](../docs/data-sources.md) - External data source details
- [Storage Layout](../docs/storage-layout.md) - File system organization
- [Model Training](../README.md#feature-engineering--modeling) - Model pipeline

---

*Generated from schema analysis on 2025-11-30*
