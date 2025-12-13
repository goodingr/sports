-- Core reference tables
CREATE TABLE IF NOT EXISTS sports (
    sport_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    league TEXT NOT NULL UNIQUE,
    default_market TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS teams (
    team_id INTEGER PRIMARY KEY AUTOINCREMENT,
    sport_id INTEGER NOT NULL REFERENCES sports(sport_id),
    code TEXT NOT NULL,
    name TEXT NOT NULL,
    active_from INTEGER,
    active_to INTEGER,
    UNIQUE (sport_id, code)
);

CREATE TABLE IF NOT EXISTS games (
    game_id TEXT PRIMARY KEY,
    sport_id INTEGER NOT NULL REFERENCES sports(sport_id),
    season INTEGER,
    game_type TEXT,
    week INTEGER,
    start_time_utc TEXT,
    home_team_id INTEGER NOT NULL REFERENCES teams(team_id),
    away_team_id INTEGER NOT NULL REFERENCES teams(team_id),
    venue TEXT,
    status TEXT DEFAULT 'scheduled',
    gsis_id TEXT,
    pfr_id TEXT,
    odds_api_id TEXT,
    espn_id TEXT
);

CREATE INDEX IF NOT EXISTS idx_games_sport_season_week
    ON games (sport_id, season, week);

CREATE INDEX IF NOT EXISTS idx_games_odds_api
    ON games (odds_api_id);

-- Odds and market data
CREATE TABLE IF NOT EXISTS books (
    book_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    region TEXT
);

CREATE TABLE IF NOT EXISTS odds_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    fetched_at_utc TEXT NOT NULL,
    sport_id INTEGER NOT NULL REFERENCES sports(sport_id),
    source TEXT,
    raw_path TEXT
);

CREATE TABLE IF NOT EXISTS odds (
    snapshot_id TEXT NOT NULL REFERENCES odds_snapshots(snapshot_id),
    game_id TEXT NOT NULL REFERENCES games(game_id),
    book_id INTEGER NOT NULL REFERENCES books(book_id),
    market TEXT NOT NULL,
    outcome TEXT NOT NULL,
    price_american REAL,
    price_decimal REAL,
    implied_prob REAL,
    line REAL,
    PRIMARY KEY (snapshot_id, book_id, game_id, market, outcome)
);

CREATE INDEX IF NOT EXISTS idx_odds_game_market
    ON odds (game_id, market);

-- Results and features
CREATE TABLE IF NOT EXISTS game_results (
    game_id TEXT PRIMARY KEY REFERENCES games(game_id),
    home_score INTEGER,
    away_score INTEGER,
    home_moneyline_close REAL,
    away_moneyline_close REAL,
    spread_close REAL,
    total_close REAL,
    source_version TEXT,
    tr_pick TEXT,
    tr_total_line REAL,
    tr_confidence REAL,
    tr_odds REAL,
    tr_model_pick TEXT,
    tr_model_prob REAL,
    tr_retrieved_at TEXT
);

CREATE TABLE IF NOT EXISTS team_features (
    feature_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id TEXT NOT NULL REFERENCES games(game_id),
    team_id INTEGER NOT NULL REFERENCES teams(team_id),
    feature_set TEXT NOT NULL,
    feature_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (game_id, team_id, feature_set)
);

CREATE TABLE IF NOT EXISTS model_input (
    input_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id TEXT NOT NULL REFERENCES games(game_id),
    team_id INTEGER NOT NULL REFERENCES teams(team_id),
    feature_set TEXT NOT NULL,
    is_home INTEGER NOT NULL,
    moneyline REAL,
    implied_prob REAL,
    spread_line REAL,
    total_line REAL,
    win INTEGER,
    dataset_hash TEXT,
    UNIQUE (game_id, team_id, feature_set)
);

-- Modeling artefacts
CREATE TABLE IF NOT EXISTS models (
    model_id TEXT PRIMARY KEY,
    trained_at TEXT NOT NULL,
    model_type TEXT NOT NULL,
    calibration TEXT NOT NULL,
    seasons_start INTEGER,
    seasons_end INTEGER,
    features TEXT,
    dataset_hash TEXT,
    metrics_json TEXT,
    artifact_path TEXT,
    predictions_path TEXT,
    league TEXT
);

CREATE TABLE IF NOT EXISTS model_predictions (
    prediction_id INTEGER PRIMARY KEY AUTOINCREMENT,
    model_id TEXT NOT NULL REFERENCES models(model_id),
    game_id TEXT NOT NULL REFERENCES games(game_id),
    team_id INTEGER NOT NULL REFERENCES teams(team_id),
    probability REAL NOT NULL,
    market_prob REAL,
    edge REAL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (model_id, game_id, team_id)
);

CREATE TABLE IF NOT EXISTS predictions (
    prediction_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id TEXT NOT NULL REFERENCES games(game_id),
    model_type TEXT NOT NULL,
    predicted_at TEXT NOT NULL,
    home_prob REAL,
    away_prob REAL,
    home_moneyline REAL,
    away_moneyline REAL,
    home_edge REAL,
    away_edge REAL,
    home_implied_prob REAL,
    away_implied_prob REAL,
    total_line REAL,
    over_prob REAL,
    under_prob REAL,
    over_moneyline REAL,
    under_moneyline REAL,
    over_edge REAL,
    under_edge REAL,
    over_implied_prob REAL,
    under_implied_prob REAL,
    UNIQUE (game_id, model_type, predicted_at)
);

CREATE INDEX IF NOT EXISTS idx_predictions_game_model
    ON predictions (game_id, model_type);

CREATE TABLE IF NOT EXISTS recommendations (
    recommendation_id INTEGER PRIMARY KEY AUTOINCREMENT,
    model_id TEXT NOT NULL REFERENCES models(model_id),
    snapshot_id TEXT REFERENCES odds_snapshots(snapshot_id),
    game_id TEXT NOT NULL REFERENCES games(game_id),
    team_id INTEGER NOT NULL REFERENCES teams(team_id),
    recommended_at TEXT NOT NULL DEFAULT (datetime('now')),
    edge REAL,
    kelly_fraction REAL,
    stake REAL,
    status TEXT DEFAULT 'pending'
);

-- Operational metadata
CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id TEXT PRIMARY KEY,
    process TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    notes TEXT
);

CREATE TABLE IF NOT EXISTS data_files (
    file_id INTEGER PRIMARY KEY AUTOINCREMENT,
    path TEXT NOT NULL,
    file_type TEXT NOT NULL,
    loaded_at TEXT NOT NULL DEFAULT (datetime('now')),
    row_count INTEGER,
    hash TEXT
);

-- External data source registry
CREATE TABLE IF NOT EXISTS data_sources (
    source_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_key TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    league TEXT,
    category TEXT NOT NULL,
    uri TEXT,
    enabled INTEGER NOT NULL DEFAULT 1,
    default_frequency TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS source_runs (
    run_id TEXT PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES data_sources(source_id),
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    message TEXT,
    records_ingested INTEGER,
    raw_path TEXT
);

CREATE INDEX IF NOT EXISTS idx_source_runs_source_id
    ON source_runs (source_id, started_at DESC);

CREATE TABLE IF NOT EXISTS source_files (
    file_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id INTEGER NOT NULL REFERENCES data_sources(source_id),
    captured_at TEXT NOT NULL,
    storage_path TEXT NOT NULL,
    hash TEXT,
    season INTEGER,
    metadata_json TEXT,
    UNIQUE (source_id, storage_path)
);

CREATE INDEX IF NOT EXISTS idx_source_files_source_season
    ON source_files (source_id, season, captured_at DESC);

-- Injury reports aggregated from multiple sources
CREATE TABLE IF NOT EXISTS injury_reports (
    injury_id INTEGER PRIMARY KEY AUTOINCREMENT,
    league TEXT NOT NULL,
    sport_id INTEGER REFERENCES sports(sport_id),
    team_id INTEGER REFERENCES teams(team_id),
    team_code TEXT,
    season INTEGER,
    week INTEGER,
    player_name TEXT NOT NULL,
    position TEXT,
    status TEXT,
    practice_status TEXT,
    report_date TEXT,
    game_date TEXT,
    detail TEXT,
    source_key TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE (league, player_name, report_date, source_key)
);

CREATE INDEX IF NOT EXISTS idx_injury_reports_league_date
    ON injury_reports (league, report_date DESC);

CREATE INDEX IF NOT EXISTS idx_injury_reports_team
    ON injury_reports (league, team_code, report_date DESC);

CREATE TABLE IF NOT EXISTS player_stats (
    stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_id TEXT NOT NULL REFERENCES games(game_id),
    team_id INTEGER NOT NULL REFERENCES teams(team_id),
    player_id INTEGER NOT NULL,
    player_name TEXT,
    min REAL,
    pts INTEGER,
    reb INTEGER,
    ast INTEGER,
    stl INTEGER,
    blk INTEGER,
    tov INTEGER,
    pf INTEGER,
    plus_minus INTEGER,
    UNIQUE (game_id, player_id)
);

CREATE INDEX IF NOT EXISTS idx_player_stats_game ON player_stats (game_id);
CREATE INDEX IF NOT EXISTS idx_player_stats_player ON player_stats (player_id);


CREATE VIEW IF NOT EXISTS odds_movement_view AS
SELECT 
    o.game_id,
    s.fetched_at_utc as timestamp,
    b.name as bookmaker,
    -- Moneyline
    MAX(CASE WHEN o.market = 'h2h' AND o.outcome = 'home' THEN o.price_american END) as home_moneyline,
    MAX(CASE WHEN o.market = 'h2h' AND o.outcome = 'away' THEN o.price_american END) as away_moneyline,
    -- Spread
    MAX(CASE WHEN o.market = 'spreads' AND o.outcome = 'home' THEN o.line END) as home_spread,
    MAX(CASE WHEN o.market = 'spreads' AND o.outcome = 'home' THEN o.price_american END) as home_spread_odds,
    MAX(CASE WHEN o.market = 'spreads' AND o.outcome = 'away' THEN o.line END) as away_spread,
    MAX(CASE WHEN o.market = 'spreads' AND o.outcome = 'away' THEN o.price_american END) as away_spread_odds,
    -- Total
    MAX(CASE WHEN o.market = 'totals' AND o.outcome = 'Over' THEN o.line END) as total_line,
    MAX(CASE WHEN o.market = 'totals' AND o.outcome = 'Over' THEN o.price_american END) as over_odds,
    MAX(CASE WHEN o.market = 'totals' AND o.outcome = 'Under' THEN o.price_american END) as under_odds
FROM odds o
JOIN odds_snapshots s ON o.snapshot_id = s.snapshot_id
JOIN books b ON o.book_id = b.book_id
GROUP BY o.game_id, s.snapshot_id, b.book_id;
