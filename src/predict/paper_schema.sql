CREATE TABLE IF NOT EXISTS paper_trials (
    trial_id TEXT PRIMARY KEY,
    registered_at TEXT NOT NULL,
    specification TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS paper_decisions (
    trial_id TEXT NOT NULL REFERENCES paper_trials(trial_id),
    game_id TEXT NOT NULL,
    home_team_id INTEGER NOT NULL,
    away_team_id INTEGER NOT NULL,
    start_time_utc TEXT NOT NULL,
    captured_at TEXT NOT NULL,
    predicted_at TEXT NOT NULL,
    odds_source TEXT NOT NULL,
    snapshot_id TEXT NOT NULL,
    odds_fetched_at TEXT NOT NULL,
    book_id INTEGER NOT NULL,
    book TEXT NOT NULL,
    side TEXT NOT NULL CHECK (side IN ('over', 'under')),
    line REAL NOT NULL,
    price REAL NOT NULL,
    opposite_price REAL NOT NULL,
    model_probability REAL NOT NULL,
    market_probability REAL NOT NULL,
    edge REAL NOT NULL,
    PRIMARY KEY (trial_id, game_id)
);

CREATE TABLE IF NOT EXISTS paper_settlements (
    trial_id TEXT NOT NULL,
    game_id TEXT NOT NULL,
    settled_at TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('win', 'loss', 'push', 'void')),
    profit_units REAL NOT NULL,
    home_score INTEGER,
    away_score INTEGER,
    close_snapshot_id TEXT,
    close_fetched_at TEXT,
    close_line REAL,
    close_price REAL,
    clv_points REAL,
    PRIMARY KEY (trial_id, game_id),
    FOREIGN KEY (trial_id, game_id) REFERENCES paper_decisions(trial_id, game_id)
);
