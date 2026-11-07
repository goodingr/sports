CREATE TABLE IF NOT EXISTS published_releases (
    release_id INTEGER PRIMARY KEY AUTOINCREMENT,
    published_at TEXT NOT NULL,
    payload TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS published_picks (
    pick_id INTEGER PRIMARY KEY AUTOINCREMENT,
    first_release_id INTEGER NOT NULL REFERENCES published_releases(release_id),
    game_id TEXT NOT NULL,
    market TEXT NOT NULL CHECK (market IN ('totals', 'moneyline')),
    home_team_id INTEGER NOT NULL,
    away_team_id INTEGER NOT NULL,
    published_at TEXT NOT NULL,
    payload TEXT NOT NULL,
    UNIQUE (game_id, market)
);

CREATE TABLE IF NOT EXISTS published_settlements (
    pick_id INTEGER PRIMARY KEY REFERENCES published_picks(pick_id),
    settled_at TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK (outcome IN ('win', 'loss', 'push', 'void')),
    profit_units REAL NOT NULL,
    home_score INTEGER,
    away_score INTEGER,
    reason TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS published_settlement_history (
    revision_id INTEGER PRIMARY KEY AUTOINCREMENT,
    pick_id INTEGER NOT NULL REFERENCES published_picks(pick_id),
    settled_at TEXT NOT NULL,
    outcome TEXT NOT NULL,
    profit_units REAL NOT NULL,
    home_score INTEGER,
    away_score INTEGER,
    reason TEXT NOT NULL
);
