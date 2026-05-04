from __future__ import annotations

import json
import sqlite3

import pandas as pd

from src.features import soccer_features
from src.features.betting_model_input import (
    build_feature_coverage_report,
    build_moneyline_model_input,
    build_moneyline_side_model_input,
    build_totals_model_input,
)


def _create_market_db(path):
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE sports (
                sport_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                league TEXT NOT NULL,
                default_market TEXT NOT NULL
            );
            CREATE TABLE teams (
                team_id INTEGER PRIMARY KEY,
                sport_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                name TEXT NOT NULL
            );
            CREATE TABLE games (
                game_id TEXT PRIMARY KEY,
                sport_id INTEGER NOT NULL,
                start_time_utc TEXT NOT NULL,
                home_team_id INTEGER NOT NULL,
                away_team_id INTEGER NOT NULL,
                status TEXT
            );
            CREATE TABLE game_results (
                game_id TEXT PRIMARY KEY,
                home_score INTEGER,
                away_score INTEGER,
                home_moneyline_close REAL,
                away_moneyline_close REAL,
                total_close REAL
            );
            CREATE TABLE books (
                book_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );
            CREATE TABLE odds_snapshots (
                snapshot_id TEXT PRIMARY KEY,
                fetched_at_utc TEXT NOT NULL,
                sport_id INTEGER NOT NULL
            );
            CREATE TABLE odds (
                snapshot_id TEXT NOT NULL,
                game_id TEXT NOT NULL,
                book_id INTEGER NOT NULL,
                market TEXT NOT NULL,
                outcome TEXT NOT NULL,
                price_american REAL,
                price_decimal REAL,
                implied_prob REAL,
                line REAL
            );
            CREATE TABLE team_features (
                feature_id INTEGER PRIMARY KEY,
                game_id TEXT NOT NULL,
                team_id INTEGER NOT NULL,
                feature_set TEXT NOT NULL,
                feature_json TEXT NOT NULL,
                created_at TEXT
            );
            CREATE TABLE injury_reports (
                injury_id INTEGER PRIMARY KEY,
                league TEXT NOT NULL,
                sport_id INTEGER,
                team_id INTEGER,
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
                created_at TEXT
            );
            CREATE TABLE player_stats (
                stat_id INTEGER PRIMARY KEY,
                game_id TEXT NOT NULL,
                team_id INTEGER NOT NULL,
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
                plus_minus INTEGER
            );
            INSERT INTO sports VALUES (1, 'Basketball', 'NBA', 'totals');
            INSERT INTO teams VALUES (1, 1, 'LAL', 'Lakers');
            INSERT INTO teams VALUES (2, 1, 'BOS', 'Celtics');
            INSERT INTO teams VALUES (3, 1, 'NYK', 'Knicks');
            INSERT INTO teams VALUES (4, 1, 'MIA', 'Heat');
            INSERT INTO games VALUES ('PREV_HOME', 1, '2025-12-30T20:00:00+00:00', 1, 3, 'final');
            INSERT INTO games VALUES ('PREV_AWAY', 1, '2025-12-29T20:00:00+00:00', 4, 2, 'final');
            INSERT INTO games VALUES ('GAME1', 1, '2026-01-01T20:00:00+00:00', 1, 2, 'final');
            INSERT INTO games VALUES ('FUTURE_HOME', 1, '2026-01-02T20:00:00+00:00', 1, 4, 'final');
            INSERT INTO game_results VALUES ('PREV_HOME', 100, 90, -150, 130, 191.5);
            INSERT INTO game_results VALUES ('PREV_AWAY', 80, 95, -120, 100, 177.5);
            INSERT INTO game_results VALUES ('GAME1', 120, 110, -145, 130, 222.5);
            INSERT INTO game_results VALUES ('FUTURE_HOME', 150, 70, -200, 170, 221.5);
            INSERT INTO books VALUES (1, 'DraftKings');
            INSERT INTO books VALUES (2, 'FanDuel');
            INSERT INTO odds_snapshots VALUES ('OPEN', '2026-01-01T12:00:00+00:00', 1);
            INSERT INTO odds_snapshots VALUES ('LATEST', '2026-01-01T19:00:00+00:00', 1);
            INSERT INTO odds_snapshots VALUES ('AFTER', '2026-01-01T21:00:00+00:00', 1);
            """
        )
        conn.executemany(
            """
            INSERT INTO odds (
                snapshot_id, game_id, book_id, market, outcome, price_american, line
            ) VALUES (?, 'GAME1', ?, ?, ?, ?, ?)
            """,
            [
                ("OPEN", 1, "totals", "Over", -110, 220.5),
                ("OPEN", 1, "totals", "Under", -110, 220.5),
                ("LATEST", 1, "totals", "Over", -105, 221.5),
                ("LATEST", 1, "totals", "Under", -115, 221.5),
                ("LATEST", 2, "totals", "Over", -110, 222.0),
                ("LATEST", 2, "totals", "Under", -110, 222.0),
                ("AFTER", 1, "totals", "Over", -200, 230.0),
                ("AFTER", 1, "totals", "Under", 170, 230.0),
                ("OPEN", 1, "h2h", "home", -130, None),
                ("OPEN", 1, "h2h", "away", 110, None),
                ("LATEST", 1, "h2h", "home", -150, None),
                ("LATEST", 1, "h2h", "away", 125, None),
                ("LATEST", 2, "h2h", "home", -145, None),
                ("LATEST", 2, "h2h", "away", 130, None),
                ("AFTER", 1, "h2h", "home", -400, None),
                ("AFTER", 1, "h2h", "away", 300, None),
            ],
        )


def _write_processed_understat_fixture(processed_dir):
    league_dir = processed_dir / "external" / "understat" / "EPL"
    league_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "id": "U1",
                "datetime": "2025-08-01T12:00:00+00:00",
                "h": {"title": "Burnley", "short_title": "BUR"},
                "a": {"title": "Chelsea", "short_title": "CHE"},
            },
            {
                "id": "U2",
                "datetime": "2025-08-10T12:00:00+00:00",
                "h": {"title": "Burnley", "short_title": "BUR"},
                "a": {"title": "Chelsea", "short_title": "CHE"},
            },
        ]
    ).to_parquet(league_dir / "2025_dates.parquet", index=False)
    pd.DataFrame(
        [
            {
                "h_a": "h",
                "xG": 1.2,
                "xGA": 0.4,
                "ppda": {"att": 100, "def": 10},
                "ppda_allowed": {"att": 80, "def": 8},
                "deep": 5,
                "deep_allowed": 2,
                "scored": 1,
                "missed": 0,
                "xpts": 2.1,
                "date": "2025-08-01T12:00:00+00:00",
                "team_title": "Burnley",
            },
            {
                "h_a": "a",
                "xG": 0.4,
                "xGA": 1.2,
                "ppda": {"att": 80, "def": 8},
                "ppda_allowed": {"att": 100, "def": 10},
                "deep": 2,
                "deep_allowed": 5,
                "scored": 0,
                "missed": 1,
                "xpts": 0.6,
                "date": "2025-08-01T12:00:00+00:00",
                "team_title": "Chelsea",
            },
            {
                "h_a": "h",
                "xG": 9.0,
                "xGA": 8.0,
                "ppda": {"att": 999, "def": 9},
                "ppda_allowed": {"att": 888, "def": 8},
                "deep": 99,
                "deep_allowed": 88,
                "scored": 9,
                "missed": 8,
                "xpts": 3.0,
                "date": "2025-08-10T12:00:00+00:00",
                "team_title": "Burnley",
            },
            {
                "h_a": "a",
                "xG": 8.0,
                "xGA": 9.0,
                "ppda": {"att": 888, "def": 8},
                "ppda_allowed": {"att": 999, "def": 9},
                "deep": 88,
                "deep_allowed": 99,
                "scored": 8,
                "missed": 9,
                "xpts": 0.1,
                "date": "2025-08-10T12:00:00+00:00",
                "team_title": "Chelsea",
            },
        ]
    ).to_parquet(league_dir / "2025_teams.parquet", index=False)


def _create_soccer_market_db(path):
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE sports (
                sport_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                league TEXT NOT NULL,
                default_market TEXT NOT NULL
            );
            CREATE TABLE teams (
                team_id INTEGER PRIMARY KEY,
                sport_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                name TEXT NOT NULL
            );
            CREATE TABLE games (
                game_id TEXT PRIMARY KEY,
                sport_id INTEGER NOT NULL,
                start_time_utc TEXT NOT NULL,
                home_team_id INTEGER NOT NULL,
                away_team_id INTEGER NOT NULL,
                status TEXT
            );
            CREATE TABLE game_results (
                game_id TEXT PRIMARY KEY,
                home_score INTEGER,
                away_score INTEGER,
                home_moneyline_close REAL,
                away_moneyline_close REAL,
                total_close REAL
            );
            CREATE TABLE books (
                book_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL
            );
            CREATE TABLE odds_snapshots (
                snapshot_id TEXT PRIMARY KEY,
                fetched_at_utc TEXT NOT NULL,
                sport_id INTEGER NOT NULL
            );
            CREATE TABLE odds (
                snapshot_id TEXT NOT NULL,
                game_id TEXT NOT NULL,
                book_id INTEGER NOT NULL,
                market TEXT NOT NULL,
                outcome TEXT NOT NULL,
                price_american REAL,
                price_decimal REAL,
                implied_prob REAL,
                line REAL
            );
            INSERT INTO sports VALUES (1, 'Soccer', 'EPL', 'totals');
            INSERT INTO teams VALUES (1, 1, 'NAN', 'Burnley');
            INSERT INTO teams VALUES (2, 1, 'CHE', 'Chelsea');
            INSERT INTO games VALUES ('EPL_PREV', 1, '2025-08-01T12:00:00+00:00', 1, 2, 'final');
            INSERT INTO games VALUES ('EPL_TARGET', 1, '2025-08-10T12:00:00+00:00', 1, 2, 'final');
            INSERT INTO game_results VALUES ('EPL_PREV', 1, 0, -120, 110, 2.5);
            INSERT INTO game_results VALUES ('EPL_TARGET', 9, 8, -130, 120, 3.0);
            INSERT INTO books VALUES (1, 'DraftKings');
            INSERT INTO odds_snapshots VALUES ('EPL_LATEST', '2025-08-10T10:00:00+00:00', 1);
            """
        )
        conn.executemany(
            """
            INSERT INTO odds (
                snapshot_id, game_id, book_id, market, outcome, price_american, line
            ) VALUES ('EPL_LATEST', 'EPL_TARGET', 1, 'totals', ?, ?, 3.0)
            """,
            [("Over", -110), ("Under", -110)],
        )


def test_totals_model_input_uses_market_snapshots_without_leakage(tmp_path):
    db_path = tmp_path / "market.db"
    _create_market_db(db_path)

    df = build_totals_model_input(db_path=db_path, leagues=["NBA"])

    assert len(df) == 1
    row = df.iloc[0]
    assert row["snapshot_id"] == "LATEST"
    assert row["line"] == 221.5
    assert row["opening_line"] == 220.5
    assert row["line_movement"] == 1.0
    assert row["target_over"] == 1
    assert row["book_line_count"] == 2
    assert row["best_over_moneyline"] == -105
    assert row["best_under_moneyline"] == -115
    assert row["selected_book_total_close"] == 221.5
    assert row["selected_book_close_snapshot_id"] == "LATEST"
    assert row["best_over_book_total_close"] == 221.5
    assert row["best_under_book_total_close"] == 221.5
    assert row["home_score_for_l5"] == 100
    assert row["away_score_for_l5"] == 95
    assert row["home_score_for_l5"] != 150
    assert row["home_score_for_l3"] == 100
    assert row["home_same_venue_score_for_l5"] == 100
    assert row["home_back_to_back"] == 0.0


def test_totals_model_input_all_snapshots_uses_as_of_best_prices(tmp_path):
    db_path = tmp_path / "market.db"
    _create_market_db(db_path)

    df = build_totals_model_input(db_path=db_path, leagues=["NBA"], latest_only=False)

    assert set(df["snapshot_id"]) == {"OPEN", "LATEST"}
    open_row = df[df["snapshot_id"] == "OPEN"].iloc[0]
    assert open_row["book"] == "DraftKings"
    assert open_row["line"] == 220.5
    assert open_row["book_line_count"] == 1
    assert open_row["best_over_moneyline"] == -110
    assert open_row["best_under_moneyline"] == -110
    assert open_row["best_over_book"] == "DraftKings"
    assert open_row["best_under_book"] == "DraftKings"
    assert open_row["selected_book_total_close"] == 221.5
    assert open_row["selected_book_close_snapshot_id"] == "LATEST"


def test_moneyline_model_input_uses_latest_pre_game_pair(tmp_path):
    db_path = tmp_path / "market.db"
    _create_market_db(db_path)

    df = build_moneyline_model_input(db_path=db_path, leagues=["NBA"])

    assert len(df) == 1
    row = df.iloc[0]
    assert row["snapshot_id"] == "LATEST"
    assert row["home_moneyline"] == -150
    assert row["away_moneyline"] == 125
    assert row["best_home_moneyline"] == -145
    assert row["best_away_moneyline"] == 130
    assert row["opening_home_moneyline"] == -130
    assert row["target_home_win"] == 1


def test_moneyline_side_model_input_expands_to_bettable_sides(tmp_path):
    db_path = tmp_path / "market.db"
    _create_market_db(db_path)

    df = build_moneyline_side_model_input(db_path=db_path, leagues=["NBA"])

    assert len(df) == 2
    home = df[df["side"] == "home"].iloc[0]
    away = df[df["side"] == "away"].iloc[0]
    assert home["team"] == "Lakers"
    assert home["moneyline"] == -150
    assert home["best_moneyline"] == -145
    assert home["target_win"] == 1
    assert away["team"] == "Celtics"
    assert away["moneyline"] == 125
    assert away["best_moneyline"] == 130
    assert away["target_win"] == 0
    assert away["is_home"] == 0.0


def test_nba_advanced_and_availability_features_are_pregame(tmp_path):
    db_path = tmp_path / "market.db"
    _create_market_db(db_path)

    home_metrics = {
        "rolling_win_pct_5": 0.8,
        "rolling_point_diff_5": 6.2,
        "rolling_off_rating_5": 116.5,
        "rolling_def_rating_5": 109.0,
        "rolling_net_rating_5": 7.5,
        "rolling_pace_5": 101.4,
    }
    away_metrics = {
        "rolling_win_pct_5": 0.4,
        "rolling_point_diff_5": -2.0,
        "rolling_off_rating_5": 110.0,
        "rolling_def_rating_5": 112.0,
        "rolling_net_rating_5": -2.0,
        "rolling_pace_5": 98.0,
    }
    with sqlite3.connect(db_path) as conn:
        conn.executemany(
            """
            INSERT INTO team_features (game_id, team_id, feature_set, feature_json)
            VALUES ('GAME1', ?, 'game_stats', ?)
            """,
            [
                (1, json.dumps(home_metrics)),
                (2, json.dumps(away_metrics)),
            ],
        )
        conn.executemany(
            """
            INSERT INTO injury_reports (
                league, sport_id, team_id, team_code, player_name, position,
                status, practice_status, report_date, game_date, source_key
            ) VALUES ('NBA', 1, ?, ?, ?, ?, ?, ?, ?, ?, 'test')
            """,
            [
                (
                    1,
                    "LAL",
                    "Pregame Out",
                    "G",
                    "Out",
                    None,
                    "2026-01-01T10:00:00+00:00",
                    "2026-01-01",
                ),
                (
                    1,
                    "LAL",
                    "Postgame Out",
                    "F",
                    "Out",
                    None,
                    "2026-01-01T21:30:00+00:00",
                    "2026-01-01",
                ),
            ],
        )
        conn.execute(
            """
            INSERT INTO player_stats (
                game_id, team_id, player_id, player_name, min, pts, reb, ast,
                stl, blk, tov, pf, plus_minus
            ) VALUES ('PREV_HOME', 1, 101, 'Pregame Out', 32, 18, 6, 5, 1, 1, 2, 3, 7)
            """
        )

    df = build_totals_model_input(db_path=db_path, leagues=["NBA"])

    row = df.iloc[0]
    assert row["home_nba_rolling_pace_5"] == 101.4
    assert row["away_nba_rolling_off_rating_5"] == 110.0
    assert row["nba_rolling_net_rating_5_diff"] == 9.5
    assert row["home_injuries_total"] == 1
    assert row["home_injuries_skill_out"] == 1
    assert row["home_injuries_out_minutes_l10"] == 32
    assert row["home_injuries_impact_points_l10"] == 18


def test_feature_coverage_report_includes_density_and_drop_reasons(tmp_path):
    db_path = tmp_path / "market.db"
    _create_market_db(db_path)

    report = build_feature_coverage_report(db_path=db_path, leagues=["NBA"], markets=["totals"])

    row = report.iloc[0]
    assert row["market"] == "totals"
    assert row["league"] == "NBA"
    assert row["row_count"] == 1
    assert row["market_non_null_pct"] > 0
    assert row["team_form_non_null_pct"] > 0
    assert "soccer_understat_team_non_null_pct" in report.columns
    assert row["drop_no_pre_game_odds"] >= 1


def test_football_data_form_features_shift_before_target_game(tmp_path, monkeypatch):
    processed = tmp_path / "processed"
    league_dir = processed / "external" / "football_data" / "premier-league"
    league_dir.mkdir(parents=True)
    pd.DataFrame(
        [
            {
                "Date": "01/08/2024",
                "HomeTeam": "Man United",
                "AwayTeam": "Fulham",
                "FTHG": 1,
                "FTAG": 0,
                "HS": 10,
                "AS": 5,
                "HST": 4,
                "AST": 1,
            },
            {
                "Date": "10/08/2024",
                "HomeTeam": "Man United",
                "AwayTeam": "Fulham",
                "FTHG": 9,
                "FTAG": 8,
                "HS": 99,
                "AS": 88,
                "HST": 50,
                "AST": 40,
            },
        ]
    ).to_parquet(league_dir / "2425_E0.parquet", index=False)
    monkeypatch.setattr(soccer_features, "PROCESSED_DATA_DIR", processed)

    home = soccer_features.normalize_team_code("EPL", "Man United")
    away = soccer_features.normalize_team_code("EPL", "Fulham")
    games = pd.DataFrame(
        [
            {
                "game_id": "EPL_1",
                "season": 2024,
                "start_time_utc": "2024-08-01T19:00:00+00:00",
                "home_team": home,
                "away_team": away,
            },
            {
                "game_id": "EPL_2",
                "season": 2024,
                "start_time_utc": "2024-08-10T19:00:00+00:00",
                "home_team": home,
                "away_team": away,
            },
        ]
    )

    features = soccer_features.build_football_data_form_features("EPL", [2024], games)

    first = features[(features["game_id"] == "EPL_1") & (features["team"] == home)].iloc[0]
    target = features[(features["game_id"] == "EPL_2") & (features["team"] == home)].iloc[0]
    assert pd.isna(first["fd_goals_for_l5"])
    assert target["fd_goals_for_l5"] == 1
    assert target["fd_shots_for_l5"] == 10


def test_processed_understat_fallback_shifts_before_target_game(tmp_path, monkeypatch):
    processed = tmp_path / "processed"
    _write_processed_understat_fixture(processed)
    monkeypatch.setattr(soccer_features, "PROCESSED_DATA_DIR", processed)
    monkeypatch.setattr(soccer_features, "RAW_DATA_DIR", tmp_path / "raw")

    games = pd.DataFrame(
        [
            {
                "game_id": "EPL_PREV",
                "season": 2025,
                "start_time_utc": "2025-08-01T12:00:00+00:00",
                "home_team": "NAN",
                "away_team": "CHE",
                "home_team_name": "Burnley",
                "away_team_name": "Chelsea",
            },
            {
                "game_id": "EPL_TARGET",
                "season": 2025,
                "start_time_utc": "2025-08-10T12:00:00+00:00",
                "home_team": "NAN",
                "away_team": "CHE",
                "home_team_name": "Burnley",
                "away_team_name": "Chelsea",
            },
        ]
    )

    features = soccer_features.build_understat_features("EPL", [2025], games)

    prior = features[(features["game_id"] == "EPL_PREV") & (features["team"] == "NAN")].iloc[0]
    target = features[(features["game_id"] == "EPL_TARGET") & (features["team"] == "NAN")].iloc[0]
    assert pd.isna(prior["ust_team_xg_avg_l5"])
    assert target["ust_team_xg_avg_l5"] == 1.2
    assert target["ust_team_goals_for_avg_l5"] == 1
    assert target["ust_team_xg_avg_l5"] != 9.0


def test_totals_input_attaches_understat_by_name_when_warehouse_code_is_bad(
    tmp_path,
    monkeypatch,
):
    processed = tmp_path / "processed"
    _write_processed_understat_fixture(processed)
    monkeypatch.setattr(soccer_features, "PROCESSED_DATA_DIR", processed)
    monkeypatch.setattr(soccer_features, "RAW_DATA_DIR", tmp_path / "raw")
    db_path = tmp_path / "soccer.db"
    _create_soccer_market_db(db_path)

    df = build_totals_model_input(db_path=db_path, leagues=["EPL"])

    assert len(df) == 1
    row = df.iloc[0]
    assert row["home_team_code"] == "NAN"
    assert row["home_soccer_ust_team_xg_avg_l5"] == 1.2
    assert row["home_soccer_ust_team_goals_for_avg_l5"] == 1
