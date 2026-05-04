from __future__ import annotations

import json
import sqlite3

from src.data.odds_coverage import build_odds_coverage_report, main


def _create_odds_coverage_db(path) -> None:
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
            INSERT INTO sports VALUES (1, 'Basketball', 'NBA', 'totals');
            INSERT INTO sports VALUES (2, 'Hockey', 'NHL', 'moneyline');
            INSERT INTO teams VALUES (1, 1, 'LAL', 'Lakers');
            INSERT INTO teams VALUES (2, 1, 'BOS', 'Celtics');
            INSERT INTO teams VALUES (3, 2, 'NYR', 'Rangers');
            INSERT INTO teams VALUES (4, 2, 'BOS', 'Bruins');
            INSERT INTO games VALUES ('NBA_G1', 1, '2026-01-10T20:00:00+00:00', 1, 2, 'final');
            INSERT INTO games VALUES ('NBA_G2', 1, '2026-01-11T20:00:00+00:00', 1, 2, 'final');
            INSERT INTO games VALUES ('NBA_G3', 1, '2026-01-12T20:00:00+00:00', 1, 2, 'final');
            INSERT INTO games VALUES ('NBA_G4', 1, '2026-01-13T20:00:00+00:00', 1, 2, 'final');
            INSERT INTO game_results VALUES ('NBA_G1', 112, 108, -130, 120, 221.5);
            INSERT INTO game_results VALUES ('NBA_G2', 104, 101, -115, 105, 210.5);
            INSERT INTO game_results VALUES ('NBA_G3', 99, 97, -110, 100, 205.5);
            INSERT INTO game_results VALUES ('NBA_G4', 120, 118, -125, 115, NULL);
            INSERT INTO books VALUES (1, 'DraftKings');
            INSERT INTO books VALUES (2, 'FanDuel');
            INSERT INTO odds_snapshots VALUES ('G1_OPEN', '2026-01-09T14:00:00+00:00', 1);
            INSERT INTO odds_snapshots VALUES ('G1_LATEST', '2026-01-10T18:00:00+00:00', 1);
            INSERT INTO odds_snapshots VALUES ('G1_AFTER', '2026-01-10T21:00:00+00:00', 1);
            INSERT INTO odds_snapshots VALUES ('G2_STALE', '2026-01-07T20:00:00+00:00', 1);
            INSERT INTO odds_snapshots VALUES ('G4_CURRENT', '2026-01-12T18:00:00+00:00', 1);
            """
        )
        conn.executemany(
            """
            INSERT INTO odds (
                snapshot_id, game_id, book_id, market, outcome, price_american, line
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("G1_OPEN", "NBA_G1", 1, "totals", "Over", -110, 220.5),
                ("G1_OPEN", "NBA_G1", 1, "totals", "Under", -110, 220.5),
                ("G1_LATEST", "NBA_G1", 1, "totals", "Over", -105, 221.5),
                ("G1_LATEST", "NBA_G1", 1, "totals", "Under", -115, 221.5),
                ("G1_LATEST", "NBA_G1", 2, "totals", "Over", -100, 221.5),
                ("G1_LATEST", "NBA_G1", 2, "totals", "Under", -110, 221.5),
                ("G1_AFTER", "NBA_G1", 1, "totals", "Over", 100, 230.0),
                ("G1_AFTER", "NBA_G1", 1, "totals", "Under", -120, 230.0),
                ("G2_STALE", "NBA_G2", 1, "totals", "Over", -110, 209.5),
                ("G2_STALE", "NBA_G2", 1, "totals", "Under", -110, 209.5),
                ("G4_CURRENT", "NBA_G4", 1, "totals", "Over", -110, 200.5),
                ("G4_CURRENT", "NBA_G4", 1, "totals", "Under", -110, 200.5),
                ("G1_OPEN", "NBA_G1", 1, "h2h", "home", -135, None),
                ("G1_OPEN", "NBA_G1", 1, "h2h", "away", 115, None),
                ("G1_LATEST", "NBA_G1", 1, "h2h", "home", -125, None),
                ("G1_LATEST", "NBA_G1", 1, "h2h", "away", 110, None),
                ("G1_LATEST", "NBA_G1", 2, "h2h", "home", -120, None),
                ("G1_LATEST", "NBA_G1", 2, "h2h", "away", 115, None),
                ("G1_AFTER", "NBA_G1", 1, "h2h", "home", -300, None),
                ("G1_AFTER", "NBA_G1", 1, "h2h", "away", 250, None),
            ],
        )


def _row(report: dict, league: str, market: str) -> dict:
    return next(
        row
        for row in report["rows"]
        if row["league"] == league and row["market"] == market
    )


def test_odds_coverage_counts_timing_clv_books_and_selected_vs_best(tmp_path) -> None:
    db_path = tmp_path / "coverage.db"
    _create_odds_coverage_db(db_path)

    report = build_odds_coverage_report(
        db_path=db_path,
        leagues=["NBA", "NHL"],
        markets=["totals", "moneyline"],
        max_hours_before_start=72.0,
    )
    totals = _row(report, "NBA", "totals")

    assert totals["settled_games"] == 4
    assert totals["games_with_any_odds"] == 3
    assert totals["games_with_complete_market_pair"] == 3
    assert totals["games_with_usable_odds"] == 3
    assert totals["games_with_opening_odds"] == 3
    assert totals["games_with_current_odds"] == 3
    assert totals["games_with_closing_odds"] == 3
    assert totals["games_with_clv"] == 2
    assert totals["games_with_clv_after_timing"] == 1
    assert totals["sample_ready_games"] == 1
    assert totals["games_without_usable_odds"] == 1
    assert totals["timing_filters"]["stale_odds_excluded"] == 1
    assert totals["hours_before_start_buckets"]["1-6h"] == 1
    assert totals["hours_before_start_buckets"]["24-72h"] == 1
    assert totals["hours_before_start_buckets"][">=72h"] == 1
    assert totals["book_coverage"]["usable_games_by_book"][0] == {
        "book": "DraftKings",
        "games": 3,
    }
    assert totals["book_coverage"]["usable_games_by_book"][1] == {
        "book": "FanDuel",
        "games": 1,
    }
    assert totals["selected_vs_best"]["best_book_differs_any_side_games"] == 1
    assert totals["selected_vs_best"]["selected_book_is_best_all_sides_games"] == 2

    moneyline = _row(report, "NBA", "moneyline")
    assert moneyline["games_with_usable_odds"] == 1
    assert moneyline["games_with_clv"] == 1
    assert moneyline["sample_ready_games"] == 1
    assert moneyline["clv_side_counts"] == {"home": 1, "away": 1}
    assert moneyline["selected_vs_best"]["best_book_differs_any_side_games"] == 1

    nhl_totals = _row(report, "NHL", "totals")
    assert nhl_totals["settled_games"] == 0
    assert nhl_totals["games_with_usable_odds"] == 0


def test_odds_coverage_cli_writes_json_and_prints_summary(tmp_path, capsys) -> None:
    db_path = tmp_path / "coverage.db"
    output_path = tmp_path / "reports" / "latest.json"
    _create_odds_coverage_db(db_path)

    main(
        [
            "--db",
            str(db_path),
            "--leagues",
            "NBA",
            "--markets",
            "totals",
            "--output",
            str(output_path),
        ]
    )

    captured = capsys.readouterr().out
    assert "Odds coverage audit" in captured
    assert "JSON written to" in captured
    assert output_path.exists()
    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["rows"][0]["league"] == "NBA"
    assert payload["rows"][0]["market"] == "totals"
    assert payload["rows"][0]["sample_ready_games"] == 1
