import sqlite3
from contextlib import contextmanager

import pandas as pd
import pytest

from src.db.core import initialize
from src.models import forward_test
from src.predict import storage


def test_save_predictions_overwrites_existing(monkeypatch, tmp_path):
    forward_dir = tmp_path / 'forward_test'
    forward_dir.mkdir()
    monkeypatch.setattr(forward_test, 'FORWARD_TEST_DIR', forward_dir)

    base = {
        'game_id': ['NBA_GAME_1'],
        'commence_time': [pd.Timestamp('2025-11-10T00:00:00Z')],
        'predicted_at': [pd.Timestamp('2025-11-08T12:00:00Z')],
        'home_team': ['LAL'],
        'away_team': ['BOS'],
        'home_moneyline': [-140],
        'away_moneyline': [120],
        'home_predicted_prob': [0.6],
        'away_predicted_prob': [0.4],
        'home_implied_prob': [0.58],
        'away_implied_prob': [0.42],
        'home_edge': [0.02],
        'away_edge': [-0.01],
    }

    df1 = pd.DataFrame(base)
    forward_test.save_predictions(df1, timestamp='20250101_000000')

    df2 = pd.DataFrame({
        **base,
        'home_predicted_prob': [0.7],
        'away_predicted_prob': [0.3],
        'predicted_at': [pd.Timestamp('2025-11-08T15:00:00Z')],
    })
    forward_test.save_predictions(df2, timestamp='20250101_010000')

    master = pd.read_parquet(forward_dir / 'ensemble' / 'predictions_master.parquet')
    assert len(master) == 1
    assert master.iloc[0]['home_predicted_prob'] == 0.7
    assert master.iloc[0]['predicted_at'] == pd.Timestamp('2025-11-08T15:00:00Z', tz='UTC')


def test_sqlite_predictions_store_current_totals(monkeypatch, tmp_path):
    db_path = tmp_path / "predictions.db"
    initialize(db_path)

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO sports (sport_id, name, league, default_market) VALUES (1, 'Basketball', 'NBA', 'moneyline')"
        )
        conn.execute("INSERT INTO teams (team_id, sport_id, code, name) VALUES (1, 1, 'LAL', 'Lakers')")
        conn.execute("INSERT INTO teams (team_id, sport_id, code, name) VALUES (2, 1, 'BOS', 'Celtics')")
        conn.execute(
            """
            INSERT INTO games (
                game_id, sport_id, season, start_time_utc, home_team_id, away_team_id, status
            ) VALUES ('NBA_GAME_1', 1, 2025, '2025-11-10T00:00:00+00:00', 1, 2, 'scheduled')
            """
        )

    @contextmanager
    def temp_connect(path=None):
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    monkeypatch.setattr(storage, "connect", temp_connect)

    first = pd.DataFrame(
        {
            "game_id": ["NBA_GAME_1"],
            "home_predicted_prob": [0.55],
            "away_predicted_prob": [0.45],
            "total_line": [220.5],
            "predicted_total_points": [224.0],
            "over_predicted_prob": [0.57],
            "under_predicted_prob": [0.43],
        }
    )
    storage.save_predictions(first, "ensemble", pd.Timestamp("2025-11-08T12:00:00Z").to_pydatetime())

    second = first.copy()
    second["predicted_total_points"] = [226.5]
    storage.save_predictions(second, "ensemble", pd.Timestamp("2025-11-08T13:00:00Z").to_pydatetime())

    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(
            "SELECT model_type, predicted_total_points FROM predictions WHERE game_id = 'NBA_GAME_1'"
        ).fetchall()

    assert rows == [("ensemble", 226.5)]


@pytest.fixture
def prediction_warehouse(tmp_path, monkeypatch):
    db_path = tmp_path / "current_predictions.db"
    initialize(db_path)
    monkeypatch.setenv("SPORTS_DB_PATH", str(db_path))
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            INSERT INTO sports (sport_id, name, league, default_market)
            VALUES (1, 'Basketball', 'NBA', 'h2h');
            INSERT INTO teams (team_id, sport_id, code, name) VALUES (1, 1, 'LAL', 'Lakers');
            INSERT INTO teams (team_id, sport_id, code, name) VALUES (2, 1, 'BOS', 'Celtics');
            INSERT INTO games (game_id, sport_id, home_team_id, away_team_id)
            VALUES ('GAME', 1, 1, 2);
            """
        )
    return db_path


def test_quote_metadata_roundtrip_and_refresh_without_lineage(prediction_warehouse):
    provenance = {
        "selected_totals_snapshot_id": "total-observed",
        "selected_totals_book_id": 2,
        "selected_totals_book": "FanDuel",
        "selected_totals_fetched_at": "2026-11-07T00:00:00+00:00",
        "selected_h2h_snapshot_id": "moneyline-observed",
        "selected_h2h_book_id": 1,
        "selected_h2h_book": "DraftKings",
        "selected_h2h_fetched_at": "2026-11-06T23:59:00+00:00",
        "moneyline_model_sha256": "a" * 64,
        "totals_model_sha256": "b" * 64,
    }
    raw = {"game_id": "GAME", "home_moneyline": -120, "away_moneyline": 110, "total_line": 221.5}
    storage.save_predictions(pd.DataFrame([{**raw, **provenance}]), "ensemble")
    stored = storage.load_current_predictions("ensemble").iloc[0]
    for field, value in provenance.items():
        assert stored[field] == value

    # Identical prices from an untraced caller cannot inherit earlier identifiers.
    storage.save_predictions(pd.DataFrame([raw]), "ensemble")
    refreshed = storage.load_current_predictions("ensemble")
    assert len(refreshed) == 1
    assert refreshed[list(provenance)].isna().all().all()
    assert refreshed.iloc[0]["total_line"] == 221.5


def test_legacy_prediction_metadata_migration_preserves_rows(prediction_warehouse):
    storage.save_predictions(
        pd.DataFrame([{"game_id": "GAME", "home_predicted_prob": 0.6}]), "ensemble"
    )
    with sqlite3.connect(prediction_warehouse) as conn:
        for column in storage.PREDICTION_METADATA_COLUMNS:
            if column != "predicted_total_points":
                conn.execute(f"ALTER TABLE predictions DROP COLUMN {column}")
    initialize(prediction_warehouse)
    stored = storage.load_current_predictions("ensemble")
    assert len(stored) == 1
    assert stored.iloc[0]["home_prob"] == 0.6
    assert stored["selected_totals_snapshot_id"].isna().all()
    assert stored["selected_h2h_snapshot_id"].isna().all()


def test_failed_current_prediction_write_preserves_previous_metadata(prediction_warehouse):
    original = {
        "game_id": "GAME",
        "home_predicted_prob": 0.6,
        "selected_h2h_snapshot_id": "original",
        "selected_h2h_book_id": 1,
        "selected_h2h_fetched_at": "2026-11-06T23:59:00+00:00",
    }
    storage.save_predictions(pd.DataFrame([original]), "ensemble")
    with sqlite3.connect(prediction_warehouse) as conn:
        conn.execute(
            """
            CREATE TRIGGER reject_prediction BEFORE INSERT ON predictions
            BEGIN SELECT RAISE(ABORT, 'synthetic failure'); END
            """
        )
    with pytest.raises(sqlite3.IntegrityError, match="synthetic failure"):
        storage.save_predictions(pd.DataFrame([{"game_id": "GAME"}]), "ensemble")
    stored = storage.load_current_predictions("ensemble")
    assert stored.iloc[0]["home_prob"] == 0.6
    assert stored.iloc[0]["selected_h2h_snapshot_id"] == "original"


def _game_and_odds():
    games = pd.DataFrame([{"game_id": "GAME", "home_team": "Lakers", "away_team": "Celtics"}])
    rows = []
    for snapshot, time, book_id, book, market, outcome, price, line in [
        ("new", "2026-11-07T00:00:00Z", 1, "DraftKings", "h2h", "home", -130, None),
        ("new", "2026-11-07T00:00:00Z", 2, "FanDuel", "h2h", "away", 120, None),
        ("old", "2026-11-06T23:00:00+00:00", 1, "DraftKings", "h2h", "home", -120, None),
        ("old", "2026-11-06T23:00:00+00:00", 1, "DraftKings", "h2h", "away", 110, None),
        ("new", "2026-11-07T00:00:00Z", 1, "DraftKings", "totals", "over", -110, 221.5),
        ("new", "2026-11-07T00:00:00Z", 1, "DraftKings", "totals", "under", -110, 222.5),
        ("old", "2026-11-06T23:00:00+00:00", 2, "FanDuel", "totals", "over", -105, 220.5),
        ("old", "2026-11-06T23:00:00+00:00", 2, "FanDuel", "totals", "under", -115, 220.5),
    ]:
        rows.append(
            {
                "game_id": "GAME",
                "snapshot_id": snapshot,
                "fetched_at_utc": time,
                "book_id": book_id,
                "book_name": book,
                "market": market,
                "outcome": outcome,
                "price_american": price,
                "line": line,
            }
        )
    return games, pd.DataFrame(rows).astype({"price_american": float})


def test_selected_quote_preserves_intact_pair_instead_of_newer_mixed_sides():
    games, odds = _game_and_odds()
    selected = storage._apply_deterministic_odds(games, odds).iloc[0]
    assert (selected["home_moneyline"], selected["away_moneyline"]) == (-120, 110)
    assert selected["selected_h2h_snapshot_id"] == "old"
    assert selected["selected_h2h_book_id"] == 1
    assert selected["selected_h2h_fetched_at"] == "2026-11-06T23:00:00+00:00"
    assert (selected["total_line"], selected["over_moneyline"], selected["under_moneyline"]) == (
        220.5,
        -105,
        -115,
    )
    assert selected["selected_totals_snapshot_id"] == "old"
    assert selected["selected_totals_book_id"] == 2
    assert selected["selected_totals_fetched_at"] == "2026-11-06T23:00:00+00:00"


@pytest.mark.parametrize(
    ("outcome", "expected"),
    [
        ("Under", "under"),
        ("Over", "over"),
        ("Draw", "draw"),
        ("home", "home"),
        ("away", "away"),
        ("Oklahoma City Thunder", "home"),
        ("Thunder", "home"),
        ("Washington Wizards", "away"),
        ("Wizards", "away"),
    ],
)
def test_explicit_outcomes_take_precedence_over_thunder_team_name(outcome, expected):
    assert storage._normalize_outcome(
        outcome, "Oklahoma City Thunder", "Washington Wizards"
    ) == expected


@pytest.mark.parametrize("hannover_is_home", [True, False])
def test_over_does_not_match_hannover_team_name(hannover_is_home):
    teams = ["Hannover 96", "Hamburger SV"]
    if not hannover_is_home:
        teams.reverse()
    assert storage._normalize_outcome("Over", *teams) == "over"


@pytest.mark.parametrize("thunder_is_home", [True, False])
def test_thunder_totals_preserve_under_price_and_paired_quote_provenance(thunder_is_home):
    games, odds = _game_and_odds()
    teams = ["Oklahoma City Thunder", "Washington Wizards"]
    if not thunder_is_home:
        teams.reverse()
    games.loc[0, ["home_team", "away_team"]] = teams
    odds["outcome"] = odds["outcome"].str.title()
    selected = storage._apply_deterministic_odds(games, odds).iloc[0]
    assert (selected["total_line"], selected["over_moneyline"], selected["under_moneyline"]) == (
        220.5,
        -105,
        -115,
    )
    assert selected["selected_totals_snapshot_id"] == "old"
    assert selected["selected_totals_book_id"] == 2
    assert selected["selected_totals_book"] == "FanDuel"
    assert selected["selected_totals_fetched_at"] == "2026-11-06T23:00:00+00:00"


def test_unpaired_prices_remain_diagnostic_with_empty_quote_provenance():
    games, odds = _game_and_odds()
    selected = storage._apply_deterministic_odds(games, odds[odds["snapshot_id"] == "new"]).iloc[0]
    assert (selected["home_moneyline"], selected["away_moneyline"]) == (-130, 120)
    assert (selected["over_moneyline"], selected["under_moneyline"]) == (-110, -110)
    for market in ("h2h", "totals"):
        for suffix in ("snapshot_id", "book_id", "book", "fetched_at"):
            assert pd.isna(selected[f"selected_{market}_{suffix}"])


@pytest.mark.parametrize(
    "field,value", [("fetched_at_utc", None), ("price_american", float("inf"))]
)
def test_invalid_pair_cannot_receive_quote_provenance(field, value):
    games, odds = _game_and_odds()
    odds.loc[odds["snapshot_id"] == "old", field] = value
    selected = storage._apply_deterministic_odds(games, odds).iloc[0]
    assert pd.isna(selected["selected_h2h_snapshot_id"])
    assert pd.isna(selected["selected_totals_snapshot_id"])
