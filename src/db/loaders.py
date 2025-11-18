"""Database loading utilities for ingestion scripts."""

from __future__ import annotations

import json
import logging
import uuid
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence

import pandas as pd

from src.data.team_mappings import normalize_team_code

from .core import connect

LOGGER = logging.getLogger(__name__)


def _safe_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ensure_odds_line_column(conn) -> None:
    try:
        conn.execute("ALTER TABLE odds ADD COLUMN line REAL")
    except sqlite3.OperationalError as exc:
        message = str(exc).lower()
        if "duplicate column name" in message:
            return
        raise


_TEAMRANKINGS_COLUMNS = {
    "tr_pick": "TEXT",
    "tr_total_line": "REAL",
    "tr_confidence": "REAL",
    "tr_odds": "REAL",
    "tr_model_pick": "TEXT",
    "tr_model_prob": "REAL",
    "tr_retrieved_at": "TEXT",
}


def _ensure_game_results_tr_columns(conn) -> None:
    """Make sure game_results has the optional TeamRankings columns."""
    existing = {
        row[1] for row in conn.execute("PRAGMA table_info(game_results)").fetchall()
    }
    for column, ddl in _TEAMRANKINGS_COLUMNS.items():
        if column in existing:
            continue
        conn.execute(f"ALTER TABLE game_results ADD COLUMN {column} {ddl}")


def _ensure_sport(conn, name: str, league: str, default_market: str) -> int:
    row = conn.execute("SELECT sport_id FROM sports WHERE league = ?", (league,)).fetchone()
    if row:
        return row[0]
    conn.execute(
        "INSERT INTO sports (name, league, default_market) VALUES (?, ?, ?)",
        (name, league, default_market),
    )
    return conn.execute("SELECT sport_id FROM sports WHERE league = ?", (league,)).fetchone()[0]


def _ensure_team(conn, sport_id: int, code: str, name: Optional[str] = None) -> int:
    row = conn.execute(
        "SELECT team_id FROM teams WHERE sport_id = ? AND code = ?",
        (sport_id, code),
    ).fetchone()
    if row:
        if name:
            conn.execute(
                "UPDATE teams SET name = ? WHERE team_id = ?",
                (name, row[0]),
            )
        return row[0]

    conn.execute(
        "INSERT INTO teams (sport_id, code, name) VALUES (?, ?, ?)",
        (sport_id, code, name or code),
    )
    return conn.execute(
        "SELECT team_id FROM teams WHERE sport_id = ? AND code = ?",
        (sport_id, code),
    ).fetchone()[0]


def _parse_datetime(date_str: Any, time_str: Any) -> Optional[str]:
    if pd.isna(date_str):
        return None
    if isinstance(date_str, str) and date_str:
        date_part = date_str
    else:
        return None

    time_part = "00:00"
    if isinstance(time_str, str) and time_str:
        time_part = time_str

    try:
        dt = datetime.fromisoformat(f"{date_part}T{time_part}")
        return dt.replace(tzinfo=timezone.utc).isoformat()
    except ValueError:
        return None


def _season_from_datetime(dt_iso: Optional[str], fallback: Optional[int]) -> Optional[int]:
    if dt_iso:
        try:
            return datetime.fromisoformat(dt_iso).year
        except ValueError:
            return fallback
    return fallback


def _normalize_score(value: Any) -> Optional[int]:
    if value is None or pd.isna(value):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_moneyline(value: Any) -> Optional[float]:
    if value is None or pd.isna(value):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def load_schedules(
    df: pd.DataFrame,
    source_version: str = "nfl_data_py",
    *,
    league: str = "NFL",
    sport_name: str = "Football",
    default_market: str = "moneyline",
) -> None:
    if df.empty:
        LOGGER.info("No schedules to load")
        return

    with connect() as conn:
        sport_id = _ensure_sport(conn, name=sport_name, league=league, default_market=default_market)
        team_cache: Dict[str, int] = {}

        for row in df.to_dict("records"):
            home_raw = str(row.get("home_team", "")).strip()
            away_raw = str(row.get("away_team", "")).strip()
            if not home_raw or not away_raw:
                continue

            home_code = normalize_team_code(league, home_raw)
            away_code = normalize_team_code(league, away_raw)
            if not home_code or not away_code:
                continue

            if home_code not in team_cache:
                team_cache[home_code] = _ensure_team(conn, sport_id, home_code, name=row.get("home_team_name", home_raw))
            if away_code not in team_cache:
                team_cache[away_code] = _ensure_team(conn, sport_id, away_code, name=row.get("away_team_name", away_raw))

            home_team_id = team_cache[home_code]
            away_team_id = team_cache[away_code]

            game_id = row.get("game_id")
            if not game_id:
                continue

            if league.upper() == "NBA" and not game_id.startswith("NBA_"):
                game_id = f"NBA_{game_id}"
            elif league.upper() == "MLB" and not game_id.startswith("MLB_"):
                game_id = f"MLB_{game_id}"
            elif league.upper() == "EPL" and not game_id.startswith("EPL_"):
                game_id = f"EPL_{game_id}"
            elif league.upper() == "LALIGA" and not game_id.startswith("LALIGA_"):
                game_id = f"LALIGA_{game_id}"
            elif league.upper() == "BUNDESLIGA" and not game_id.startswith("BUNDESLIGA_"):
                game_id = f"BUNDESLIGA_{game_id}"
            elif league.upper() == "SERIEA" and not game_id.startswith("SERIEA_"):
                game_id = f"SERIEA_{game_id}"
            elif league.upper() == "LIGUE1" and not game_id.startswith("LIGUE1_"):
                game_id = f"LIGUE1_{game_id}"

            start_time = _parse_datetime(row.get("gameday"), row.get("gametime"))
            season = _season_from_datetime(start_time, int(row.get("season")) if not pd.isna(row.get("season")) else None)

            gsis = row.get("gsis")
            if gsis is not None and not pd.isna(gsis):
                gsis_id = str(int(gsis)) if isinstance(gsis, float) else str(gsis)
            else:
                gsis_id = None

            pfr_id = row.get("pfr") if row.get("pfr") and not pd.isna(row.get("pfr")) else None

            status = "final" if (_normalize_score(row.get("home_score")) is not None and _normalize_score(row.get("away_score")) is not None) else "scheduled"

            conn.execute(
                """
                INSERT INTO games (
                    game_id, sport_id, season, game_type, week, start_time_utc,
                    home_team_id, away_team_id, venue, status, gsis_id, pfr_id
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(game_id) DO UPDATE SET
                    sport_id = excluded.sport_id,
                    season = excluded.season,
                    game_type = excluded.game_type,
                    week = excluded.week,
                    start_time_utc = excluded.start_time_utc,
                    home_team_id = excluded.home_team_id,
                    away_team_id = excluded.away_team_id,
                    venue = excluded.venue,
                    status = excluded.status,
                    gsis_id = COALESCE(excluded.gsis_id, games.gsis_id),
                    pfr_id = COALESCE(excluded.pfr_id, games.pfr_id)
                """,
                (
                    game_id,
                    sport_id,
                    season,
                    row.get("game_type"),
                    int(row.get("week")) if not pd.isna(row.get("week")) else None,
                    start_time,
                    home_team_id,
                    away_team_id,
                    row.get("stadium"),
                    status,
                    gsis_id,
                    pfr_id,
                ),
            )

            conn.execute(
                """
                INSERT INTO game_results (
                    game_id, home_score, away_score, home_moneyline_close,
                    away_moneyline_close, spread_close, total_close, source_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(game_id) DO UPDATE SET
                    home_score = excluded.home_score,
                    away_score = excluded.away_score,
                    home_moneyline_close = excluded.home_moneyline_close,
                    away_moneyline_close = excluded.away_moneyline_close,
                    spread_close = excluded.spread_close,
                    total_close = excluded.total_close,
                    source_version = excluded.source_version
                """,
                (
                    game_id,
                    _normalize_score(row.get("home_score")),
                    _normalize_score(row.get("away_score")),
                    _normalize_moneyline(row.get("home_moneyline")),
                    _normalize_moneyline(row.get("away_moneyline")),
                    row.get("spread_line") if not pd.isna(row.get("spread_line")) else None,
                    row.get("total_line") if not pd.isna(row.get("total_line")) else None,
                    source_version,
                ),
            )

        LOGGER.info("Stored %d schedule rows into database", len(df))


def _american_to_decimal(american: float) -> Optional[float]:
    if american is None:
        return None
    if american > 0:
        return american / 100.0 + 1.0
    if american < 0:
        return 100.0 / abs(american) + 1.0
    return 1.0


def _implied_probability(american: float) -> Optional[float]:
    if american is None:
        return None
    if american < 0:
        return abs(american) / (abs(american) + 100.0)
    return 100.0 / (american + 100.0)


def _get_or_create_book(conn, name: str, region: Optional[str]) -> int:
    conn.execute(
        "INSERT OR IGNORE INTO books (name, region) VALUES (?, ?)",
        (name, region),
    )
    row = conn.execute("SELECT book_id FROM books WHERE name = ?", (name,)).fetchone()
    return row[0]


def _register_data_source(
    conn,
    *,
    source_key: str,
    name: str,
    league: Optional[str],
    category: str,
    uri: Optional[str],
    enabled: bool,
    default_frequency: Optional[str],
) -> int:
    conn.execute(
        """
        INSERT INTO data_sources (source_key, name, league, category, uri, enabled, default_frequency)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_key) DO UPDATE SET
            name = excluded.name,
            league = excluded.league,
            category = excluded.category,
            uri = excluded.uri,
            enabled = excluded.enabled,
            default_frequency = excluded.default_frequency
        """,
        (
            source_key,
            name,
            league,
            category,
            uri,
            int(enabled),
            default_frequency,
        ),
    )
    row = conn.execute(
        "SELECT source_id FROM data_sources WHERE source_key = ?",
        (source_key,),
    ).fetchone()
    return row[0]


def register_data_source(
    *,
    source_key: str,
    name: str,
    league: Optional[str],
    category: str,
    uri: Optional[str] = None,
    enabled: bool = True,
    default_frequency: Optional[str] = None,
) -> int:
    with connect() as conn:
        return _register_data_source(
            conn,
            source_key=source_key,
            name=name,
            league=league,
            category=category,
            uri=uri,
            enabled=enabled,
            default_frequency=default_frequency,
        )


def start_source_run(
    *,
    source_key: str,
    name: str,
    league: Optional[str],
    category: str,
    uri: Optional[str] = None,
    enabled: bool = True,
    default_frequency: Optional[str] = None,
) -> Dict[str, Any]:
    run_id = uuid.uuid4().hex
    started_at = datetime.now(timezone.utc).isoformat()

    with connect() as conn:
        source_id = _register_data_source(
            conn,
            source_key=source_key,
            name=name,
            league=league,
            category=category,
            uri=uri,
            enabled=enabled,
            default_frequency=default_frequency,
        )
        conn.execute(
            """
            INSERT INTO source_runs (
                run_id, source_id, started_at, status
            ) VALUES (?, ?, ?, ?)
            """,
            (run_id, source_id, started_at, "running"),
        )

    return {
        "run_id": run_id,
        "source_id": source_id,
        "started_at": started_at,
        "source_key": source_key,
    }


def finalize_source_run(
    run_id: str,
    *,
    status: str,
    message: Optional[str] = None,
    records_ingested: Optional[int] = None,
    raw_path: Optional[str] = None,
) -> None:
    finished_at = datetime.now(timezone.utc).isoformat()
    with connect() as conn:
        conn.execute(
            """
            UPDATE source_runs
            SET finished_at = ?, status = ?, message = ?, records_ingested = ?, raw_path = COALESCE(?, raw_path)
            WHERE run_id = ?
            """,
            (finished_at, status, message, records_ingested, raw_path, run_id),
        )


def record_source_file(
    source_id: int,
    *,
    storage_path: str,
    captured_at: Optional[str] = None,
    hash_value: Optional[str] = None,
    season: Optional[int] = None,
    metadata: Optional[Mapping[str, Any]] = None,
) -> None:
    captured = captured_at or datetime.now(timezone.utc).isoformat()
    metadata_json = json.dumps(metadata) if metadata else None

    with connect() as conn:
        conn.execute(
            """
            INSERT INTO source_files (source_id, captured_at, storage_path, hash, season, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(source_id, storage_path) DO UPDATE SET
                captured_at = excluded.captured_at,
                hash = excluded.hash,
                season = COALESCE(excluded.season, source_files.season),
                metadata_json = COALESCE(excluded.metadata_json, source_files.metadata_json)
            """,
            (source_id, captured, storage_path, hash_value, season, metadata_json),
        )


def has_successful_source_run(source_key: str) -> bool:
    """Return True if the source has at least one successful run recorded."""

    with connect() as conn:
        row = conn.execute(
            """
            SELECT 1
            FROM source_runs sr
            JOIN data_sources ds ON ds.source_id = sr.source_id
            WHERE ds.source_key = ?
              AND sr.status = 'success'
            ORDER BY sr.finished_at DESC
            LIMIT 1
            """,
            (source_key,),
        ).fetchone()
    return row is not None


def resolve_source(source_key: str) -> Optional[int]:
    with connect() as conn:
        row = conn.execute(
            "SELECT source_id FROM data_sources WHERE source_key = ?",
            (source_key,),
        ).fetchone()
        return row[0] if row else None


def store_injury_reports(df: pd.DataFrame, *, league: str, source_key: str) -> int:
    if df.empty:
        LOGGER.info("No injury rows to store for league %s", league)
        return 0

    config = SPORT_CONFIG_BY_LEAGUE.get(league.upper())
    if not config:
        raise ValueError(f"Unsupported league for injuries: {league}")

    records = df.to_dict("records")
    stored = 0

    with connect() as conn:
        _ensure_odds_line_column(conn)
        sport_id = _ensure_sport(
            conn,
            name=config["sport_name"],
            league=config["league"],
            default_market=config["default_market"],
        )

        for row in records:
            player_name = (row.get("player_name") or "").strip()
            if not player_name:
                continue

            team_code_raw = row.get("team_code") or row.get("team")
            team_code = normalize_team_code(league, str(team_code_raw) if team_code_raw else None)
            team_name = row.get("team_name") or team_code
            team_id = None
            if team_code:
                team_id = _ensure_team(conn, sport_id, team_code, team_name or team_code)

            detail_parts = [
                str(row.get("detail") or ""),
                str(row.get("notes") or ""),
            ]
            detail_text = "; ".join(part for part in detail_parts if part and part.lower() != "nan") or None

            try:
                season_value = int(row.get("season")) if row.get("season") not in (None, "") else None
            except (TypeError, ValueError):
                season_value = None
            try:
                week_value = int(row.get("week")) if row.get("week") not in (None, "") else None
            except (TypeError, ValueError):
                week_value = None

            conn.execute(
                """
                INSERT INTO injury_reports (
                    league, sport_id, team_id, team_code, season, week,
                    player_name, position, status,
                    practice_status, report_date, game_date, detail, source_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(league, player_name, report_date, source_key) DO UPDATE SET
                    team_id = COALESCE(excluded.team_id, injury_reports.team_id),
                    team_code = COALESCE(excluded.team_code, injury_reports.team_code),
                    season = COALESCE(excluded.season, injury_reports.season),
                    week = COALESCE(excluded.week, injury_reports.week),
                    position = COALESCE(excluded.position, injury_reports.position),
                    status = COALESCE(excluded.status, injury_reports.status),
                    practice_status = COALESCE(excluded.practice_status, injury_reports.practice_status),
                    game_date = COALESCE(excluded.game_date, injury_reports.game_date),
                    detail = COALESCE(excluded.detail, injury_reports.detail),
                    source_key = excluded.source_key
                """,
                (
                    league.upper(),
                    sport_id,
                    team_id,
                    team_code or None,
                    season_value,
                    week_value,
                    player_name,
                    (row.get("position") or None),
                    (row.get("status") or None),
                    (row.get("practice_status") or None),
                    (row.get("report_date") or None),
                    (row.get("game_date") or None),
                    detail_text,
                    source_key,
                ),
            )
            stored += 1

    LOGGER.info("Stored %d injury rows for league %s", stored, league)
    return stored


def _find_game_by_details(
    conn,
    sport_id: int,
    home_team_id: int,
    away_team_id: int,
    start_iso: Optional[str],
    odds_api_id: Optional[str],
) -> Optional[str]:
    if odds_api_id:
        # First try exact odds_api_id match
        row = conn.execute(
            "SELECT game_id FROM games WHERE odds_api_id = ?",
            (odds_api_id,),
        ).fetchone()
        if row:
            return row[0]
        
        # For NBA, try matching by game_id pattern (NBA_<event_id>)
        # Check if any game_id matches the pattern where event_id is in the game_id
        row = conn.execute(
            "SELECT game_id FROM games WHERE sport_id = ? AND game_id LIKE ?",
            (sport_id, f"%_{odds_api_id}"),
        ).fetchone()
        if row:
            return row[0]
        
        # Also try direct match if game_id is exactly "NBA_<event_id>" or "NFL_<event_id>"
        for league_prefix in ["NBA_", "NFL_"]:
            potential_game_id = f"{league_prefix}{odds_api_id}"
            row = conn.execute(
                "SELECT game_id FROM games WHERE game_id = ?",
                (potential_game_id,),
            ).fetchone()
            if row:
                return row[0]

    if start_iso:
        row = conn.execute(
            """
            SELECT game_id FROM games
            WHERE sport_id = ? AND home_team_id = ? AND away_team_id = ? AND start_time_utc = ?
            """,
            (sport_id, home_team_id, away_team_id, start_iso),
        ).fetchone()
        if row:
            return row[0]

    row = conn.execute(
        """
        SELECT game_id FROM games
        WHERE sport_id = ? AND home_team_id = ? AND away_team_id = ?
        ORDER BY start_time_utc DESC
        LIMIT 1
        """,
        (sport_id, home_team_id, away_team_id),
    ).fetchone()
    if row:
        return row[0]
    return None


SPORT_CONFIG = {
    "americanfootball_nfl": {"league": "NFL", "sport_name": "Football", "default_market": "moneyline"},
    "americanfootball_ncaaf": {"league": "CFB", "sport_name": "Football", "default_market": "moneyline"},
    "basketball_nba": {"league": "NBA", "sport_name": "Basketball", "default_market": "moneyline"},
    "baseball_mlb": {"league": "MLB", "sport_name": "Baseball", "default_market": "moneyline"},
    "soccer_epl": {"league": "EPL", "sport_name": "Soccer", "default_market": "moneyline"},
    "soccer_spain_la_liga": {"league": "LALIGA", "sport_name": "Soccer", "default_market": "moneyline"},
    "soccer_germany_bundesliga": {"league": "BUNDESLIGA", "sport_name": "Soccer", "default_market": "moneyline"},
    "soccer_italy_serie_a": {"league": "SERIEA", "sport_name": "Soccer", "default_market": "moneyline"},
    "soccer_france_ligue_one": {"league": "LIGUE1", "sport_name": "Soccer", "default_market": "moneyline"},
}

SPORT_CONFIG_BY_LEAGUE = {config["league"].upper(): config for config in SPORT_CONFIG.values()}


def load_odds_snapshot(
    payload: Dict[str, Any],
    raw_path: Optional[str] = None,
    region: Optional[str] = None,
    sport_key: Optional[str] = None,
) -> None:
    results = payload.get("results", [])
    if not results:
        LOGGER.info("No odds data to load")
        return

    fetched_at = payload.get("fetched_at")
    if fetched_at:
        try:
            fetched_dt = datetime.fromisoformat(fetched_at.replace("Z", "+00:00"))
        except ValueError:
            fetched_dt = datetime.now(timezone.utc)
    else:
        fetched_dt = datetime.now(timezone.utc)

    snapshot_id = uuid.uuid4().hex

    config = SPORT_CONFIG.get((sport_key or "americanfootball_nfl").lower(), SPORT_CONFIG["americanfootball_nfl"])

    with connect() as conn:
        sport_id = _ensure_sport(
            conn,
            name=config["sport_name"],
            league=config["league"],
            default_market=config["default_market"],
        )
        source = payload.get("source", "the-odds-api")
        conn.execute(
            "INSERT INTO odds_snapshots (snapshot_id, fetched_at_utc, sport_id, source, raw_path) VALUES (?, ?, ?, ?, ?)",
            (snapshot_id, fetched_dt.isoformat(), sport_id, source, raw_path),
        )

        for event in results:
            commence_time = event.get("commence_time")
            try:
                commence_iso = datetime.fromisoformat(commence_time.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
            except Exception:  # noqa: BLE001
                commence_iso = None

            home_name = event.get("home_team", "")
            away_name = event.get("away_team", "")
            home_code = normalize_team_code(config["league"], home_name)
            away_code = normalize_team_code(config["league"], away_name)

            home_team_id = _ensure_team(conn, sport_id, home_code, event.get("home_team"))
            away_team_id = _ensure_team(conn, sport_id, away_code, event.get("away_team"))

            game_id = _find_game_by_details(
                conn,
                sport_id=sport_id,
                home_team_id=home_team_id,
                away_team_id=away_team_id,
                start_iso=commence_iso,
                odds_api_id=event.get("id"),
            )

            if not game_id:
                base_id = event.get("id") or uuid.uuid4().hex
                game_id = f"{config['league']}_{base_id}" if not base_id.upper().startswith(config["league"]) else base_id
                season = commence_iso and datetime.fromisoformat(commence_iso).year
                conn.execute(
                    """
                    INSERT INTO games (
                        game_id, sport_id, season, game_type, week, start_time_utc,
                        home_team_id, away_team_id, venue, status, odds_api_id
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(game_id) DO UPDATE SET
                        start_time_utc = excluded.start_time_utc,
                        odds_api_id = excluded.odds_api_id
                    """,
                    (
                        game_id,
                        sport_id,
                        season,
                        event.get("sport_title"),
                        None,
                        commence_iso,
                        home_team_id,
                        away_team_id,
                        None,
                        "scheduled",
                        event.get("id"),
                    ),
                )
            else:
                conn.execute(
                    "UPDATE games SET odds_api_id = COALESCE(?, odds_api_id) WHERE game_id = ?",
                    (event.get("id"), game_id),
                )
            
            # Extract moneylines from first bookmaker to update game_results
            # We'll collect all outcomes first, then determine home/away based on the outcome_key logic
            home_ml_close = None
            away_ml_close = None
            for bookmaker in event.get("bookmakers", []):
                for market in bookmaker.get("markets", []):
                    if market.get("key") == "h2h":
                        for outcome in market.get("outcomes", []):
                            price = outcome.get("price")
                            american = _safe_float(price)
                            if american is None:
                                continue
                            outcome_name = outcome.get("name", "").strip()
                            # Use the same logic as below to determine home/away
                            if outcome_name.lower() == home_name.lower():
                                home_ml_close = american
                            elif outcome_name.lower() == away_name.lower():
                                away_ml_close = american
                            # Also try matching by outcome_key (which will be set as "home"/"away" below)
                        break
                if home_ml_close is not None and away_ml_close is not None:
                    break
            
            # Track outcomes for later game_results update
            outcomes_by_key = {}
            total_close_value = None
            
            for bookmaker in event.get("bookmakers", []):
                book_title = bookmaker.get("title") or bookmaker.get("key")
                book_id = _get_or_create_book(conn, book_title, region)
                for market in bookmaker.get("markets", []):
                    market_key = (market.get("key") or "").lower()
                    for outcome in market.get("outcomes", []):
                        price = outcome.get("price")
                        american = _safe_float(price)

                        outcome_name = outcome.get("name", "").strip()
                        outcome_key = None
                        if outcome_name.lower() == event.get("home_team", "").lower():
                            outcome_key = "home"
                        elif outcome_name.lower() == event.get("away_team", "").lower():
                            outcome_key = "away"
                        else:
                            outcome_key = outcome_name

                        # Track moneyline for home/away outcomes
                        if market_key == "h2h" and outcome_key in ("home", "away") and american is not None:
                            outcomes_by_key[outcome_key] = american

                        line_value = _safe_float(outcome.get("point"))
                        if market_key == "totals" and line_value is not None and total_close_value is None:
                            total_close_value = line_value

                        conn.execute(
                            """
                            INSERT OR REPLACE INTO odds (
                                snapshot_id, game_id, book_id, market, outcome,
                                price_american, price_decimal, implied_prob, line
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                snapshot_id,
                                game_id,
                                book_id,
                                market_key,
                                outcome_key,
                                american,
                                _american_to_decimal(american) if american is not None else None,
                                _implied_probability(american) if american is not None else None,
                                line_value,
                            ),
                        )
            
            # Update game_results with moneylines from outcomes (after processing all bookmakers)
            if outcomes_by_key or total_close_value is not None:
                home_ml = outcomes_by_key.get("home")
                away_ml = outcomes_by_key.get("away")
                total_close_norm = _safe_float(total_close_value)
                LOGGER.debug(
                    "Game %s: outcomes_by_key=%s, home_ml=%s, away_ml=%s, total_close=%s",
                    game_id, outcomes_by_key, home_ml, away_ml, total_close_norm
                )
                if home_ml is None and away_ml is None and total_close_norm is None:
                    LOGGER.debug("Game %s: no moneyline or totals data available to persist", game_id)
                else:
                    existing = conn.execute(
                        "SELECT game_id FROM game_results WHERE game_id = ?",
                        (game_id,),
                    ).fetchone()

                    if existing:
                        LOGGER.debug("Updating game_results for game_id %s with odds data", game_id)
                        conn.execute(
                            """
                            UPDATE game_results
                            SET home_moneyline_close = COALESCE(?, home_moneyline_close),
                                away_moneyline_close = COALESCE(?, away_moneyline_close),
                        total_close = COALESCE(?, total_close)
                            WHERE game_id = ?
                            """,
                            (_normalize_moneyline(home_ml), _normalize_moneyline(away_ml), total_close_norm, game_id),
                        )
                    else:
                        LOGGER.debug("Inserting game_results for game_id %s with odds data", game_id)
                        conn.execute(
                            """
                            INSERT INTO game_results (
                                game_id, home_moneyline_close, away_moneyline_close, total_close, source_version
                            ) VALUES (?, ?, ?, ?, ?)
                            """,
                            (
                                game_id,
                                _normalize_moneyline(home_ml),
                                _normalize_moneyline(away_ml),
                                total_close_norm,
                                source,
                            ),
                        )
            else:
                LOGGER.debug("Game %s: no outcome data captured for odds snapshot", game_id)

        LOGGER.info("Stored odds snapshot %s with %d events", snapshot_id, len(results))


FOOTBALL_DATA_SOURCE = "football-data"
TEAMRANKINGS_SOURCE = "teamrankings"

FOOTBALL_DATA_LEAGUE_DIRS: Dict[str, str] = {
    "premier-league": "EPL",
    "la-liga": "LALIGA",
    "bundesliga": "BUNDESLIGA",
    "serie-a": "SERIEA",
    "ligue-1": "LIGUE1",
}

TEAMRANKINGS_FILE_PREFIXES: Dict[str, str] = {
    "nba": "NBA",
    "nfl": "NFL",
    "cfb": "CFB",
}


def _resolve_league_directories(
    leagues: Optional[Sequence[str]],
    mapping: Mapping[str, str],
) -> Sequence[tuple[str, str]]:
    """Return (directory_name, league_code) pairs for football-data inputs."""
    if not leagues:
        return list(mapping.items())

    reverse = {league.upper(): directory for directory, league in mapping.items()}
    resolved: list[tuple[str, str]] = []
    seen: set[str] = set()
    for entry in leagues:
        if not entry:
            continue
        entry_lower = entry.lower()
        entry_upper = entry.upper()
        if entry_lower in mapping:
            league_code = mapping[entry_lower]
            if league_code not in seen:
                resolved.append((entry_lower, league_code))
                seen.add(league_code)
            continue
        if entry_upper in reverse:
            league_code = entry_upper
            directory = reverse[entry_upper]
            if league_code not in seen:
                resolved.append((directory, league_code))
                seen.add(league_code)
            continue
        raise ValueError(
            f"Unknown league '{entry}'. Available: {sorted(set(mapping.values()))}"
        )
    return resolved


def _coerce_to_utc_datetime(value: Any, *, dayfirst: bool = False) -> Optional[datetime]:
    """Best-effort conversion to timezone-aware UTC datetime."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    if isinstance(value, pd.Timestamp):
        dt_value = value.to_pydatetime()
    elif isinstance(value, datetime):
        dt_value = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            dt_value = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            parsed = pd.to_datetime(text, errors="coerce", dayfirst=dayfirst)
            if pd.isna(parsed):
                return None
            dt_value = parsed.to_pydatetime()
    else:
        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            return None
        if isinstance(parsed, pd.Timestamp):
            dt_value = parsed.to_pydatetime()
        elif isinstance(parsed, datetime):
            dt_value = parsed
        else:
            return None

    if dt_value.tzinfo is None:
        return dt_value.replace(tzinfo=timezone.utc)
    return dt_value.astimezone(timezone.utc)


def _parse_football_data_datetime(row: Mapping[str, Any]) -> Optional[datetime]:
    """Extract the kickoff datetime from a football-data row."""
    direct_keys = ("start_time_utc", "start_time", "kickoff_utc", "match_datetime")
    for key in direct_keys:
        if key in row:
            dt_value = _coerce_to_utc_datetime(row.get(key))
            if dt_value:
                return dt_value

    date_value = row.get("match_date")
    dt_value = _coerce_to_utc_datetime(date_value) if date_value is not None else None
    if not dt_value:
        date_value = row.get("Date") or row.get("date")
        dt_value = _coerce_to_utc_datetime(date_value, dayfirst=True)
    if not dt_value:
        return None

    time_value = row.get("Time") or row.get("kickoff_time")
    hour = 0
    minute = 0
    if isinstance(time_value, str):
        time_text = time_value.strip()
        if time_text:
            normalized = time_text.replace(".", ":")
            parts = normalized.split(":")
            try:
                hour = int(parts[0])
                minute = int(parts[1]) if len(parts) > 1 else 0
            except ValueError:
                hour = minute = 0
    elif isinstance(time_value, (int, float)) and not pd.isna(time_value):
        # Handle times like 1900 meaning 19:00
        value_str = str(int(time_value))
        if len(value_str) in {3, 4}:
            hour = int(value_str[:-2])
            minute = int(value_str[-2:])

    dt_value = dt_value.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if dt_value.tzinfo is None:
        return dt_value.replace(tzinfo=timezone.utc)
    return dt_value.astimezone(timezone.utc)


def _parse_total_line_from_column(column_name: str) -> Optional[float]:
    for delimiter in (">", "<"):
        if delimiter in column_name:
            try:
                return float(column_name.split(delimiter, 1)[1])
            except ValueError:
                continue
    return None


def _extract_football_total_line(row: Mapping[str, Any]) -> Optional[float]:
    """Best-effort extraction of a totals line from football-data columns."""
    explicit_keys = ("total_line", "TotalLine", "closing_total_line")
    for key in explicit_keys:
        if key in row:
            total = _safe_float(row.get(key))
            if total is not None:
                return total

    candidate_columns = (
        "AvgC>2.5",
        "Avg>2.5",
        "B365C>2.5",
        "B365>2.5",
        "MaxC>2.5",
        "Max>2.5",
    )
    for column in candidate_columns:
        if column in row:
            total = _parse_total_line_from_column(column)
            if total is not None:
                return total
    return None


def _find_game_by_codes(
    conn,
    *,
    league: str,
    home_code: str,
    away_code: str,
    target_ts: Optional[int],
    tolerance_seconds: int,
    target_date: Optional[str],
) -> Optional[str]:
    """Locate a game_id by team codes and approximate start time."""
    league_upper = league.upper()
    if target_ts is not None:
        row = conn.execute(
            """
            SELECT g.game_id,
                   ABS(strftime('%s', g.start_time_utc) - ?) AS delta
            FROM games g
            JOIN sports s ON g.sport_id = s.sport_id
            JOIN teams th ON g.home_team_id = th.team_id
            JOIN teams ta ON g.away_team_id = ta.team_id
            WHERE s.league = ?
              AND th.code = ?
              AND ta.code = ?
              AND g.start_time_utc IS NOT NULL
              AND ABS(strftime('%s', g.start_time_utc) - ?) <= ?
            ORDER BY delta
            LIMIT 1
            """,
            (target_ts, league_upper, home_code, away_code, target_ts, tolerance_seconds),
        ).fetchone()
        if row:
            return row[0]

    if target_date:
        row = conn.execute(
            """
            SELECT g.game_id
            FROM games g
            JOIN sports s ON g.sport_id = s.sport_id
            JOIN teams th ON g.home_team_id = th.team_id
            JOIN teams ta ON g.away_team_id = ta.team_id
            WHERE s.league = ?
              AND th.code = ?
              AND ta.code = ?
              AND date(g.start_time_utc) = ?
            ORDER BY g.start_time_utc DESC
            LIMIT 1
            """,
            (league_upper, home_code, away_code, target_date),
        ).fetchone()
        if row:
            return row[0]

    row = conn.execute(
        """
        SELECT g.game_id
        FROM games g
        JOIN sports s ON g.sport_id = s.sport_id
        JOIN teams th ON g.home_team_id = th.team_id
        JOIN teams ta ON g.away_team_id = ta.team_id
        WHERE s.league = ?
          AND th.code = ?
          AND ta.code = ?
        ORDER BY g.start_time_utc DESC
        LIMIT 1
        """,
        (league_upper, home_code, away_code),
    ).fetchone()
    if row:
        return row[0]

    return None


def import_football_data_totals(
    leagues: Optional[Sequence[str]] = None,
    *,
    base_dir: str | Path | None = None,
    tolerance_seconds: int = 6 * 3600,
) -> Dict[str, Dict[str, int]]:
    """
    Load historical totals and scores from football-data.co.uk parquet exports.
    """
    base_path = Path(base_dir or Path("data/processed/external/football_data"))
    summary: Dict[str, Dict[str, int]] = {}

    try:
        targets = _resolve_league_directories(leagues, FOOTBALL_DATA_LEAGUE_DIRS)
    except ValueError as exc:  # pragma: no cover - defensive
        LOGGER.error("%s", exc)
        return summary

    if not base_path.exists():
        LOGGER.warning("Football-data directory missing: %s", base_path)
        return summary

    with connect() as conn:
        for directory, league_code in targets:
            league_path = base_path / directory
            files = sorted(league_path.glob("*.parquet")) if league_path.exists() else []
            league_stats = summary.setdefault(
                league_code,
                {"files": 0, "rows": 0, "matched": 0, "unmatched": 0},
            )
            if not files:
                LOGGER.info(
                    "football-data %s: no parquet files found under %s",
                    league_code,
                    league_path,
                )
                continue

            league_stats["files"] += len(files)
            for parquet_file in files:
                try:
                    df = pd.read_parquet(parquet_file)
                except Exception as exc:  # noqa: BLE001
                    LOGGER.exception(
                        "football-data %s: failed to read %s (%s)",
                        league_code,
                        parquet_file,
                        exc,
                    )
                    continue

                total_rows = len(df)
                matched = 0
                unmatched = 0
                if total_rows == 0:
                    LOGGER.info(
                        "football-data %s: %s is empty",
                        league_code,
                        parquet_file.name,
                    )
                else:
                    for record in df.to_dict(orient="records"):
                        home_name = record.get("home_team") or record.get("HomeTeam")
                        away_name = record.get("away_team") or record.get("AwayTeam")
                        if not home_name or not away_name:
                            unmatched += 1
                            continue

                        home_code = normalize_team_code(league_code, home_name)
                        away_code = normalize_team_code(league_code, away_name)
                        if not home_code or not away_code:
                            unmatched += 1
                            continue

                        game_dt = _parse_football_data_datetime(record)
                        if not game_dt:
                            unmatched += 1
                            continue

                        game_ts = int(game_dt.timestamp())
                        game_id = _find_game_by_codes(
                            conn,
                            league=league_code,
                            home_code=home_code,
                            away_code=away_code,
                            target_ts=game_ts,
                            tolerance_seconds=tolerance_seconds,
                            target_date=game_dt.date().isoformat(),
                        )
                        if not game_id:
                            LOGGER.warning(
                                "football-data %s: unmatched row %s vs %s on %s",
                                league_code,
                                home_name,
                                away_name,
                                game_dt.date().isoformat(),
                            )
                            unmatched += 1
                            continue

                        spread_close = _safe_float(record.get("AHCh")) or _safe_float(
                            record.get("AHh")
                        )
                        total_close = _extract_football_total_line(record)
                        home_score = _normalize_score(record.get("FTHG"))
                        away_score = _normalize_score(record.get("FTAG"))

                        conn.execute(
                            """
                            INSERT INTO game_results (
                                game_id, home_score, away_score, total_close, spread_close, source_version
                            ) VALUES (?, ?, ?, ?, ?, ?)
                            ON CONFLICT(game_id) DO UPDATE SET
                                home_score = COALESCE(excluded.home_score, game_results.home_score),
                                away_score = COALESCE(excluded.away_score, game_results.away_score),
                                total_close = COALESCE(excluded.total_close, game_results.total_close),
                                spread_close = COALESCE(excluded.spread_close, game_results.spread_close),
                                source_version = excluded.source_version
                            """,
                            (
                                game_id,
                                home_score,
                                away_score,
                                total_close,
                                spread_close,
                                FOOTBALL_DATA_SOURCE,
                            ),
                        )
                        matched += 1

                league_stats["rows"] += total_rows
                league_stats["matched"] += matched
                league_stats["unmatched"] += unmatched
                LOGGER.info(
                    "football-data %s: %s matched %d/%d rows",
                    league_code,
                    parquet_file.name,
                    matched,
                    total_rows,
                )

    return summary


def _resolve_teamrankings_filters(leagues: Optional[Sequence[str]]) -> set[str]:
    if not leagues:
        return set()

    resolved: set[str] = set()
    for entry in leagues:
        if not entry:
            continue
        entry_lower = entry.lower()
        if entry_lower in TEAMRANKINGS_FILE_PREFIXES:
            resolved.add(TEAMRANKINGS_FILE_PREFIXES[entry_lower])
        else:
            resolved.add(entry.upper())
    return resolved


def _teamrankings_league_from_name(name: str) -> Optional[str]:
    lowered = name.lower()
    for prefix, league in TEAMRANKINGS_FILE_PREFIXES.items():
        if lowered.startswith(prefix):
            return league
    return None


def import_teamrankings_picks(
    *,
    leagues: Optional[Sequence[str]] = None,
    base_dir: str | Path | None = None,
    tolerance_seconds: int = 6 * 3600,
) -> Dict[str, Dict[str, int]]:
    """
    Attach TeamRankings over/under picks to existing games.
    """
    base_path = Path(base_dir or Path("data/processed/external/teamrankings"))
    summary: Dict[str, Dict[str, int]] = {}
    if not base_path.exists():
        LOGGER.warning("teamrankings: directory missing %s", base_path)
        return summary

    files = sorted(base_path.glob("*.parquet"))
    if not files:
        LOGGER.info("teamrankings: no parquet files found in %s", base_path)
        return summary

    league_filters = _resolve_teamrankings_filters(leagues)

    with connect() as conn:
        _ensure_game_results_tr_columns(conn)

        for parquet_file in files:
            league_code = _teamrankings_league_from_name(parquet_file.stem)
            if not league_code:
                LOGGER.warning(
                    "teamrankings: unable to determine league from %s",
                    parquet_file.name,
                )
                continue

            if league_filters and league_code not in league_filters:
                continue

            try:
                df = pd.read_parquet(parquet_file)
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception(
                    "teamrankings %s: failed to read %s (%s)",
                    league_code,
                    parquet_file,
                    exc,
                )
                continue

            stats = summary.setdefault(
                league_code,
                {"files": 0, "rows": 0, "updates": 0, "unmatched": 0},
            )
            stats["files"] += 1
            stats["rows"] += len(df)

            if df.empty:
                LOGGER.info(
                    "teamrankings %s: %s is empty",
                    league_code,
                    parquet_file.name,
                )
                continue

            matched = 0
            unmatched = 0
            for record in df.to_dict(orient="records"):
                home_name = record.get("home_team")
                away_name = record.get("away_team")
                if not home_name or not away_name:
                    unmatched += 1
                    continue

                home_code = normalize_team_code(league_code, home_name)
                away_code = normalize_team_code(league_code, away_name)
                if not home_code or not away_code:
                    unmatched += 1
                    continue

                game_dt = _coerce_to_utc_datetime(record.get("game_date"))
                if not game_dt:
                    unmatched += 1
                    continue

                game_id = _find_game_by_codes(
                    conn,
                    league=league_code,
                    home_code=home_code,
                    away_code=away_code,
                    target_ts=int(game_dt.timestamp()),
                    tolerance_seconds=tolerance_seconds,
                    target_date=game_dt.date().isoformat(),
                )
                if not game_id:
                    LOGGER.warning(
                        "teamrankings %s: unmatched pick %s at %s on %s",
                        league_code,
                        away_name,
                        home_name,
                        game_dt.date().isoformat(),
                    )
                    unmatched += 1
                    continue

                conn.execute(
                    "INSERT OR IGNORE INTO game_results (game_id, source_version) VALUES (?, ?)",
                    (game_id, TEAMRANKINGS_SOURCE),
                )

                retrieved_at_raw = record.get("retrieved_at")
                if isinstance(retrieved_at_raw, str):
                    retrieved_at_value = retrieved_at_raw
                elif retrieved_at_raw:
                    retrieved_dt = _coerce_to_utc_datetime(retrieved_at_raw)
                    retrieved_at_value = retrieved_dt.isoformat() if retrieved_dt else None
                else:
                    retrieved_at_value = None

                conn.execute(
                    """
                    UPDATE game_results
                    SET tr_pick = ?,
                        tr_total_line = ?,
                        tr_confidence = ?,
                        tr_odds = ?,
                        tr_model_pick = ?,
                        tr_model_prob = ?,
                        tr_retrieved_at = ?
                    WHERE game_id = ?
                    """,
                    (
                        record.get("pick"),
                        _safe_float(record.get("total_line")),
                        _safe_float(record.get("confidence_value")),
                        _safe_float(record.get("odds_value")),
                        record.get("model_pick") or record.get("similar_games_pick"),
                        _safe_float(
                            record.get("model_prob") or record.get("similar_games_prob")
                        ),
                        retrieved_at_value,
                        game_id,
                    ),
                )
                matched += 1

            stats["updates"] += matched
            stats["unmatched"] += unmatched
            LOGGER.info(
                "teamrankings %s: %s matched %d/%d rows",
                league_code,
                parquet_file.name,
                matched,
                len(df),
            )

    return summary


def import_sbr_odds(
    *,
    league: str = "NBA",
    seasons: Optional[Sequence[str]] = None,
    base_dir: str | Path | None = None,
    tolerance_seconds: int = 24 * 3600,
) -> Dict[str, Dict[str, int]]:
    """
    Load historical odds from SportsbookReviewOnline into game_results.
    """
    base_path = Path(base_dir or Path("data/processed/external/sbr")) / league.lower()
    if not base_path.exists():
        LOGGER.warning("SBR directory missing: %s", base_path)
        return {}

    if seasons:
        season_files = [(season, base_path / f"{season}.parquet") for season in seasons]
    else:
        season_files = sorted((path.stem, path) for path in base_path.glob("*.parquet"))

    summary: Dict[str, Dict[str, int]] = {}
    if not season_files:
        LOGGER.info("No SBR parquet files found in %s", base_path)
        return summary

    with connect() as conn:
        for season, parquet_file in season_files:
            stats = summary.setdefault(season, {"rows": 0, "matched": 0, "unmatched": 0})
            if not parquet_file.exists():
                LOGGER.warning("SBR file missing: %s", parquet_file)
                continue

            try:
                df = pd.read_parquet(parquet_file)
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("Failed to read %s: %s", parquet_file, exc)
                continue

            if df.empty:
                LOGGER.info("SBR %s %s is empty", league, season)
                continue

            if not pd.api.types.is_datetime64_any_dtype(df["game_date"]):
                df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce", utc=True)

            stats["rows"] += len(df)
            for row in df.itertuples(index=False):
                home_name = getattr(row, "home_team", "")
                away_name = getattr(row, "visitor_team", "")
                if not home_name or not away_name:
                    stats["unmatched"] += 1
                    continue

                home_code = normalize_team_code(league, home_name)
                away_code = normalize_team_code(league, away_name)
                if not home_code or not away_code:
                    LOGGER.warning("SBR %s %s: team normalization failed (%s vs %s)", league, season, away_name, home_name)
                    stats["unmatched"] += 1
                    continue

                game_dt = getattr(row, "game_date", None)
                if pd.isna(game_dt):
                    stats["unmatched"] += 1
                    continue
                game_dt = pd.to_datetime(game_dt, utc=True)
                target_date = game_dt.date().isoformat()

                game_id = _find_game_by_codes(
                    conn,
                    league=league,
                    home_code=home_code,
                    away_code=away_code,
                    target_ts=None,
                    tolerance_seconds=tolerance_seconds,
                    target_date=target_date,
                )
                if not game_id:
                    LOGGER.warning(
                        "SBR %s %s: unmatched game %s vs %s on %s",
                        league,
                        season,
                        away_name,
                        home_name,
                        target_date,
                    )
                    stats["unmatched"] += 1
                    continue

                home_score = _normalize_score(getattr(row, "home_score", None))
                away_score = _normalize_score(getattr(row, "visitor_score", None))
                spread_close = _safe_float(getattr(row, "spread_close", None))
                total_close = _safe_float(getattr(row, "total_close", None))
                home_ml = _normalize_moneyline(getattr(row, "home_moneyline", None))
                away_ml = _normalize_moneyline(getattr(row, "visitor_moneyline", None))

                conn.execute(
                    """
                    INSERT INTO game_results (
                        game_id,
                        home_score,
                        away_score,
                        home_moneyline_close,
                        away_moneyline_close,
                        spread_close,
                        total_close,
                        source_version
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'sbr')
                    ON CONFLICT(game_id) DO UPDATE SET
                        home_score = COALESCE(excluded.home_score, game_results.home_score),
                        away_score = COALESCE(excluded.away_score, game_results.away_score),
                        home_moneyline_close = COALESCE(excluded.home_moneyline_close, game_results.home_moneyline_close),
                        away_moneyline_close = COALESCE(excluded.away_moneyline_close, game_results.away_moneyline_close),
                        spread_close = COALESCE(excluded.spread_close, game_results.spread_close),
                        total_close = COALESCE(excluded.total_close, game_results.total_close),
                        source_version = excluded.source_version
                    """,
                    (
                        game_id,
                        home_score,
                        away_score,
                        home_ml,
                        away_ml,
                        spread_close,
                        total_close,
                    ),
                )
                stats["matched"] += 1

    return summary
