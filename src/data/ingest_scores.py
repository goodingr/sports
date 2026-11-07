"""Fetch recent scores from The Odds API and update the database."""

import argparse
import json
import logging
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests

from src.data.config import RAW_DATA_DIR, OddsAPIKeyManager, OddsAPISettings
from src.data.team_mappings import NBA_ALIASES
from src.db.core import connect
from src.db.loaders import _parse_utc_datetime
from src.predict.config import LEAGUE_SPORT_KEYS, SUPPORTED_LEAGUES

_NBA_SCORE_SOURCE = "the-odds-api-scores"
_NBA_TEAM_CODES = {**NBA_ALIASES, **{code.lower(): code for code in NBA_ALIASES.values()}}
_NBA_CANCELLED = {"canceled", "cancelled", "no_contest", "voided"}


def _get_sport_key(league: str) -> str:
    try:
        return LEAGUE_SPORT_KEYS[league.upper()]
    except KeyError as exc:
        raise ValueError(f"Unknown league: {league}") from exc

LOGGER = logging.getLogger(__name__)

def fetch_scores(league: str, days_from: int = 3, dotenv_path: Optional[Path] = None) -> List[Dict]:
    """Fetch recent scores for a league."""
    try:
        settings = OddsAPISettings.from_env(dotenv_path)
    except RuntimeError as exc:
        LOGGER.warning("Unable to load Odds API settings for %s scores: %s", league, exc)
        return []

    sport_key = _get_sport_key(league)
    url = f"{settings.base_url}/sports/{sport_key}/scores/"
    params = {
        "apiKey": settings.api_key,
        "daysFrom": min(max(1, days_from), 3), # API limit is usually 3 days for free/standard
        "dateFormat": "iso"
    }

    max_attempts = len(OddsAPIKeyManager.get_available_keys())
    for attempt in range(max_attempts):
        params["apiKey"] = OddsAPIKeyManager.get_current_key()
        
        try:
            response = requests.get(url, params=params, timeout=20)
            response.raise_for_status()
            return response.json()
        except requests.RequestException as exc:
            if isinstance(exc, requests.HTTPError) and response.status_code == 401:
                LOGGER.warning("Odds API returned 401 Unauthorized with key ending in ...%s", params["apiKey"][-4:])
                if attempt < max_attempts - 1:
                    LOGGER.info("Rotating API key and retrying...")
                    OddsAPIKeyManager.rotate_key()
                    continue
            
            # Non-401 error or exhausted retries
            LOGGER.warning("Failed to fetch %s scores from The Odds API: %s", league, exc)
            return []
    return []

def write_score_snapshot(data: List[Dict], league: str) -> None:
    """Save raw score data to data/raw/scores/<league>_<timestamp>.json."""
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    out_dir = RAW_DATA_DIR / "scores" / league
    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / f"scores_{timestamp}.json"
    output_path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    LOGGER.info("Saved raw scores to %s", output_path)


def _nba_final_scores(event: dict, names: tuple[str, str]) -> tuple[int, int] | None:
    scores = event.get("scores")
    if not isinstance(scores, list) or len(scores) != 2:
        return None
    values = {
        item["name"]: item.get("score")
        for item in scores
        if isinstance(item, dict) and isinstance(item.get("name"), str)
    }
    if values.keys() != set(names):
        return None
    raw_home, raw_away = (values[name] for name in names)
    if not all(
        type(value) is int or isinstance(value, str) and value.isdecimal()
        for value in (raw_home, raw_away)
    ):
        return None
    try:
        home, away = int(raw_home), int(raw_away)
    except ValueError:
        return None
    return (home, away) if 0 <= home < 2**63 and 0 <= away < 2**63 and home != away else None


def _update_nba_scores(scores_data: List[Dict]) -> int:
    received_by = datetime.now(timezone.utc)
    event_counts = Counter(
        event["id"]
        for event in scores_data
        if isinstance(event, dict) and isinstance(event.get("id"), str)
    )
    updated = 0
    with connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS nba_odds_api_score_state (
                game_id TEXT PRIMARY KEY REFERENCES games(game_id) ON DELETE CASCADE,
                provider_event_id TEXT NOT NULL,
                home_team_id INTEGER NOT NULL,
                away_team_id INTEGER NOT NULL,
                last_update_utc TEXT NOT NULL,
                completed INTEGER NOT NULL CHECK (completed IN (0, 1)),
                home_score INTEGER,
                away_score INTEGER,
                applied INTEGER NOT NULL CHECK (applied IN (0, 1))
            )
            """
        )
        for event in scores_data:
            if not isinstance(event, dict) or event.get("sport_key") != _get_sport_key("NBA"):
                continue
            event_id, completed = event.get("id"), event.get("completed")
            names = event.get("home_team"), event.get("away_team")
            if (
                not isinstance(event_id, str)
                or not event_id.strip()
                or event_counts[event_id] != 1
                or type(completed) is not bool
                or not all(isinstance(name, str) for name in names)
            ):
                continue
            home_code, away_code = (_NBA_TEAM_CODES.get(name.strip().lower()) for name in names)
            if not home_code or not away_code or home_code == away_code:
                continue
            try:
                observed = _parse_utc_datetime(event.get("last_update"), require_offset=True)
                kickoff = (
                    _parse_utc_datetime(event.get("commence_time"), require_offset=True)
                    if completed
                    else None
                )
            except OverflowError:
                continue
            if (
                observed is None
                or observed > received_by
                or (completed and (kickoff is None or observed <= kickoff))
            ):
                continue
            stamp = observed.isoformat(timespec="microseconds")
            scores = _nba_final_scores(event, names) if completed else (None, None)
            if scores is None:
                continue
            matches = conn.execute(
                """
                SELECT g.game_id, g.home_team_id, g.away_team_id, g.status,
                       h.code AS home_code, a.code AS away_code,
                       r.home_score, r.away_score, r.source_version
                FROM games g
                JOIN sports s ON s.sport_id = g.sport_id
                LEFT JOIN teams h ON h.team_id = g.home_team_id AND h.sport_id = g.sport_id
                LEFT JOIN teams a ON a.team_id = g.away_team_id AND a.sport_id = g.sport_id
                LEFT JOIN game_results r ON r.game_id = g.game_id
                WHERE s.league = 'NBA' AND g.odds_api_id = ?
                LIMIT 2
                """,
                (event_id,),
            ).fetchall()
            if len(matches) != 1:
                continue
            game = matches[0]
            status = (game["status"] or "").lower()
            if status in _NBA_CANCELLED or (game["home_code"], game["away_code"]) != (
                home_code,
                away_code,
            ):
                continue
            game_id = game["game_id"]
            previous = conn.execute(
                "SELECT * FROM nba_odds_api_score_state WHERE game_id = ?", (game_id,)
            ).fetchone()
            identity = event_id, game["home_team_id"], game["away_team_id"]
            if previous is not None and (
                stamp <= previous["last_update_utc"]
                or identity
                != (
                    previous["provider_event_id"],
                    previous["home_team_id"],
                    previous["away_team_id"],
                )
            ):
                continue
            current_scores = game["home_score"], game["away_score"]
            owns_result = (
                previous is not None
                and previous["applied"]
                and game["source_version"] == _NBA_SCORE_SOURCE
                and current_scores == (previous["home_score"], previous["away_score"])
            )
            # Observing a legacy score again does not establish who wrote it.
            applied = owns_result or (completed and current_scores == (None, None))
            if applied and (completed or previous["completed"]):
                new_status = "final" if completed else "in_progress"
                if scores != current_scores or new_status != status or not owns_result:
                    conn.execute(
                        """
                        INSERT INTO game_results (game_id, home_score, away_score, source_version)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(game_id) DO UPDATE SET
                            home_score = excluded.home_score,
                            away_score = excluded.away_score,
                            source_version = excluded.source_version
                        """,
                        (game_id, *scores, _NBA_SCORE_SOURCE),
                    )
                    conn.execute(
                        "UPDATE games SET status = ? WHERE game_id = ?", (new_status, game_id)
                    )
                    updated += 1
            conn.execute(
                """
                INSERT INTO nba_odds_api_score_state (
                    game_id, provider_event_id, home_team_id, away_team_id, last_update_utc,
                    completed, home_score, away_score, applied
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(game_id) DO UPDATE SET
                    last_update_utc = excluded.last_update_utc,
                    completed = excluded.completed,
                    home_score = excluded.home_score,
                    away_score = excluded.away_score,
                    applied = excluded.applied
                """,
                (game_id, *identity, stamp, completed, *scores, applied),
            )
    return updated


def update_database(scores_data: List[Dict], league: str) -> int:
    """Update game results in the database."""
    if not scores_data:
        return 0
    if league.upper() == "NBA":
        return _update_nba_scores(scores_data)
        
    updated_count = 0
    
    with connect() as conn:
        for event in scores_data:
            # Process both completed and live games
            is_completed = event.get("completed", False)
            
            # FAST FIX: User reported false live scores (e.g. 1-1 when 0-0).
            # Disabling live score ingestion for now. Only ingest FINAL scores.
            if not is_completed:
                 continue
            
            event_id = event.get("id")
            if not event_id:
                continue

            # Extract scores
            scores_list = event.get("scores") or []
            home_score = None
            away_score = None
            
            home_team = event.get("home_team")
            away_team = event.get("away_team")
            
            for entry in scores_list:
                name = entry.get("name")
                score_val = entry.get("score")
                if name is None or score_val is None:
                    continue
                try:
                    parsed = int(score_val)
                except (ValueError, TypeError):
                    continue
                    
                if name == home_team:
                    home_score = parsed
                elif name == away_team:
                    away_score = parsed
            
            # Fallback for 2-item list if names don't match exactly (sometimes happens)
            if (home_score is None or away_score is None) and len(scores_list) == 2:
                try:
                    # Assuming order matches home/away or we can't tell. 
                    # Actually, API usually returns them in order but relying on name is safer.
                    # If names fail, we skip to be safe, or check if one matched.
                    pass 
                except Exception:
                    pass

            if home_score is None or away_score is None:
                continue

            # Update DB
            # We match by odds_api_id (event_id)
            # We need to find the game_id first
            
            # First, check if we have this game by odds_api_id
            cursor = conn.execute(
                "SELECT game_id, status FROM games WHERE odds_api_id = ?",
                (event_id,)
            )
            row = cursor.fetchone()
            
            game_id = None
            if row:
                if str(row["status"] or "").lower() == "voided":
                    continue
                game_id = row[0]
            else:
                # Fallback: Match by teams and date?
                # For now, let's stick to odds_api_id as primary linkage for API data
                # If we ingested the game from API, it has the ID.
                LOGGER.debug("No game found for odds_api_id %s (Home: %s, Away: %s)", event_id, home_team, away_team)
                continue
                
            if game_id:
                # Upsert into game_results
                cursor = conn.execute(
                    """
                    INSERT INTO game_results (game_id, home_score, away_score)
                    SELECT game_id, ?, ?
                    FROM games
                    WHERE game_id = ? AND LOWER(COALESCE(status, 'scheduled')) != 'voided'
                    ON CONFLICT(game_id) DO UPDATE SET
                        home_score = excluded.home_score,
                        away_score = excluded.away_score
                    """,
                    (home_score, away_score, game_id)
                )
                if cursor.rowcount == 0:
                    continue
                
                # Update status
                new_status = 'final' if is_completed else 'in_progress'
                conn.execute(
                    "UPDATE games SET status = ? WHERE game_id = ?",
                    (new_status, game_id)
                )
                updated_count += 1
                
    return updated_count

def run(
    leagues: List[str],
    days_from: int = 3,
    dotenv_path: Optional[Path] = None,
    *,
    resolve_stale: bool = False,
    stale_lookback_days: int = 14,
) -> None:
    """Run ingestion for specified leagues."""
    total_updated = 0
    for league in leagues:
        LOGGER.info("Fetching scores for %s...", league)
        data = fetch_scores(league, days_from=days_from, dotenv_path=dotenv_path)
        if data:
            write_score_snapshot(data, league)
            count = update_database(data, league)
            LOGGER.info("Updated %d games for %s", count, league)
            total_updated += count
        else:
            LOGGER.info("No score data returned for %s", league)
            
    LOGGER.info("Total games updated: %d", total_updated)

    if resolve_stale:
        try:
            from src.data.score_backfill import resolve_stale_scores

            stale_updated = resolve_stale_scores(leagues, lookback_days=stale_lookback_days)
            LOGGER.info("Resolved %d stale games with ESPN score backfill", stale_updated)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Stale score resolution failed: %s", exc)

def main():
    parser = argparse.ArgumentParser(description="Ingest scores from The Odds API")
    parser.add_argument("--leagues", nargs="+", help="Specific leagues to update (default: all supported)")
    parser.add_argument("--days-from", type=int, default=3, help="Days back to fetch (max 3)")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    parser.add_argument("--dotenv", type=Path, default=None)
    parser.add_argument("--resolve-stale", action="store_true", help="Use ESPN scoreboard backfill for stale past games")
    parser.add_argument("--stale-lookback-days", type=int, default=14)
    
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))
    
    leagues = args.leagues or SUPPORTED_LEAGUES
    # Handle comma-separated strings if passed as a single argument
    if len(leagues) == 1 and "," in leagues[0]:
        leagues = [l.strip() for l in leagues[0].split(",")]
        
    run(
        leagues,
        days_from=args.days_from,
        dotenv_path=args.dotenv,
        resolve_stale=args.resolve_stale,
        stale_lookback_days=args.stale_lookback_days,
    )

if __name__ == "__main__":
    main()
