"""Backfill and resolve completed scores from ESPN scoreboard data."""

from __future__ import annotations

import argparse
import logging
import sqlite3
import time
from collections import Counter
from datetime import date, datetime, timedelta, timezone
from typing import Dict, Iterable, List, Optional, Tuple
from zoneinfo import ZoneInfo

import requests

from src.data.team_mappings import normalize_team_code
from src.db.core import connect
from src.predict.config import SUPPORTED_LEAGUES

LOGGER = logging.getLogger(__name__)

ESPN_BASE_URL = "https://site.api.espn.com/apis/site/v2/sports"
_SCORE_START_TOLERANCE = timedelta(minutes=10)
LEAGUE_PATH_MAP = {
    "NHL": "hockey/nhl",
    "NBA": "basketball/nba",
    "NCAAB": "basketball/mens-college-basketball",
    "CFB": "football/college-football",
    "NFL": "football/nfl",
    "EPL": "soccer/eng.1",
    "LALIGA": "soccer/esp.1",
    "BUNDESLIGA": "soccer/ger.1",
    "SERIEA": "soccer/ita.1",
    "LIGUE1": "soccer/fra.1",
}


def iter_dates(start_date: date, end_date: date) -> Iterable[date]:
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def fetch_espn_scoreboard(league: str, target_date: date, *, timeout: int = 30) -> Dict:
    league_upper = league.upper()
    path = LEAGUE_PATH_MAP.get(league_upper)
    if not path:
        raise ValueError(f"Unsupported ESPN score league: {league}")

    params: dict[str, str | int] = {"dates": target_date.strftime("%Y%m%d"), "limit": 1000}
    if league_upper == "NCAAB":
        params["groups"] = 50

    response = requests.get(f"{ESPN_BASE_URL}/{path}/scoreboard", params=params, timeout=timeout)
    response.raise_for_status()
    return response.json()


def parse_espn_event(event: Dict, league: str) -> Optional[Dict]:
    competition = (event.get("competitions") or [{}])[0]
    competitors = competition.get("competitors") or []
    if len(competitors) != 2:
        return None

    status_type = competition.get("status", event.get("status", {})).get("type", {})
    completed = bool(status_type.get("completed")) or status_type.get("state") == "post"
    nonstandard_status = str(status_type.get("name") or "").upper() in {
        "STATUS_ABANDONED",
        "STATUS_AWARDED",
        "STATUS_CANCELED",
        "STATUS_CANCELLED",
        "STATUS_FORFEIT",
        "STATUS_NO_CONTEST",
        "STATUS_POSTPONED",
        "STATUS_SUSPENDED",
    }
    if not completed or nonstandard_status:
        return None

    home_comp = next((item for item in competitors if item.get("homeAway") == "home"), None)
    away_comp = next((item for item in competitors if item.get("homeAway") == "away"), None)
    if not home_comp or not away_comp:
        return None

    home_raw = home_comp.get("team", {}).get("displayName")
    away_raw = away_comp.get("team", {}).get("displayName")
    home_code = normalize_team_code(league, home_raw)
    away_code = normalize_team_code(league, away_raw)
    if not home_code or not away_code:
        # An unmapped team blocks backfill of every game involving that team.
        # Log at WARNING so missing mappings surface in pipeline runs instead of
        # silently leaving missing_scores rows behind.
        LOGGER.warning(
            "Could not normalize ESPN teams for %s on %s: away=%r home=%r",
            league,
            event.get("date"),
            away_raw,
            home_raw,
        )
        return None

    try:
        home_score = int(home_comp.get("score"))
        away_score = int(away_comp.get("score"))
    except (TypeError, ValueError):
        return None

    return {
        "league": league.upper(),
        "home_team": home_code,
        "away_team": away_code,
        "home_score": home_score,
        "away_score": away_score,
        "date": event.get("date"),
        "espn_id": event.get("id"),
    }


def _score_fixture(
    conn: sqlite3.Connection, game: Dict, league: str, team_names: str,
) -> sqlite3.Row | None:
    start = _parse_start_time(game.get("date"), require_offset=True)
    teams = game.get("home_team"), game.get("away_team")
    if start is None or not all(isinstance(team, str) and team for team in teams) or teams[0] == teams[1]:
        return None
    event_id = game["espn_id"]
    # SQLite narrows the scan. Python checks exact UTC times, including legacy formats SQLite cannot read.
    candidates = conn.execute(
        f"""
        SELECT g.game_id, g.status, g.espn_id, g.start_time_utc,
               ht.code AS home_code, at.code AS away_code, {team_names}
        FROM games g
        JOIN sports s ON s.sport_id = g.sport_id
        LEFT JOIN teams ht ON ht.team_id = g.home_team_id AND ht.sport_id = g.sport_id
        LEFT JOIN teams at ON at.team_id = g.away_team_id AND at.sport_id = g.sport_id
        WHERE s.league = ? AND (
            g.espn_id = ?
            OR abs(julianday(g.start_time_utc) - julianday(?)) < 1.0 / 24
            OR (g.start_time_utc IS NOT NULL AND julianday(g.start_time_utc) IS NULL)
        )
        """,
        (league, event_id, start.isoformat()),
    ).fetchall()
    exact = [row for row in candidates if row["espn_id"] == event_id]
    if len(exact) > 1:
        return None
    matches = []
    for row in exact or candidates:
        candidate_start = _parse_start_time(row["start_time_utc"])
        if candidate_start is None or abs(candidate_start - start) > _SCORE_START_TOLERANCE:
            continue
        if all(
            row[f"{side}_code"] == team
            or normalize_team_code(league, row[f"{side}_name"] or row[f"{side}_code"]) == team
            for side, team in zip(("home", "away"), teams)
        ):
            matches.append(row)
    if len(matches) != 1 or (not exact and matches[0]["espn_id"] not in (None, "")):
        return None
    return matches[0]


def update_scores_in_db(games: List[Dict], league: str, *, only_missing: bool = False) -> int:
    if not games:
        return 0

    league = league.upper()
    updated = 0
    unmatched: List[Dict] = []
    by_event = {}
    conflicting = set()
    for game in games:
        event_id = game.get("espn_id")
        if isinstance(event_id, str) and event_id.strip():
            if by_event.setdefault(event_id, game) != game:
                conflicting.add(event_id)
        else:
            unmatched.append(game)

    with connect() as conn:
        conn.execute("BEGIN IMMEDIATE")
        team_columns = {row["name"] for row in conn.execute("PRAGMA table_info(teams)").fetchall()}
        team_names = (
            "ht.name AS home_name, at.name AS away_name"
            if "name" in team_columns
            else "NULL AS home_name, NULL AS away_name"
        )
        matched = []
        for event_id, game in by_event.items():
            row = None if event_id in conflicting else _score_fixture(conn, game, league, team_names)
            if row is None:
                unmatched.append(game)
            else:
                matched.append((game, row))
        # Resolve the whole batch before writing so two source IDs cannot race to bind one row.
        fixtures = Counter(row["game_id"] for _, row in matched)
        for game, row in matched:
            game_id = row["game_id"]
            if fixtures[game_id] != 1:
                unmatched.append(game)
                continue
            if str(row["status"] or "").lower() == "voided":
                continue

            cursor = conn.execute(
                """
                INSERT INTO game_results (game_id, home_score, away_score, source_version)
                SELECT game_id, ?, ?, 'espn_scoreboard'
                FROM games
                WHERE game_id = ? AND LOWER(COALESCE(status, 'scheduled')) != 'voided'
                  AND (espn_id IS NULL OR espn_id IN ('', ?))
                ON CONFLICT(game_id) DO UPDATE SET
                    home_score = excluded.home_score,
                    away_score = excluded.away_score,
                    source_version = excluded.source_version
                WHERE NOT ? OR game_results.home_score IS NULL OR game_results.away_score IS NULL
                """,
                (game["home_score"], game["away_score"], game_id, game["espn_id"], only_missing),
            )
            if cursor.rowcount == 0:
                continue
            conn.execute(
                "UPDATE games SET status = 'final', espn_id = COALESCE(NULLIF(espn_id, ''), ?) WHERE game_id = ?",
                (game["espn_id"], game_id),
            )
            updated += 1

    if unmatched:
        # Surface unresolved or conflicting fixtures for operator review.
        sample = ", ".join(
            f"{g.get('away_team')}@{g.get('home_team')} {g.get('date')}" for g in unmatched[:3]
        )
        LOGGER.warning(
            "%s: %d ESPN scoreboard game(s) had no matching games row (sample: %s)",
            league,
            len(unmatched),
            sample,
        )
    return updated


def backfill_scores(
    leagues: Iterable[str],
    start_date: date,
    end_date: date,
    *,
    pause_seconds: float = 0.25,
    only_missing: bool = False,
) -> int:
    total_updated = 0
    for league in leagues:
        league_upper = league.upper()
        parsed_games: List[Dict] = []
        for target_date in iter_dates(start_date, end_date):
            try:
                payload = fetch_espn_scoreboard(league_upper, target_date)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Failed to fetch ESPN scores for %s %s: %s", league_upper, target_date, exc)
                continue
            events = payload.get("events") or []
            parsed_games.extend(
                parsed for event in events if (parsed := parse_espn_event(event, league_upper)) is not None
            )
            if pause_seconds > 0:
                time.sleep(pause_seconds)

        updated = update_scores_in_db(parsed_games, league_upper, only_missing=only_missing)
        LOGGER.info("Updated %d %s scores from ESPN", updated, league_upper)
        total_updated += updated
    return total_updated


def _parse_start_time(value: object, *, require_offset: bool = False) -> Optional[datetime]:
    if not isinstance(value, str) or len(value.strip()) <= 10:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return None if require_offset else parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        return None


def stale_score_targets(
    leagues: Optional[Iterable[str]] = None,
    *,
    stale_hours: int = 6,
    lookback_days: int = 14,
) -> List[Tuple[str, date]]:
    league_filter = {league.upper() for league in leagues} if leagues else None
    params: list[object] = [f"-{stale_hours} hours", f"-{lookback_days} days"]
    where = ""
    if league_filter:
        placeholders = ",".join("?" for _ in league_filter)
        where = f"AND s.league IN ({placeholders})"
        params.extend(sorted(league_filter))

    query = f"""
        SELECT s.league, g.start_time_utc
        FROM games g
        JOIN sports s ON s.sport_id = g.sport_id
        LEFT JOIN game_results gr ON gr.game_id = g.game_id
        WHERE julianday(g.start_time_utc) < julianday('now', ?)
          AND julianday(g.start_time_utc) >= julianday('now', ?)
          AND LOWER(COALESCE(g.status, 'scheduled')) != 'voided'
          AND (gr.home_score IS NULL OR gr.away_score IS NULL)
          {where}
        ORDER BY g.start_time_utc
    """

    eastern = ZoneInfo("America/New_York")
    targets: set[Tuple[str, date]] = set()
    with connect() as conn:
        rows = conn.execute(query, params).fetchall()
    for row in rows:
        start_time = _parse_start_time(row["start_time_utc"])
        if start_time is None:
            continue
        targets.add((row["league"], start_time.astimezone(eastern).date()))
    return sorted(targets)


def resolve_stale_scores(
    leagues: Optional[Iterable[str]] = None,
    *,
    stale_hours: int = 6,
    lookback_days: int = 14,
) -> int:
    targets = stale_score_targets(leagues, stale_hours=stale_hours, lookback_days=lookback_days)
    if not targets:
        LOGGER.info("No stale games need ESPN score resolution")
        return 0

    total_updated = 0
    for league, target_date in targets:
        total_updated += backfill_scores([league], target_date, target_date, only_missing=True)
    return total_updated


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill completed scores from ESPN scoreboard data.")
    parser.add_argument("--leagues", nargs="+", default=SUPPORTED_LEAGUES)
    parser.add_argument("--start", help="Start date YYYY-MM-DD")
    parser.add_argument("--end", help="End date YYYY-MM-DD")
    parser.add_argument("--resolve-stale", action="store_true", help="Resolve stale past games instead of a fixed date range")
    parser.add_argument("--stale-hours", type=int, default=6)
    parser.add_argument("--lookback-days", type=int, default=14)
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"])
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))
    leagues = args.leagues
    if len(leagues) == 1 and "," in leagues[0]:
        leagues = [league.strip() for league in leagues[0].split(",") if league.strip()]

    if args.resolve_stale:
        updated = resolve_stale_scores(leagues, stale_hours=args.stale_hours, lookback_days=args.lookback_days)
    else:
        if not args.start or not args.end:
            raise SystemExit("--start and --end are required unless --resolve-stale is set")
        updated = backfill_scores(
            leagues,
            datetime.strptime(args.start, "%Y-%m-%d").date(),
            datetime.strptime(args.end, "%Y-%m-%d").date(),
        )
    print(f"Updated scores: {updated}")


if __name__ == "__main__":
    main()
