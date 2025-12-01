"""Pull soccer schedules and results from database.sqlite, ESPN API, and other sources."""

from __future__ import annotations

import argparse
import logging
from datetime import date, datetime, timedelta
from pathlib import Path
import os
import time
from typing import Dict, Iterable, Iterator, List, Optional, Tuple

import pandas as pd
import requests
import sqlite3
import time

from src.db.loaders import load_schedules
from src.data.team_mappings import normalize_team_code

from .config import RAW_DATA_DIR, ensure_directories
from .sources.utils import DEFAULT_HEADERS


LOGGER = logging.getLogger(__name__)

# League ID to our league code mapping (from database.sqlite)
LEAGUE_ID_MAP = {
    1729: "EPL",      # England Premier League
    4769: "LIGUE1",   # France Ligue 1
    7809: "BUNDESLIGA",  # Germany 1. Bundesliga
    10257: "SERIEA",  # Italy Serie A
    21518: "LALIGA",  # Spain LIGA BBVA
}

DEFAULT_SOCCER_DB = Path("database.sqlite")

# ESPN API endpoints for soccer leagues
ESPN_SOCCER_MAP = {
    "EPL": "soccer/eng.1",
    "LALIGA": "soccer/esp.1",
    "BUNDESLIGA": "soccer/ger.1",
    "SERIEA": "soccer/ita.1",
    "LIGUE1": "soccer/fra.1",
}

FOOTBALL_DATA_COMPETITIONS = {
    "EPL": "PL",
    "LALIGA": "PD",
    "BUNDESLIGA": "BL1",
    "SERIEA": "SA",
    "LIGUE1": "FL1",
}
FOOTBALL_DATA_API_BASE = "https://api.football-data.org/v4"
FOOTBALL_DATA_ENV_KEY = "FOOTBALL_DATA_API_KEY"
_FOOTBALL_DATA_LIMIT_KEYWORDS = (
    "rate limit",
    "too many",
    "exceeded",
    "request limit",
)

FINAL_STATUS_CODES = {
    "STATUS_FINAL",
    "STATUS_FINAL_OVERTIME",
    "STATUS_FINAL_SHOOTOUT",
    "STATUS_FULL_TIME",
}

MAX_SCOREBOARD_DAYS = 45


def _to_int_list(seasons: Iterable[int | str]) -> List[int]:
    parsed: List[int] = []
    for season in seasons:
        value = int(season)
        if value < 2008:
            raise ValueError("Soccer seasons before 2008 are not supported in database.sqlite")
        parsed.append(value)
    parsed.sort()
    return parsed


def _extract_season_from_string(season_str: str) -> Optional[int]:
    """Extract year from season string like '2008/2009' -> 2008."""
    if not season_str:
        return None
    try:
        # Format is typically "2008/2009" - take first year
        year = int(season_str.split("/")[0])
        return year
    except (ValueError, AttributeError):
        return None


def _season_date_range(season: int) -> Tuple[date, date]:
    start = date(int(season), 7, 1)
    end = date(int(season) + 1, 6, 30)
    return start, end


def _iter_dates(start_date: date, end_date: date) -> Iterator[date]:
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def _parse_score(value: Optional[str], *, is_final: bool) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None if not is_final else 0


def _format_game_times(start_iso: Optional[str]) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not start_iso:
        return None, None, None
    try:
        dt = pd.to_datetime(start_iso, utc=True)
    except Exception:
        return None, None, None
    local = dt.tz_convert("UTC")
    return local.date().isoformat(), local.strftime("%H:%M"), local.strftime("%A")


def fetch_from_espn(
    leagues: Optional[List[str]] = None,
    *,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    days_back: Optional[int] = None,
    days_forward: int = 0,
    seasons: Optional[List[int]] = None,
) -> pd.DataFrame:
    """Fetch soccer matches from ESPN's scoreboard API for the given date range."""
    if leagues is None:
        leagues = list(ESPN_SOCCER_MAP.keys())

    ranges: List[Tuple[date, date]] = []
    today = datetime.utcnow().date()
    if start_date and end_date:
        ranges.append((start_date, end_date))
    elif seasons:
        for season in seasons:
            ranges.append(_season_date_range(int(season)))
    else:
        back = days_back or 0
        start = today - timedelta(days=back)
        end = today + timedelta(days=days_forward)
        ranges.append((start, end))

    headers = dict(DEFAULT_HEADERS)
    headers.setdefault("Referer", "https://www.espn.com/")

    records: List[Dict[str, object]] = []

    for league in leagues:
        league_upper = league.upper()
        path = ESPN_SOCCER_MAP.get(league_upper)
        if not path:
            LOGGER.warning("No ESPN mapping for league %s", league_upper)
            continue

        url = f"https://site.api.espn.com/apis/site/v2/sports/{path}/scoreboard"
        seen_ids: set[str] = set()

        for start, end in ranges:
            span_days = (end - start).days
            if span_days > MAX_SCOREBOARD_DAYS:
                LOGGER.info(
                    "Date span %s-%s for %s exceeds %s days; consider splitting",
                    start,
                    end,
                    league_upper,
                    MAX_SCOREBOARD_DAYS,
                )
            for target_date in _iter_dates(start, end):
                date_str = target_date.strftime("%Y%m%d")
                try:
                    response = requests.get(
                        url,
                        params={"dates": date_str},
                        headers=headers,
                        timeout=30,
                    )
                    response.raise_for_status()
                    payload = response.json()
                    if not payload:
                        continue
                except requests.RequestException as exc:
                    LOGGER.warning("Failed to fetch ESPN data for %s on %s: %s", league_upper, date_str, exc)
                    continue

                time.sleep(0.2)

                for event in payload.get("events", []):
                    event_id = event.get("id")
                    if not event_id or event_id in seen_ids:
                        continue

                    competitions = event.get("competitions") or []
                    if not competitions:
                        continue
                    competition = competitions[0]
                    competitors = competition.get("competitors") or []
                    if len(competitors) < 2:
                        continue

                    home_entry = next((c for c in competitors if c.get("homeAway") == "home"), None)
                    away_entry = next((c for c in competitors if c.get("homeAway") == "away"), None)
                    if not home_entry or not away_entry:
                        continue

                    home_team = home_entry.get("team") or {}
                    away_team = away_entry.get("team") or {}

                    home_name = (
                        home_team.get("displayName")
                        or home_team.get("name")
                        or home_team.get("shortDisplayName")
                        or ""
                    )
                    away_name = (
                        away_team.get("displayName")
                        or away_team.get("name")
                        or away_team.get("shortDisplayName")
                        or ""
                    )

                    status_info = competition.get("status", {})
                    status_type = status_info.get("type", {}) if isinstance(status_info, dict) else {}
                    status_code = str(status_type.get("name") or "").upper()
                    is_final = status_code in FINAL_STATUS_CODES

                    start_time_iso = competition.get("date") or event.get("date")
                    gameday, gametime, weekday = _format_game_times(start_time_iso)

                    season_year = event.get("season", {}).get("year")
                    if season_year is None and gameday:
                        dt = datetime.fromisoformat(gameday)
                        season_year = dt.year if dt.month >= 7 else dt.year - 1

                    odds_list = competition.get("odds") or []
                    home_ml = None
                    away_ml = None
                    if odds_list:
                        first_odd = odds_list[0]
                        if first_odd:
                            moneyline = first_odd.get("moneyline", {})
                            home_ml = moneyline.get("home")
                            away_ml = moneyline.get("away")
                        else:
                            moneyline = {}
                            home_ml = None
                            away_ml = None
                        if isinstance(home_ml, dict):
                            home_ml = home_ml.get("price") or home_ml.get("odds")
                        if isinstance(away_ml, dict):
                            away_ml = away_ml.get("price") or away_ml.get("odds")

                    venue = competition.get("venue", {}) or {}
                    stadium = venue.get("fullName") or venue.get("address", {}).get("city")

                    record = {
                        "game_id": f"{league_upper}_{event_id}",
                        "league_code": league_upper,
                        "event_id": event_id,
                        "season": int(season_year) if season_year is not None else None,
                        "game_type": "REG",
                        "gameday": gameday,
                        "gametime": gametime,
                        "weekday": weekday,
                        "home_team_name": home_name,
                        "away_team_name": away_name,
                        "home_score": _parse_score(home_entry.get("score"), is_final=is_final),
                        "away_score": _parse_score(away_entry.get("score"), is_final=is_final),
                        "home_moneyline": home_ml,
                        "away_moneyline": away_ml,
                        "status": status_code,
                        "is_final": is_final,
                        "stadium": stadium,
                        "source": "espn",
                    }
                    records.append(record)
                    seen_ids.add(event_id)

    return pd.DataFrame(records)


def fetch_from_football_data(
    leagues: List[str],
    seasons: List[int],
    *,
    api_key: Optional[str],
    sleep_seconds: float = 6.0,
    max_retries: int = 5,
) -> pd.DataFrame:
    """Fetch soccer matches from football-data.org for the specified seasons."""
    if not api_key:
        LOGGER.warning("Football-Data API key not provided; skipping football-data.org fetch")
        return pd.DataFrame()
    if not seasons:
        return pd.DataFrame()

    headers = {
        "X-Auth-Token": api_key,
        "User-Agent": "sports-betting-analytics/1.0",
    }

    all_records: List[Dict[str, object]] = []

    for league in leagues:
        league_upper = league.upper()
        comp_code = FOOTBALL_DATA_COMPETITIONS.get(league_upper)
        if not comp_code:
            LOGGER.warning("No Football-Data competition code for league %s", league_upper)
            continue

        for season in seasons:
            params = {"season": int(season)}
            url = f"{FOOTBALL_DATA_API_BASE}/competitions/{comp_code}/matches"
            payload = None
            attempt_wait = sleep_seconds
            for attempt in range(1, max_retries + 1):
                try:
                    response = requests.get(url, headers=headers, params=params, timeout=30)
                except requests.RequestException as exc:  # pragma: no cover - network guard
                    if attempt == max_retries:
                        LOGGER.warning("Football-Data request error for %s %s: %s", league_upper, season, exc)
                        response = None
                        break
                    LOGGER.warning("Football-Data request error for %s %s (attempt %s/%s): %s",
                                   league_upper, season, attempt, max_retries, exc)
                    time.sleep(attempt_wait)
                    attempt_wait = min(attempt_wait * 2, 60.0)
                    continue

                if response is None:
                    break

                if response.status_code in (403, 429):
                    retry_message = ""
                    try:
                        retry_message = str(response.json().get("message", ""))
                    except Exception:
                        retry_message = response.text[:200]
                    retry_message_lower = retry_message.lower()
                    is_limit = (
                        response.status_code == 429
                        or any(keyword in retry_message_lower for keyword in _FOOTBALL_DATA_LIMIT_KEYWORDS)
                    )
                    if is_limit and attempt < max_retries:
                        retry_after = response.headers.get("Retry-After")
                        wait_time = float(retry_after) if retry_after else attempt_wait
                        LOGGER.warning(
                            "Football-Data rate limit hit for %s %s (status %s, message=%s). Retrying in %.1f seconds.",
                            league_upper,
                            season,
                            response.status_code,
                            retry_message or "N/A",
                            wait_time,
                        )
                        time.sleep(wait_time)
                        attempt_wait = min(wait_time * 2, 60.0)
                        continue

                try:
                    response.raise_for_status()
                    payload = response.json()
                except requests.HTTPError as exc:
                    LOGGER.warning("Football-Data fetch failed for %s %s (status %s): %s",
                                   league_upper, season, response.status_code, exc)
                break

            if not payload:
                continue

            matches = payload.get("matches") or []
            LOGGER.info("Football-Data returned %d matches for %s %s", len(matches), league_upper, season)

            for match in matches:
                match_id = match.get("id")
                if match_id is None:
                    continue

                utc_date = match.get("utcDate")
                gameday, gametime, weekday = _format_game_times(utc_date)

                status = str(match.get("status") or "").upper()
                is_final = status in {"FINISHED", "AWARDED"}

                home_team = match.get("homeTeam") or {}
                away_team = match.get("awayTeam") or {}
                home_name = home_team.get("shortName") or home_team.get("name") or ""
                away_name = away_team.get("shortName") or away_team.get("name") or ""

                score = match.get("score") or {}
                ft_score = score.get("fullTime") or {}
                home_score = ft_score.get("home")
                away_score = ft_score.get("away")

                record = {
                    "game_id": f"{league_upper}_{match_id}",
                    "league_code": league_upper,
                    "season": int(season),
                    "game_type": "REG",
                    "week": match.get("matchday"),
                    "gameday": gameday,
                    "gametime": gametime,
                    "weekday": weekday,
                    "home_team_name": home_name,
                    "away_team_name": away_name,
                    "home_score": home_score if home_score is not None else None,
                    "away_score": away_score if away_score is not None else None,
                    "home_moneyline": None,
                    "away_moneyline": None,
                    "stadium": match.get("venue"),
                    "status": status,
                    "is_final": is_final,
                    "source_version": "football_data_api",
                }
                all_records.append(record)

            time.sleep(sleep_seconds)

    return pd.DataFrame(all_records)


def fetch_from_database(
    leagues: Optional[List[str]] = None,
    seasons: Optional[List[int]] = None,
    db_path: Path = DEFAULT_SOCCER_DB,
) -> pd.DataFrame:
    """Fetch soccer matches from database.sqlite."""
    if not db_path.exists():
        LOGGER.warning("Soccer database not found at %s", db_path)
        return pd.DataFrame()
    
    conn = sqlite3.connect(db_path)
    
    try:
        # Build query
        league_ids = []
        if leagues:
            # Map league codes to IDs
            reverse_map = {v: k for k, v in LEAGUE_ID_MAP.items()}
            for league in leagues:
                league_upper = league.upper()
                if league_upper in reverse_map:
                    league_ids.append(reverse_map[league_upper])
        else:
            league_ids = list(LEAGUE_ID_MAP.keys())
        
        if not league_ids:
            return pd.DataFrame()
        
        query = """
            SELECT 
                m.id,
                m.league_id,
                l.name as league_name,
                m.season,
                m.stage,
                m.date,
                m.match_api_id,
                m.home_team_api_id,
                m.away_team_api_id,
                m.home_team_goal,
                m.away_team_goal,
                m.B365H as home_moneyline,
                m.B365A as away_moneyline,
                ht.team_long_name as home_team_name,
                ht.team_short_name as home_team_short,
                at.team_long_name as away_team_name,
                at.team_short_name as away_team_short
            FROM Match m
            JOIN League l ON m.league_id = l.id
            JOIN Team ht ON m.home_team_api_id = ht.team_api_id
            JOIN Team at ON m.away_team_api_id = at.team_api_id
            WHERE m.league_id IN ({})
        """.format(",".join("?" * len(league_ids)))
        
        params = league_ids
        
        if seasons:
            # Filter by season - season is stored as "2008/2009" format
            season_filters = []
            for season in seasons:
                season_filters.append(f"m.season LIKE '{season}/%'")
            query += " AND (" + " OR ".join(season_filters) + ")"
        
        query += " ORDER BY m.season, m.date"
        
        matches_df = pd.read_sql_query(query, conn, params=params)
        
        if matches_df.empty:
            # Check what seasons are actually available
            available_seasons_query = """
                SELECT DISTINCT season 
                FROM Match 
                WHERE league_id IN ({})
                ORDER BY season
            """.format(",".join("?" * len(league_ids)))
            available_seasons = pd.read_sql_query(available_seasons_query, conn, params=league_ids)
            if not available_seasons.empty:
                available_years = sorted(set([int(s.split("/")[0]) for s in available_seasons["season"].tolist() if "/" in str(s)]))
                if seasons and all(s > max(available_years) for s in seasons):
                    LOGGER.info("No matches found for requested seasons %s. Database contains historical data through %d.", seasons, max(available_years))
                else:
                    LOGGER.warning("No matches found in database for specified leagues/seasons")
            else:
                LOGGER.warning("No matches found in database for specified leagues/seasons")
            return pd.DataFrame()
        
        LOGGER.info("Fetched %d matches from database", len(matches_df))
        return matches_df
        
    finally:
        conn.close()


def _transform_to_games(df: pd.DataFrame, league_code: str) -> pd.DataFrame:
    """Transform database matches to our game format."""
    if df.empty:
        return df
    
    records: List[dict[str, object]] = []
    
    for _, row in df.iterrows():
        # Extract season year
        season = _extract_season_from_string(str(row.get("season", "")))
        if not season:
            continue
        
        # Get team names and normalize
        home_team_raw = str(row.get("home_team_name", "")).strip()
        away_team_raw = str(row.get("away_team_name", "")).strip()
        
        if not home_team_raw or not away_team_raw:
            continue
        
        home_team = normalize_team_code(league_code, home_team_raw)
        away_team = normalize_team_code(league_code, away_team_raw)
        
        if not home_team or not away_team:
            continue
        
        # Parse date
        date_str = row.get("date")
        if date_str:
            try:
                game_date = pd.to_datetime(date_str).date()
                gameday = game_date.isoformat()
                weekday = game_date.strftime("%A")
            except Exception:
                gameday = None
                weekday = None
        else:
            gameday = None
            weekday = None
        
        # Get scores (may be None for future games)
        home_score = row.get("home_team_goal")
        away_score = row.get("away_team_goal")
        
        # Create game_id
        match_id = row.get("id") or row.get("match_api_id")
        game_id = f"{league_code}_{match_id}"
        
        # Get moneylines (convert from decimal odds if available)
        home_ml = row.get("home_moneyline")
        away_ml = row.get("away_moneyline")
        
        # Convert decimal odds to American if needed (B365 odds are typically decimal)
        def _decimal_to_american(decimal: Optional[float]) -> Optional[float]:
            if decimal is None or pd.isna(decimal):
                return None
            try:
                dec = float(decimal)
                if dec >= 2.0:
                    # Likely decimal odds, convert to American
                    return (dec - 1) * 100
                elif dec > 1.0:
                    # Already American-style but small, might be decimal
                    return (dec - 1) * 100
                else:
                    # Already American
                    return dec
            except (ValueError, TypeError):
                return None
        
        if home_ml is not None:
            home_ml = _decimal_to_american(home_ml)
        if away_ml is not None:
            away_ml = _decimal_to_american(away_ml)
        
        record = {
            "game_id": game_id,
            "season": season,
            "game_type": "REG",
            "week": row.get("stage"),  # Use stage as week equivalent
            "gameday": gameday,
            "gametime": None,
            "weekday": weekday,
            "home_team": home_team,
            "home_team_name": home_team_raw,
            "away_team": away_team,
            "away_team_name": away_team_raw,
            "home_score": int(home_score) if home_score is not None and not pd.isna(home_score) else None,
            "away_score": int(away_score) if away_score is not None and not pd.isna(away_score) else None,
            "spread_line": None,
            "total_line": None,
            "home_moneyline": home_ml,
            "away_moneyline": away_ml,
            "stadium": None,
            "source_version": "soccer_database",
        }
        records.append(record)
    
    return pd.DataFrame.from_records(records)


def _parse_cli_date(value: Optional[str]) -> Optional[date]:
    if not value:
        return None
    return datetime.strptime(value, "%Y-%m-%d").date()


def run(
    leagues: Optional[List[str]] = None,
    seasons: Optional[List[int]] = None,
    db_path: Optional[Path] = None,
    use_espn: bool = True,
    use_database: bool = True,
    use_football_data: bool = True,
    days_back: Optional[int] = None,
    days_forward: int = 2,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    football_data_api_key: Optional[str] = None,
) -> None:
    """Fetch and load soccer schedules from ESPN API and/or database.sqlite."""
    ensure_directories()

    leagues = [l.upper() for l in leagues] if leagues else list(ESPN_SOCCER_MAP.keys())
    parsed_start = _parse_cli_date(start_date)
    parsed_end = _parse_cli_date(end_date)

    all_games: List[dict[str, object]] = []

    if use_espn:
        LOGGER.info("Fetching soccer matches from ESPN scoreboard...")
        espn_matches = fetch_from_espn(
            leagues=leagues,
            start_date=parsed_start,
            end_date=parsed_end,
            days_back=days_back,
            days_forward=days_forward,
            seasons=seasons,
        )

        if not espn_matches.empty:
            for _, row in espn_matches.iterrows():
                league_code = row["league_code"]
                home_team = normalize_team_code(league_code, row["home_team_name"])
                away_team = normalize_team_code(league_code, row["away_team_name"])

                if not home_team or not away_team:
                    LOGGER.debug(
                        "Skipping ESPN record %s due to unknown team mapping (%s vs %s)",
                        row["game_id"],
                        row["home_team_name"],
                        row["away_team_name"],
                    )
                    continue

                game_record = {
                    "game_id": row["game_id"],
                    "season": row.get("season"),
                    "game_type": "REG",
                    "week": None,
                    "gameday": row.get("gameday"),
                    "gametime": row.get("gametime"),
                    "weekday": row.get("weekday"),
                    "home_team": home_team,
                    "home_team_name": row["home_team_name"],
                    "away_team": away_team,
                    "away_team_name": row["away_team_name"],
                    "home_score": row.get("home_score"),
                    "away_score": row.get("away_score"),
                    "spread_line": None,
                    "total_line": None,
                    "home_moneyline": row.get("home_moneyline"),
                    "away_moneyline": row.get("away_moneyline"),
                    "stadium": row.get("stadium"),
                    "source_version": "espn_api",
                }
                all_games.append(game_record)

            LOGGER.info("Processed %d ESPN matches", len(all_games))

    if use_football_data and seasons:
        api_key = football_data_api_key or os.getenv(FOOTBALL_DATA_ENV_KEY)
        fd_matches = fetch_from_football_data(
            leagues=leagues,
            seasons=seasons,
            api_key=api_key,
        )
        if not fd_matches.empty:
            all_games.extend(fd_matches.to_dict("records"))

    if use_database and seasons:
        db_file = db_path or DEFAULT_SOCCER_DB
        if db_file.exists():
            LOGGER.info("Fetching historical soccer matches from database...")
            db_matches = fetch_from_database(leagues=leagues, seasons=seasons, db_path=db_file)
            if not db_matches.empty:
                for league_id, group in db_matches.groupby("league_id"):
                    league_code = LEAGUE_ID_MAP.get(league_id)
                    if not league_code:
                        continue
                    games = _transform_to_games(group, league_code)
                    if not games.empty:
                        for _, row in games.iterrows():
                            all_games.append(row.to_dict())
                LOGGER.info(
                    "Processed %d matches from historical database",
                    len([g for g in all_games if g.get("source_version") == "soccer_database"]),
                )
            else:
                LOGGER.info(
                    "No soccer matches found in database for seasons %s (available data ends around 2015)",
                    seasons,
                )
        else:
            LOGGER.info("Soccer database not found at %s. Skipping historical data.", db_file)

    if not all_games:
        LOGGER.warning("No soccer matches found from any source")
        return

    games_df = pd.DataFrame(all_games)
    if games_df.empty:
        LOGGER.warning("No soccer matches to process after normalization")
        return

    games_df = games_df.sort_values("gameday")
    games_df.drop_duplicates(subset=["game_id"], keep="last", inplace=True)

    for league_code in games_df["game_id"].str.split("_").str[0].unique():
        league_games = games_df[games_df["game_id"].str.startswith(f"{league_code}_")]
        if league_games.empty:
            continue

        LOGGER.info("Processing %d %s games", len(league_games), league_code)
        source_tag = "espn_api"
        if "source_version" in league_games.columns:
            source_tag = league_games["source_version"].iloc[0]

        file_tag = f"{league_code.lower()}_{source_tag}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        schedules_path = RAW_DATA_DIR / "results" / f"schedules_{file_tag}.parquet"
        league_games.to_parquet(schedules_path, index=False)
        LOGGER.info("Saved %s schedules to %s", league_code, schedules_path)

        load_schedules(
            league_games,
            source_version=source_tag,
            league=league_code,
            sport_name="Soccer",
            default_market="moneyline",
        )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download soccer schedules and results")
    parser.add_argument(
        "--leagues",
        nargs="+",
        choices=["EPL", "LALIGA", "BUNDESLIGA", "SERIEA", "LIGUE1"],
        default=None,
        help="List of leagues to download (default: all)",
    )
    parser.add_argument(
        "--seasons",
        nargs="+",
        default=None,
        help="List of seasons (years) to download (default: all available)",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_SOCCER_DB,
        help=f"Path to database.sqlite file (default: {DEFAULT_SOCCER_DB})",
    )
    parser.add_argument(
        "--no-espn",
        action="store_true",
        help="Skip fetching from ESPN API (only use database)",
    )
    parser.add_argument(
        "--no-db",
        action="store_true",
        help="Skip database-backed historical fetch (only ESPN)",
    )
    parser.add_argument(
        "--no-football-data",
        action="store_true",
        help="Skip football-data.org fetch (requires FOOTBALL_DATA_API_KEY)",
    )
    parser.add_argument(
        "--football-data-api-key",
        type=str,
        default=None,
        help="API key for football-data.org (falls back to FOOTBALL_DATA_API_KEY env var)",
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=7,
        help="Number of days in the past to fetch from ESPN when no explicit range is provided",
    )
    parser.add_argument(
        "--days-forward",
        type=int,
        default=2,
        help="Number of days ahead to fetch from ESPN when no explicit range is provided",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        help="Explicit start date (YYYY-MM-DD) for ESPN fetch",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default=None,
        help="Explicit end date (YYYY-MM-DD) for ESPN fetch",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging verbosity",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))
    
    seasons = _to_int_list(args.seasons) if args.seasons else None
    leagues = [l.upper() for l in args.leagues] if args.leagues else None
    
    run(
        leagues=leagues,
        seasons=seasons,
        db_path=args.db_path,
        use_espn=not args.no_espn,
        use_database=not args.no_db,
        use_football_data=not args.no_football_data,
        days_back=args.days_back,
        days_forward=args.days_forward,
        start_date=args.start_date,
        end_date=args.end_date,
        football_data_api_key=args.football_data_api_key,
    )


if __name__ == "__main__":
    main()
