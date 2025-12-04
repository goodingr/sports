"""Pull NBA schedules and results via nba_api."""

from __future__ import annotations

import argparse
import logging
import textwrap
import time
from datetime import datetime, timedelta
from json.decoder import JSONDecodeError
from typing import Iterable, List, Optional

import pandas as pd
from nba_api.stats.endpoints import leaguegamefinder
from requests import RequestException

from src.db.loaders import load_schedules

from .config import RAW_DATA_DIR, ensure_directories
from src.data.utils import save_raw_json


LOGGER = logging.getLogger(__name__)

try:  # pragma: no cover - imported for runtime resilience
    from nba_api.stats.library.http import STATS_HEADERS as NBA_STATS_HEADERS_BASE  # type: ignore
except Exception:  # pragma: no cover - fallback if import location changes
    NBA_STATS_HEADERS_BASE = {
        "Host": "stats.nba.com",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Referer": "https://www.nba.com/",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
    }

NBA_STATS_HEADERS = dict(NBA_STATS_HEADERS_BASE)
NBA_STATS_HEADERS.update(
    {
        "Referer": "https://www.nba.com/stats/",
        "Origin": "https://www.nba.com",
        "x-nba-stats-token": "true",
        "x-nba-stats-origin": "stats",
    }
)

_NBA_API_MAX_ATTEMPTS = 4
_NBA_API_BASE_BACKOFF_SECONDS = 2.0


def _season_to_string(season: int) -> str:
    return f"{season}-{str(season + 1)[-2:]}"


def _format_nba_api_date(value: datetime) -> str:
    """Convert datetime to NBA stats API date string (MM/DD/YYYY)."""
    return value.strftime("%m/%d/%Y")


def _log_nba_api_failure(
    finder: leaguegamefinder.LeagueGameFinder | None,
    season_str: str,
    attempt: int,
    attempts: int,
    error: Exception,
) -> None:
    status_code = None
    response_preview = None

    if finder is not None:
        response = getattr(finder, "nba_response", None)
        if response is not None:
            status_code = getattr(response, "_status_code", None)
            try:
                raw_response = response.get_response()
            except Exception:  # pragma: no cover - best effort logging
                raw_response = None
            if raw_response:
                response_preview = textwrap.shorten(
                    raw_response.replace("\n", " "),
                    width=500,
                    placeholder="…",
                )

    LOGGER.warning(
        "NBA stats API returned an invalid payload for %s (attempt %s/%s, status=%s): %s",
        season_str,
        attempt,
        attempts,
        status_code,
        error,
    )
    if response_preview:
        LOGGER.debug(
            "NBA stats raw response preview for %s: %s",
            season_str,
            response_preview,
        )


def _get_status_code(finder: leaguegamefinder.LeagueGameFinder | None) -> int | None:
    """Extract HTTP status code from NBA API response."""
    if finder is None:
        return None
    response = getattr(finder, "nba_response", None)
    if response is None:
        return None
    return getattr(response, "_status_code", None)


def _fetch_single_season(
    season: int,
    season_type: str,
    *,
    attempts: int = _NBA_API_MAX_ATTEMPTS,
    timeout: int = 30,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> pd.DataFrame:
    season_str = _season_to_string(season)
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        finder: leaguegamefinder.LeagueGameFinder | None = None
        try:
            finder = leaguegamefinder.LeagueGameFinder(
                league_id_nullable="00",
                season_nullable=season_str,
                season_type_nullable=season_type,
                date_from_nullable=date_from,
                date_to_nullable=date_to,
                headers=NBA_STATS_HEADERS,
                timeout=timeout,
                get_request=False,
            )
            finder.get_request()
            
            # Save raw response
            try:
                raw_json = finder.get_json()
                if raw_json:
                    import json
                    data = json.loads(raw_json)
                    save_raw_json(data, "NBA", "nba_api")
            except Exception as exc:
                LOGGER.warning("Failed to save raw NBA JSON: %s", exc)

            frames = finder.get_data_frames()
            if not frames:
                LOGGER.warning(
                    "NBA stats API returned no results for %s %s",
                    season_type,
                    season_str,
                )
                return pd.DataFrame()
            return frames[0]
        except (JSONDecodeError, ValueError, IndexError, KeyError) as exc:
            status_code = _get_status_code(finder)
            # Handle 403 Forbidden as a non-retryable error - likely season unavailable or API blocked
            if status_code == 403:
                LOGGER.warning(
                    "NBA stats API returned 403 Forbidden for %s %s. "
                    "This season may not be available or the API may be blocking requests. "
                    "Skipping this season.",
                    season_type,
                    season_str,
                )
                return pd.DataFrame()
            last_error = exc
            _log_nba_api_failure(finder, season_str, attempt, attempts, exc)
        except RequestException as exc:  # pragma: no cover - network guard
            last_error = exc
            LOGGER.warning(
                "NBA stats API request failed for %s (attempt %s/%s): %s",
                season_str,
                attempt,
                attempts,
                exc,
            )

        if attempt < attempts:
            sleep_seconds = min(60.0, _NBA_API_BASE_BACKOFF_SECONDS * attempt)
            LOGGER.info(
                "Retrying NBA stats API for %s in %.1f seconds (attempt %s/%s)",
                season_str,
                sleep_seconds,
                attempt + 1,
                attempts,
            )
            time.sleep(sleep_seconds)

    error_message = (
        f"Failed to download NBA {season_type} logs for {season_str} "
        f"after {attempts} attempts"
    )
    if last_error:
        raise RuntimeError(error_message) from last_error
    raise RuntimeError(error_message)


def _fetch_game_logs(
    seasons: Iterable[int],
    season_type: str,
    *,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for season in seasons:
        season_str = _season_to_string(season)
        LOGGER.info("Downloading NBA %s data for %s", season_type, season_str)
        df = _fetch_single_season(
            season,
            season_type,
            date_from=date_from,
            date_to=date_to,
        )
        if df.empty:
            continue
        df["season_year"] = season
        frames.append(df)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _transform_to_games(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    records: List[dict[str, object]] = []
    grouped = df.groupby("GAME_ID")
    for game_id, group in grouped:
        if len(group) < 2:
            continue

        home_rows = group[group["MATCHUP"].str.contains(" vs. ", na=False)]
        away_rows = group[group["MATCHUP"].str.contains(" @ ", na=False)]
        if home_rows.empty or away_rows.empty:
            continue

        home_row = home_rows.iloc[0]
        away_row = away_rows.iloc[0]

        home_wl = str(home_row.get("WL") or "").strip().upper()
        away_wl = str(away_row.get("WL") or "").strip().upper()
        is_final = home_wl in {"W", "L"} and away_wl in {"W", "L"}

        try:
            game_date_str = str(home_row["GAME_DATE"]).strip()
        except Exception:
            game_date_str = ""

        game_date = None
        for fmt in ("%b %d, %Y", "%Y-%m-%d"):
            if not game_date_str:
                break
            try:
                game_date = datetime.strptime(game_date_str, fmt).date()
                break
            except ValueError:
                continue

        record = {
            "game_id": f"NBA_{game_id}",
            "season": int(home_row.get("season_year") or 0),
            "game_type": "REG",
            "week": None,
            "gameday": game_date.isoformat() if game_date else None,
            "gametime": None,
            "weekday": game_date.strftime("%A") if game_date else None,
            "home_team": home_row["TEAM_ABBREVIATION"],
            "home_team_name": home_row["TEAM_NAME"],
            "away_team": away_row["TEAM_ABBREVIATION"],
            "away_team_name": away_row["TEAM_NAME"],
            "home_score": int(home_row["PTS"]) if is_final else None,
            "away_score": int(away_row["PTS"]) if is_final else None,
            "spread_line": None,
            "total_line": None,
            "home_moneyline": None,
            "away_moneyline": None,
            "stadium": None,
            "source_version": "nba_api",
        }
        records.append(record)

    return pd.DataFrame.from_records(records)


def _to_int_list(seasons: Iterable[int | str]) -> List[int]:
    parsed: List[int] = []
    for season in seasons:
        value = int(season)
        if value < 2000:
            raise ValueError("NBA seasons before 2000 are not supported")
        parsed.append(value)
    parsed.sort()
    return parsed


def run(
    seasons: List[int],
    season_type: str = "Regular Season",
    *,
    days_back: Optional[int] = None,
) -> None:
    ensure_directories()
    date_from = None
    date_to = None

    if days_back is not None:
        if days_back <= 0:
            raise ValueError("days_back must be positive")
        utc_now = datetime.utcnow()
        start_date = utc_now - timedelta(days=days_back)
        date_from = _format_nba_api_date(start_date)
        date_to = _format_nba_api_date(utc_now)
        LOGGER.info(
            "Limiting NBA results download to %s-%s (last %s days)",
            date_from,
            date_to,
            days_back,
        )

    logs = _fetch_game_logs(seasons, season_type, date_from=date_from, date_to=date_to)
    games = _transform_to_games(logs)

    if games.empty:
        LOGGER.warning("No NBA games found for seasons %s", seasons)
        return

    file_tag = f"nba_{seasons[0]}_{seasons[-1]}"
    schedules_path = RAW_DATA_DIR / "results" / f"schedules_{file_tag}.parquet"
    games.to_parquet(schedules_path, index=False)
    LOGGER.info("Saved NBA schedules to %s", schedules_path)

    load_schedules(
        games,
        source_version="nba_api",
        league="NBA",
        sport_name="Basketball",
        default_market="moneyline",
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download NBA schedules and results")
    parser.add_argument(
        "--seasons",
        nargs="+",
        default=list(range(2015, 2024)),
        help="List of NBA seasons (e.g., 2023 for 2023-24 season)",
    )
    parser.add_argument(
        "--season-type",
        default="Regular Season",
        choices=["Regular Season", "Playoffs"],
        help="NBA season type",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level))
    seasons = _to_int_list(args.seasons)
    run(seasons, season_type=args.season_type)


if __name__ == "__main__":
    main()
