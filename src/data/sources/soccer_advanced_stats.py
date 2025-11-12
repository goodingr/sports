"""Scrape advanced soccer statistics (xG, shots, possession proxies) from Understat."""

from __future__ import annotations

import json
import logging
import re
import time
from datetime import datetime
from typing import Dict, Iterable, List, Optional

import pandas as pd
import requests

from .utils import SourceDefinition, source_run, write_dataframe


LOGGER = logging.getLogger(__name__)

UNDERSTAT_LEAGUE_CODES: Dict[str, str] = {
    "EPL": "EPL",
    "LALIGA": "La_Liga",
    "BUNDESLIGA": "Bundesliga",
    "SERIEA": "Serie_A",
    "LIGUE1": "Ligue_1",
}

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/118.0.0.0 Safari/537.36"
    ),
    "Referer": "https://understat.com/",
}


def _normalize_seasons(seasons: Iterable[int] | None) -> List[int]:
    if seasons:
        return sorted({int(season) for season in seasons})
    current = datetime.now().year
    return [current - 1, current]


def _safe_float(value: Optional[str]) -> float:
    try:
        if value is None:
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _fetch_understat_league_data(
    league_code: str,
    season: int,
    *,
    timeout: int = 30,
) -> pd.DataFrame:
    """Fetch league statistics from Understat for a specific season."""
    url = f"https://understat.com/league/{league_code}/{season}"
    LOGGER.info("Fetching Understat data for %s season %s", league_code, season)

    try:
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as exc:
        LOGGER.error("Failed to fetch Understat data for %s %s: %s", league_code, season, exc)
        return pd.DataFrame()

    html = response.text
    match = re.search(r"var\s+teamsData\s*=\s*JSON\.parse\('([^']+)'\);", html)
    if not match:
        LOGGER.error("Could not locate teamsData JSON in Understat response for %s %s", league_code, season)
        return pd.DataFrame()

    json_blob = match.group(1)
    decoded = json_blob.encode("utf-8").decode("unicode_escape")

    try:
        teams_data = json.loads(decoded)
    except json.JSONDecodeError as exc:
        LOGGER.error("Failed to parse teamsData JSON for %s %s: %s", league_code, season, exc)
        return pd.DataFrame()

    rows: List[Dict[str, float | str | int]] = []

    for team_id, team_info in teams_data.items():
        team_name = team_info.get("title") or team_info.get("team_title") or team_id
        history = team_info.get("history", [])
        if not history:
            continue

        matches = len(history)

        totals = {
            "xG": 0.0,
            "xGA": 0.0,
            "xGD": 0.0,
            "scored": 0.0,
            "conceded": 0.0,
            "shots": 0.0,
            "shots_on_target": 0.0,
            "deep": 0.0,
            "xpts": 0.0,
            "points": 0.0,
        }
        wins = draws = losses = 0

        for game in history:
            totals["xG"] += _safe_float(game.get("xG"))
            totals["xGA"] += _safe_float(game.get("xGA"))
            totals["xGD"] += _safe_float(game.get("xGDiff"))
            totals["scored"] += _safe_float(game.get("scored"))
            totals["conceded"] += _safe_float(game.get("missed"))
            totals["shots"] += _safe_float(game.get("shots"))
            totals["shots_on_target"] += _safe_float(game.get("shotsOnTarget"))
            totals["deep"] += _safe_float(game.get("deep"))
            totals["xpts"] += _safe_float(game.get("xPTS"))
            totals["points"] += _safe_float(game.get("pts"))

            result = game.get("result")
            if result == "W":
                wins += 1
            elif result == "D":
                draws += 1
            elif result == "L":
                losses += 1

        if matches == 0:
            continue

        rows.append(
            {
                "league": league_code,
                "team": str(team_name),
                "season": season,
                "matches": matches,
                "xG": round(totals["xG"], 4),
                "xGA": round(totals["xGA"], 4),
                "xGD": round(totals["xGD"], 4),
                "goals_for": round(totals["scored"], 2),
                "goals_against": round(totals["conceded"], 2),
                "shots": round(totals["shots"], 2),
                "shots_on_target": round(totals["shots_on_target"], 2),
                "deep_entries": round(totals["deep"], 2),
                "expected_points": round(totals["xpts"], 2),
                "points": round(totals["points"], 2),
                "wins": wins,
                "draws": draws,
                "losses": losses,
                "avg_xG": round(totals["xG"] / matches, 4),
                "avg_xGA": round(totals["xGA"] / matches, 4),
                "avg_shots": round(totals["shots"] / matches, 4),
                "avg_shots_on_target": round(totals["shots_on_target"] / matches, 4),
                "avg_deep_entries": round(totals["deep"] / matches, 4),
            }
        )

    return pd.DataFrame(rows)


def ingest(
    *,
    leagues: Iterable[str] | None = None,
    seasons: Iterable[int] | None = None,
    timeout: int = 30,
) -> str:
    """Scrape advanced soccer statistics for European leagues."""
    definition = SourceDefinition(
        key="soccer_advanced_stats",
        name="Understat advanced soccer stats",
        league=None,
        category="advanced_metrics",
        url="https://understat.com/",
        default_frequency="daily",
        storage_subdir="soccer/advanced_stats",
    )

    league_list = [l.upper() for l in leagues] if leagues else list(UNDERSTAT_LEAGUE_CODES.keys())
    season_list = _normalize_seasons(seasons)

    output_dir = ""
    with source_run(definition) as run:
        output_dir = str(run.storage_dir)
        all_frames: List[pd.DataFrame] = []

        for league in league_list:
            code = UNDERSTAT_LEAGUE_CODES.get(league)
            if not code:
                LOGGER.warning("No Understat mapping for league %s, skipping", league)
                continue

            for season in season_list:
                df = _fetch_understat_league_data(code, season, timeout=timeout)
                if df.empty:
                    continue
                df["league"] = league
                all_frames.append(df)
                time.sleep(1)

        if not all_frames:
            run.set_message("No soccer advanced stats scraped")
            run.set_raw_path(run.storage_dir)
            return output_dir

        stats = pd.concat(all_frames, ignore_index=True)
        stats["team"] = stats["team"].astype(str)

        path = run.make_path("advanced_stats.parquet")
        write_dataframe(stats, path)
        run.record_file(path, metadata={"rows": len(stats)}, records=len(stats))

        run.set_records(len(stats))
        run.set_message(f"Scraped {len(stats)} soccer advanced stat rows")
        run.set_raw_path(run.storage_dir)

    return output_dir


__all__ = ["ingest"]

