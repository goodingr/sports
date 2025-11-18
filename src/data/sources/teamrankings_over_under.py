"""Download TeamRankings over/under picks across multiple date ranges."""

from __future__ import annotations

import argparse
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Sequence, Tuple

import pandas as pd
import requests
from bs4 import BeautifulSoup

from .utils import DEFAULT_HEADERS, SourceDefinition, source_run, write_dataframe


LOGGER = logging.getLogger(__name__)

LEAGUE_PATHS = {
    "NBA": "nba",
    "NFL": "nfl",
    "CFB": "college-football",
}

BASE_URL = "https://www.teamrankings.com/{path}-over-under-picks/"


def _clean_percentage(value: str | None) -> Optional[float]:
    if not value:
        return None
    stripped = value.replace("%", "").strip()
    if not stripped:
        return None
    try:
        return float(stripped) / 100.0
    except ValueError:
        return None


def _normalize_half_char(value: str) -> str:
    if not value:
        return value
    return value.replace("½", ".5")


def _parse_pick_text(text: str) -> Tuple[Optional[str], Optional[float], Optional[int]]:
    if not text:
        return None, None, None
    cleaned = _normalize_half_char(text)
    match = re.match(r"(?P<rot>\d+)\s+(?P<pick>Over|Under)\s+(?P<line>[\d.]+)", cleaned, re.IGNORECASE)
    if not match:
        return None, None, None
    rotation = int(match.group("rot"))
    pick = match.group("pick").title()
    try:
        total_line = float(match.group("line"))
    except ValueError:
        total_line = None
    return pick, total_line, rotation


def _parse_model_cell(text: str) -> Tuple[Optional[str], Optional[float]]:
    if not text:
        return None, None
    cleaned = text.strip()
    if not cleaned:
        return None, None
    parts = cleaned.split()
    pick = parts[0].title()
    probability = _clean_percentage(parts[-1]) if parts else None
    return pick, probability


def _parse_confidence(td) -> Tuple[Optional[int], Optional[float]]:
    stars = None
    probability = None
    if td is None:
        return stars, probability
    data_sort = td.get("data-sort", "")
    match = re.match(r"sort(\d+)-([\d.]+)", data_sort)
    if match:
        try:
            stars = int(match.group(1))
        except ValueError:
            stars = None
        try:
            probability = float(match.group(2))
        except ValueError:
            probability = None

    star_span = td.find(class_=re.compile(r"tr_stars_", re.IGNORECASE))
    if star_span and not stars:
        for cls in star_span.get("class", []):
            if cls.startswith("tr_stars_"):
                try:
                    stars = int(cls.split("_")[-1])
                except (IndexError, ValueError):
                    continue
    return stars, probability


def _extract_ranges(html: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    select = soup.find("select", {"id": "range"})
    if not select:
        return []
    ranges: List[str] = []
    for option in select.find_all("option"):
        value = option.get("value")
        if value:
            ranges.append(value.strip())
    return list(dict.fromkeys(ranges))


def _fetch_page(league: str, range_key: Optional[str], season: Optional[str], timeout: int) -> str:
    league_path = LEAGUE_PATHS[league.upper()]
    params = {}
    if range_key:
        params["range"] = range_key
    if season:
        params["season"] = season
    url = BASE_URL.format(path=league_path)
    response = requests.get(url, params=params, headers=DEFAULT_HEADERS, timeout=timeout)
    response.raise_for_status()
    return response.text


def _parse_table_rows(html: str, league: str, range_key: Optional[str], season: Optional[str]) -> pd.DataFrame:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        LOGGER.warning("teamrankings %s: no table found for range %s", league, range_key or "default")
        return pd.DataFrame()
    body = table.find("tbody") or table

    rows: List[dict] = []
    retrieved_at = datetime.now(timezone.utc).isoformat()

    for tr in body.find_all("tr"):
        cells = tr.find_all("td")
        if len(cells) < 8:
            continue

        date_cell = cells[0]
        sort_value = date_cell.get("data-sort")
        if not sort_value:
            continue
        try:
            game_dt = datetime.fromisoformat(sort_value)
        except ValueError:
            continue

        status_text = cells[1].get_text(strip=True) or None
        pick_text = cells[2].get_text(strip=True)
        pick, total_line, rotation = _parse_pick_text(pick_text)
        matchup = cells[3].get_text(" ", strip=True)
        confidence_stars, confidence_prob = _parse_confidence(cells[4])
        odds_value = _clean_percentage(cells[5].get_text(strip=True))
        similar_pick, similar_prob = _parse_model_cell(cells[6].get_text(" ", strip=True))
        model_pick, model_prob = _parse_model_cell(cells[7].get_text(" ", strip=True))

        if not matchup:
            continue
        lower_matchup = matchup.lower()
        away_team = home_team = None
        if " at " in lower_matchup:
            idx = lower_matchup.index(" at ")
            away_team = matchup[:idx].strip()
            home_team = matchup[idx + 4 :].strip()
        elif " vs " in lower_matchup:
            idx = lower_matchup.index(" vs ")
            away_team = matchup[:idx].strip()
            home_team = matchup[idx + 4 :].strip()

        rows.append(
            {
                "league": league.upper(),
                "game_date": game_dt,
                "status": status_text,
                "teamrankings_game_id": rotation,
                "pick": pick,
                "total_line": total_line,
                "matchup": matchup,
                "away_team": away_team,
                "home_team": home_team,
                "confidence_value": confidence_stars,
                "confidence_probability": confidence_prob,
                "odds_value": odds_value,
                "similar_games_pick": similar_pick,
                "similar_games_prob": similar_prob,
                "model_pick": model_pick,
                "model_prob": model_prob,
                "range_key": range_key,
                "season": season,
                "source": "teamrankings",
                "retrieved_at": retrieved_at,
            }
        )

    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def ingest(
    *,
    league: str = "NBA",
    ranges: Optional[Sequence[str]] = None,
    seasons: Optional[Sequence[str]] = None,
    timeout: int = 30,
) -> str:
    league = league.upper()
    if league not in LEAGUE_PATHS:
        raise ValueError(f"Unsupported league '{league}'. Supported: {', '.join(sorted(LEAGUE_PATHS))}")

    definition = SourceDefinition(
        key=f"teamrankings_{league.lower()}_over_under",
        name=f"TeamRankings {league} over/under picks",
        league=league,
        category="odds",
        url=BASE_URL.format(path=LEAGUE_PATHS[league]),
        default_frequency="daily",
        storage_subdir="teamrankings",
    )

    seasons_list: List[Optional[str]] = list(seasons or [None])

    output_dir = ""
    with source_run(definition) as run:
        output_dir = str(run.storage_dir)
        combined_frames: List[pd.DataFrame] = []

        for season in seasons_list:
            initial_html = _fetch_page(league, None, season, timeout)
            available_ranges = _extract_ranges(initial_html)
            target_ranges: List[Optional[str]] = []
            if ranges:
                target_ranges = list(dict.fromkeys(ranges))
            elif available_ranges:
                target_ranges = available_ranges
            else:
                target_ranges = [None]

            for range_key in target_ranges:
                html = (
                    initial_html
                    if range_key is None
                    else _fetch_page(league, range_key, season, timeout)
                )
                df = _parse_table_rows(html, league, range_key, season)
                if df.empty:
                    LOGGER.info(
                        "teamrankings %s: no rows for range=%s season=%s",
                        league,
                        range_key or "default",
                        season or "current",
                    )
                    continue

                combined_frames.append(df)
                csv_path = run.make_path(
                    f"{league.lower()}_over_under_{(season or 'current').replace('-', '')}_{range_key or 'default'}.csv"
                )
                write_dataframe(df, csv_path)
                run.record_file(
                    csv_path,
                    metadata={
                        "league": league,
                        "range": range_key or "default",
                        "season": season or "current",
                        "rows": len(df),
                    },
                    records=len(df),
                )

        if not combined_frames:
            run.set_message("No TeamRankings over/under rows captured")
            run.set_raw_path(run.storage_dir)
            return output_dir

        final_df = pd.concat(combined_frames, ignore_index=True)
        final_df = final_df.drop_duplicates(
            subset=["league", "game_date", "teamrankings_game_id", "pick"], keep="last"
        ).sort_values("game_date")
        final_df["game_date"] = pd.to_datetime(final_df["game_date"], errors="coerce")

        processed_dir = Path("data/processed/external/teamrankings")
        processed_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        parquet_path = processed_dir / f"{league.lower()}_over_under_picks_{timestamp}.parquet"
        final_df.to_parquet(parquet_path, index=False)

        run.set_records(len(final_df))
        run.set_message(
            f"Captured {len(final_df)} TeamRankings {league} rows across {len(combined_frames)} pulls"
        )
        run.set_raw_path(run.storage_dir)

    return output_dir


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download TeamRankings over/under picks for NBA/NFL/CFB",
    )
    parser.add_argument(
        "--league",
        choices=sorted(LEAGUE_PATHS.keys()),
        default="NBA",
        help="League to scrape (default: NBA)",
    )
    parser.add_argument(
        "--ranges",
        nargs="+",
        help="Optional list of date ranges to request (e.g., october last-21-days). Defaults to whatever the site exposes.",
    )
    parser.add_argument(
        "--seasons",
        nargs="+",
        help="Optional list of season slugs (e.g., 2021-2022). Defaults to the current season provided by the site.",
    )
    parser.add_argument("--timeout", type=int, default=30, help="Request timeout in seconds")
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    ingest(league=args.league, ranges=args.ranges, seasons=args.seasons, timeout=args.timeout)


if __name__ == "__main__":
    main()
