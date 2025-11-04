"""Scrape schedules from Basketball-Reference."""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.db.loaders import load_schedules

from .utils import DEFAULT_HEADERS, SourceDefinition, source_run, write_dataframe, write_text


LOGGER = logging.getLogger(__name__)

MONTHS = [
    "october",
    "november",
    "december",
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
]


def _normalize_seasons(seasons: Iterable[int] | None) -> List[int]:
    if seasons:
        return sorted({int(season) for season in seasons})
    current = datetime.now().year
    return [current]


def _month_url(season: int, month: str) -> str:
    return f"https://www.basketball-reference.com/leagues/NBA_{season}_games-{month}.html"


def _parse_schedule(html: str, season: int) -> pd.DataFrame:
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", {"id": "schedule"})
    if not table:
        return pd.DataFrame()

    rows = []
    for tr in table.tbody.find_all("tr", recursive=False):
        if "class" in tr.attrs and "thead" in tr["class"]:
            continue

        date_cell = tr.find("th", {"data-stat": "date_game"})
        if not date_cell:
            continue
        date_iso = date_cell.get("csk") or date_cell.get_text(strip=True)
        if not date_iso:
            continue

        visitor_cell = tr.find("td", {"data-stat": "visitor_team_name"})
        home_cell = tr.find("td", {"data-stat": "home_team_name"})
        if not visitor_cell or not home_cell:
            continue

        visitor = visitor_cell.get_text(strip=True)
        home = home_cell.get_text(strip=True)

        visitor_pts_cell = tr.find("td", {"data-stat": "visitor_pts"})
        home_pts_cell = tr.find("td", {"data-stat": "home_pts"})
        try:
            visitor_pts = int(visitor_pts_cell.get_text(strip=True)) if visitor_pts_cell and visitor_pts_cell.get_text(strip=True) else None
        except ValueError:
            visitor_pts = None
        try:
            home_pts = int(home_pts_cell.get_text(strip=True)) if home_pts_cell and home_pts_cell.get_text(strip=True) else None
        except ValueError:
            home_pts = None

        time_cell = tr.find("td", {"data-stat": "game_start_time"})
        gametime = time_cell.get_text(strip=True) if time_cell else ""

        box_td = tr.find("td", {"data-stat": "box_score_text"})
        box_link = box_td.find("a").get("href") if box_td and box_td.find("a") else None
        game_id = (
            f"NBA_{Path(box_link).stem.upper()}" if box_link else f"NBA_{season}_{visitor}_{home}".replace(" ", "_")
        )
        
        # Try to extract odds from schedule row if available
        # Some Basketball-Reference pages have betting lines in the schedule
        line_cells = tr.find_all("td", {"data-stat": lambda x: x and "line" in x.lower()})
        home_ml = None
        away_ml = None
        for cell in line_cells:
            text = cell.get_text(strip=True)
            # Look for moneyline format (+150, -180, etc.)
            if text and ("+" in text or "-" in text):
                try:
                    ml_value = int(text)
                    # Determine which team this is for (first occurrence is usually home, second away)
                    if home_ml is None:
                        home_ml = ml_value
                    elif away_ml is None:
                        away_ml = ml_value
                except ValueError:
                    pass

        game_type_td = tr.find("td", {"data-stat": "game_type"})
        game_type = game_type_td.get_text(strip=True) if game_type_td else "Regular"

        rows.append(
            {
                "game_id": game_id,
                "season": season,
                "game_type": game_type,
                "week": None,
                "gameday": date_iso,
                "gametime": gametime,
                "home_team": home,
                "away_team": visitor,
                "home_score": home_pts,
                "away_score": visitor_pts,
                "home_moneyline": home_ml,
                "away_moneyline": away_ml,
                "spread_line": None,
                "total_line": None,
                "stadium": None,
                "pfr": box_link,
            }
        )

    return pd.DataFrame(rows)


def ingest(*, seasons: Iterable[int] | None = None, timeout: int = 30) -> str:
    season_list = _normalize_seasons(seasons)
    definition = SourceDefinition(
        key="basketball_reference",
        name="Basketball-Reference schedules",
        league="NBA",
        category="schedules",
        url="https://www.basketball-reference.com/",
        default_frequency="daily",
        storage_subdir="nba/basketball_reference",
    )

    output_dir = ""
    with source_run(definition) as run:
        output_dir = str(run.storage_dir)
        total_rows = 0

        for season in season_list:
            season_rows = 0
            for month in MONTHS:
                url = _month_url(season, month)
                response = requests.get(url, timeout=timeout, headers=DEFAULT_HEADERS)
                if response.status_code == 404:
                    continue
                response.raise_for_status()

                html_filename = f"{season}_{month}.html"
                html_path = run.make_path(html_filename)
                write_text(response.text, html_path)
                run.record_file(html_path, season=season, metadata={"url": url, "month": month})

                df = _parse_schedule(response.text, season)
                if df.empty:
                    continue

                parquet_path = run.make_path(f"{season}_{month}.parquet")
                write_dataframe(df, parquet_path)
                run.record_file(
                    parquet_path,
                    season=season,
                    metadata={"url": url, "month": month, "row_count": len(df)},
                    records=len(df),
                )
                load_schedules(df, source_version="basketball_reference", league="NBA")
                season_rows += len(df)

            total_rows += season_rows
            LOGGER.info("Loaded %s rows from Basketball-Reference for %s", season_rows, season)

        if total_rows:
            run.set_records(total_rows)
            run.set_message(f"Loaded {total_rows} Basketball-Reference rows")
        run.set_raw_path(run.storage_dir)

    return output_dir


__all__ = ["ingest"]

