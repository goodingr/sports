"""Scrape schedules and results from Pro Football Reference."""

from __future__ import annotations

import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.db.loaders import load_schedules

from .utils import DEFAULT_HEADERS, SourceDefinition, source_run, write_dataframe, write_text


LOGGER = logging.getLogger(__name__)

BASE_URL = "https://www.pro-football-reference.com/years/{season}/games.htm"
POSTSEASON_WEEK_PATTERN = re.compile(r"(Wildcard|Division|Conference|Super Bowl)", re.IGNORECASE)


def _normalize_seasons(seasons: Iterable[int] | None) -> List[int]:
    if seasons:
        return sorted({int(season) for season in seasons})
    current = datetime.now().year
    return [current]


def _extract_game_id(box_link: Optional[str], season: int, week_label: str, home_team: str, away_team: str) -> str:
    if box_link:
        stem = Path(box_link).stem
        return f"NFL_{stem.upper()}"
    slug_week = re.sub(r"[^0-9A-Za-z]", "_", week_label)
    slug_home = re.sub(r"[^0-9A-Za-z]", "_", home_team)
    slug_away = re.sub(r"[^0-9A-Za-z]", "_", away_team)
    return f"NFL_{season}_{slug_week}_{slug_away}_AT_{slug_home}".upper()


def _parse_schedule(html: str, season: int) -> pd.DataFrame:
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", {"id": "games"})
    if not table:
        LOGGER.warning("No schedule table found for season %s", season)
        return pd.DataFrame()

    rows = []
    for tr in table.tbody.find_all("tr", recursive=False):
        if "class" in tr.attrs and "thead" in tr["class"]:
            continue

        week_th = tr.find("th", {"data-stat": "week_num"})
        if not week_th:
            continue

        week_label = week_th.get_text(strip=True)
        if not week_label or week_label.lower() == "week":
            continue

        date_cell = tr.find("td", {"data-stat": "game_date"})
        date_iso = date_cell.get("csk") if date_cell and date_cell.has_attr("csk") else date_cell.get_text(strip=True) if date_cell else ""

        time_cell = tr.find("td", {"data-stat": "game_time"})
        gametime = time_cell.get_text(strip=True) if time_cell else ""

        winner_cell = tr.find("td", {"data-stat": "winner"})
        loser_cell = tr.find("td", {"data-stat": "loser"})
        if not winner_cell or not loser_cell:
            continue

        winner = winner_cell.get_text(strip=True)
        loser = loser_cell.get_text(strip=True)

        location_cell = tr.find("td", {"data-stat": "game_location"})
        location_marker = location_cell.get_text(strip=True) if location_cell else ""

        pts_win_cell = tr.find("td", {"data-stat": "pts_win"})
        pts_lose_cell = tr.find("td", {"data-stat": "pts_lose"})

        try:
            pts_win = int(pts_win_cell.get_text(strip=True)) if pts_win_cell and pts_win_cell.get_text(strip=True) else None
        except ValueError:
            pts_win = None

        try:
            pts_lose = int(pts_lose_cell.get_text(strip=True)) if pts_lose_cell and pts_lose_cell.get_text(strip=True) else None
        except ValueError:
            pts_lose = None

        if location_marker == "@":
            away_team, home_team = winner, loser
            away_score, home_score = pts_win, pts_lose
        else:
            home_team, away_team = winner, loser
            home_score, away_score = pts_win, pts_lose

        box_td = tr.find("td", {"data-stat": "boxscore_word"})
        box_link = box_td.find("a").get("href") if box_td and box_td.find("a") else None

        game_id = _extract_game_id(box_link, season, week_label, home_team, away_team)

        game_type = "Postseason" if POSTSEASON_WEEK_PATTERN.search(week_label) else "Regular"
        week_number: Optional[int]
        try:
            week_number = int(week_label)
        except ValueError:
            week_number = None

        rows.append(
            {
                "game_id": game_id,
                "season": season,
                "game_type": game_type,
                "week": week_number,
                "gameday": date_iso,
                "gametime": gametime,
                "home_team": home_team,
                "away_team": away_team,
                "home_score": home_score,
                "away_score": away_score,
                "home_moneyline": None,
                "away_moneyline": None,
                "spread_line": None,
                "total_line": None,
                "stadium": None,
                "pfr": box_link,
            }
        )

    return pd.DataFrame(rows)


def ingest(*, seasons: Iterable[int] | None = None, timeout: int = 30) -> str:
    """Fetch NFL schedules and results from Pro Football Reference."""

    season_list = _normalize_seasons(seasons)
    definition = SourceDefinition(
        key="pro_football_reference",
        name="Pro Football Reference schedules",
        league="NFL",
        category="schedules",
        url="https://www.pro-football-reference.com/",
        default_frequency="daily",
        storage_subdir="nfl/pro_football_reference",
    )

    output_dir = ""
    with source_run(definition) as run:
        output_dir = str(run.storage_dir)
        aggregated_rows = 0

        for season in season_list:
            url = BASE_URL.format(season=season)
            LOGGER.info("Downloading Pro Football Reference schedule for %s", season)
            response = requests.get(url, timeout=timeout, headers=DEFAULT_HEADERS)
            response.raise_for_status()

            html_path = run.make_path(f"games_{season}.html")
            write_text(response.text, html_path)
            run.record_file(html_path, season=season, metadata={"url": url})

            df = _parse_schedule(response.text, season)
            if df.empty:
                LOGGER.warning("No games parsed for %s", season)
                continue

            parquet_path = run.make_path(f"schedules_{season}.parquet")
            write_dataframe(df, parquet_path)
            run.record_file(
                parquet_path,
                season=season,
                metadata={"url": url, "row_count": len(df)},
                records=len(df),
            )

            load_schedules(df, source_version="pro_football_reference", league="NFL")
            aggregated_rows += len(df)

        if aggregated_rows:
            run.set_records(aggregated_rows)
            run.set_message(f"Loaded {aggregated_rows} schedule rows from PFR")
        run.set_raw_path(run.storage_dir)

    return output_dir


__all__ = ["ingest"]

