"""Download historical NBA odds from SportsbookReviewOnline archives."""

from __future__ import annotations

import argparse
import logging
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

from src.data.sources.utils import SourceDefinition, source_run, write_dataframe

LOGGER = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

ARCHIVE_URLS = {
    "NBA": "https://www.sportsbookreviewsonline.com/scoresoddsarchives/nba/nbaoddsarchives.htm",
    "CFB": "https://www.sportsbookreviewsonline.com/scoresoddsarchives/ncaafootball/ncaafootballoddsarchives.htm",
}

SEASON_LINK_PATTERNS = {
    "NBA": re.compile(r"/scoresoddsarchives/nba-odds-(\d{4}-\d{2})/?"),
    "CFB": re.compile(r"/scoresoddsarchives/ncaa-football-(\d{4}-\d{2})/?"),
}

TEAM_OVERRIDES = {
    "GoldenState": "Golden State",
    "LAClippers": "LA Clippers",
    "LALakers": "LA Lakers",
    "NewOrleans": "New Orleans",
    "NewYork": "New York",
    "OklahomaCity": "Oklahoma City",
    "SanAntonio": "San Antonio",
}


def _request(session: requests.Session, url: str) -> str:
    LOGGER.debug("Fetching %s", url)
    response = session.get(url, timeout=60)
    response.raise_for_status()
    return response.text


def _list_season_links(session: requests.Session, league: str) -> Dict[str, str]:
    archive_url = ARCHIVE_URLS[league]
    html = _request(session, archive_url)
    soup = BeautifulSoup(html, "html.parser")
    links: Dict[str, str] = {}
    pattern = SEASON_LINK_PATTERNS[league]
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        match = pattern.search(href)
        if not match:
            continue
        slug = match.group(1)
        url = urljoin(archive_url, href)
        links.setdefault(slug, url)
    if not links:
        raise RuntimeError(f"Unable to locate season links on {archive_url}")
    return links


def _clean_team(value: str) -> str:
    value = value.strip()
    if value in TEAM_OVERRIDES:
        return TEAM_OVERRIDES[value]
    if not value:
        return value
    return re.sub(r"(?<!^)(?=[A-Z])", " ", value)


def _safe_float(value: str | None) -> Optional[float]:
    if value is None:
        return None
    cleaned = value.strip().lower()
    if not cleaned:
        return None
    if cleaned in {"pk", "pick"}:
        return 0.0
    if cleaned in {"even", "ev", "e"}:
        return 100.0
    cleaned = cleaned.replace(",", "")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _safe_int(value: str | None) -> Optional[int]:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    try:
        return int(cleaned)
    except ValueError:
        try:
            return int(float(cleaned))
        except ValueError:
            return None


def _season_years(slug: str) -> Tuple[int, int]:
    try:
        start, end = slug.split("-")
        start_year = int(start)
        if len(end) == 2:
            end_year = int(str(start_year)[:2] + end)
        else:
            end_year = int(end)
        return start_year, end_year
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"Invalid season slug '{slug}'") from exc


def _parse_table(html: str, season: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "html.parser")
    table = soup.find("table")
    if not table:
        LOGGER.warning("No odds table found for season %s", season)
        return pd.DataFrame()

    rows: List[List[str]] = []
    for tr in table.find_all("tr"):
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        if not cells:
            continue
        first = cells[0].strip().lower()
        if first in {"date", ""}:
            continue
        rows.append(cells)

    if not rows or len(rows) < 2:
        return pd.DataFrame()
    if len(rows) % 2 != 0:
        LOGGER.warning("Uneven row count detected for season %s", season)
        rows = rows[:-1]

    start_year, _ = _season_years(season)
    current_year = start_year
    previous_month: Optional[int] = None

    records: List[dict] = []
    for idx in range(0, len(rows), 2):
        visitor = rows[idx]
        home = rows[idx + 1]
        date_value = visitor[0].zfill(4)
        month = int(date_value[:2])
        day = int(date_value[2:])
        if previous_month is not None and month < previous_month:
            current_year += 1
        previous_month = month

        game_dt = datetime(current_year, month, day, tzinfo=timezone.utc)

        record = {
            "season": season,
            "game_date": game_dt,
            "visitor_rotation": _safe_int(visitor[1]),
            "home_rotation": _safe_int(home[1]),
            "visitor_team": _clean_team(visitor[3]),
            "home_team": _clean_team(home[3]),
            "visitor_q1": _safe_int(visitor[4]),
            "visitor_q2": _safe_int(visitor[5]),
            "visitor_q3": _safe_int(visitor[6]),
            "visitor_q4": _safe_int(visitor[7]),
            "visitor_score": _safe_int(visitor[8]),
            "home_q1": _safe_int(home[4]),
            "home_q2": _safe_int(home[5]),
            "home_q3": _safe_int(home[6]),
            "home_q4": _safe_int(home[7]),
            "home_score": _safe_int(home[8]),
            "total_open": _safe_float(visitor[9]),
            "total_close": _safe_float(visitor[10]),
            "visitor_moneyline": _safe_float(visitor[11]),
            "visitor_second_half": _safe_float(visitor[12]),
            "spread_open": _safe_float(home[9]),
            "spread_close": _safe_float(home[10]),
            "home_moneyline": _safe_float(home[11]),
            "home_second_half": _safe_float(home[12]),
        }
        records.append(record)

    return pd.DataFrame.from_records(records)


def ingest(
    *,
    league: str = "NBA",
    seasons: Optional[Sequence[str]] = None,
) -> str:
    league = league.upper()
    if league not in ARCHIVE_URLS:
        raise ValueError(f"Unsupported league '{league}'. Only NBA archives are available.")

    definition = SourceDefinition(
        key=f"sbro_{league.lower()}",
        name=f"SBR Odds Archive {league}",
        league=league,
        category="odds",
        url=ARCHIVE_URLS[league],
        default_frequency="manual",
        storage_subdir=f"sbro/{league.lower()}",
    )

    session = requests.Session()
    session.headers.update(HEADERS)

    season_links = _list_season_links(session, league)
    selected_seasons = list(seasons) if seasons else list(season_links.keys())
    missing = [season for season in selected_seasons if season not in season_links]
    if missing:
        raise ValueError(f"Unknown season slug(s): {', '.join(missing)}")

    output_dir = ""
    with source_run(definition) as run:
        output_dir = str(run.storage_dir)
        processed_dir = Path("data/processed/external/sbr") / league.lower()
        processed_dir.mkdir(parents=True, exist_ok=True)

        total_rows = 0
        for season in selected_seasons:
            url = season_links[season]
            html = _request(session, url)
            html_path = run.make_path(f"{league.lower()}_{season}.html")
            html_path.write_text(html, encoding="utf-8")
            run.record_file(html_path, metadata={"season": season, "url": url})

            df = _parse_table(html, season)
            if df.empty:
                LOGGER.warning("SBR archive %s %s returned no rows", league, season)
                continue

            df["league"] = league
            parquet_path = processed_dir / f"{season}.parquet"
            df.to_parquet(parquet_path, index=False)
            csv_path = run.make_path(f"{league.lower()}_{season}.csv")
            write_dataframe(df, csv_path)
            run.record_file(
                csv_path,
                metadata={"season": season, "rows": len(df)},
                records=len(df),
            )
            total_rows += len(df)

        run.set_records(total_rows)
        run.set_message(f"Downloaded {total_rows} rows across {len(selected_seasons)} season(s)")
        run.set_raw_path(run.storage_dir)

    return output_dir


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download SportsbookReviewOnline NBA odds archives")
    parser.add_argument(
        "--league",
        choices=("NBA", "CFB"),
        default="NBA",
        help="League to download (NBA or CFB supported)",
    )
    parser.add_argument(
        "--seasons",
        nargs="+",
        help="Optional list of season slugs like 2022-23 (default: all seasons available)",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s:%(message)s")
    ingest(league=args.league, seasons=args.seasons)


if __name__ == "__main__":
    main()
