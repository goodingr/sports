"""Scrape NBA/NHL odds (moneyline + totals) from Killersports.com."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlencode

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag

from src.data.config import load_env
from src.db import loaders

from .utils import DEFAULT_HEADERS, SourceDefinition, source_run, write_dataframe, write_text

LOGGER = logging.getLogger(__name__)

BASE_HOST = "https://killersports.com"
QUERY_ENDPOINT = f"{BASE_HOST}/query"

DEFAULT_FIELDS = {"NBA": None, "NHL": None}


@dataclass(slots=True)
class KillersportsQuery:
    league: str
    sdql: str
    query_type: str = "games"
    show: int = 5000
    future: int = 10
    fields: Optional[str] = None
    extra_params: Optional[Dict[str, str]] = None


class KillersportsClient:
    """Lightweight HTTP client that handles authentication + queries."""

    def __init__(self, timeout: int = 30, username: Optional[str] = None, password: Optional[str] = None) -> None:
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.logged_in = False
        if username and password:
            self.login(username, password)

    def login(self, username: str, password: str) -> None:
        payload = {"username": username, "password": password}
        resp = self.session.post(f"{BASE_HOST}/login", data=payload, timeout=self.timeout)
        resp.raise_for_status()
        url_lower = resp.url.lower()
        if "login_message=validation%20failed" in url_lower:
            raise RuntimeError("Killersports login failed (invalid credentials)")
        self.logged_in = True
        LOGGER.debug("Authenticated with Killersports as %s", username)

    def fetch_query(self, query: KillersportsQuery) -> str:
        params: Dict[str, str] = {
            "filter": query.league.upper(),
            "sdql": query.sdql,
            "_qt": query.query_type,
            "show": str(query.show),
            "future": str(query.future),
            "init": "1",
        }
        if query.fields:
            params["fields"] = query.fields
        if query.extra_params:
            params.update({key: str(value) for key, value in query.extra_params.items()})

        fields_payload = params.pop("fields", None)
        query_string = urlencode(params)
        if fields_payload:
            query_string = f"{query_string}&fields={fields_payload}"
        url = f"{QUERY_ENDPOINT}?{query_string}"
        resp = self.session.get(url, timeout=self.timeout)
        resp.raise_for_status()
        LOGGER.debug("Killersports query url: %s (status %s)", resp.url, resp.status_code)
        return resp.text


def _default_season_for_league(league: str, today: Optional[date] = None) -> int:
    current = today or datetime.utcnow().date()
    league_upper = league.upper()
    if league_upper in {"NBA", "NHL"}:
        return current.year if current.month >= 7 else current.year - 1
    return current.year


def _resolve_sdql(
    league: str,
    *,
    sdql: Optional[str],
    season: Optional[int],
    date_filter: Optional[str],
) -> str:
    if sdql:
        return sdql
    if date_filter:
        return f"date={date_filter}"
    if season is None:
        season = _default_season_for_league(league)
    return f"season={season}"


def _parse_date(value: str) -> Optional[str]:
    text = (value or "").strip()
    if not text or text == "-":
        return None
    for fmt in ("%b %d, %Y", "%b %d %Y"):
        try:
            return datetime.strptime(text, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    LOGGER.debug("Unable to parse Killersports date: %s", value)
    return None


def _parse_int(value: str) -> Optional[int]:
    text = (value or "").replace(",", "").strip()
    if not text or text in {"-", "–"}:
        return None
    if text.upper() == "PK":
        return 0
    try:
        return int(text)
    except ValueError:
        return None


def _parse_float(value: str) -> Optional[float]:
    text = (value or "").strip()
    if not text or text in {"-", ""}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_score(value: str) -> Tuple[Optional[int], Optional[int]]:
    text = (value or "").strip()
    if not text or text in {"-", ""} or "-" not in text:
        return None, None
    parts = text.split("-")
    if len(parts) != 2:
        return None, None
    return _parse_int(parts[0]), _parse_int(parts[1])


def _parse_rest(value: str) -> Tuple[Optional[int], Optional[int]]:
    text = (value or "").strip()
    if not text or "&" not in text:
        return None, None
    parts = text.split("&", maxsplit=1)
    return _parse_int(parts[0]), _parse_int(parts[1])


def _normalize_result(value: str) -> Optional[str]:
    text = (value or "").strip()
    return text if text and text != "-" else None


def _parse_overtime(value: str) -> Optional[int]:
    parsed = _parse_int(value)
    return parsed if parsed is not None else None


def _extract_rows(html: str, league: str) -> pd.DataFrame:
    soup = BeautifulSoup(html, "lxml")
    table = soup.find("table", id="DT_Table")
    if not isinstance(table, Tag):
        LOGGER.warning("Killersports %s response did not include DT_Table", league)
        return pd.DataFrame()

    body = table.find("tbody") or table
    records: List[Dict[str, object]] = []
    for row in body.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 10:
            continue
        values = [cell.get_text(strip=True) for cell in cells]
        rest_team, rest_opp = _parse_rest(values[7] if len(values) > 7 else "")
        team_score, opp_score = _parse_score(values[6] if len(values) > 6 else "")
        record = {
            "league": league.upper(),
            "game_date": _parse_date(values[0]) if len(values) > 0 else None,
            "day": values[1].strip() if len(values) > 1 else None,
            "season": _parse_int(values[2]) if len(values) > 2 else None,
            "team": values[3].strip() if len(values) > 3 else None,
            "opponent": values[4].strip() if len(values) > 4 else None,
            "site": values[5].strip() if len(values) > 5 else None,
            "final_score": values[6].strip() if len(values) > 6 else None,
            "team_score": team_score,
            "opponent_score": opp_score,
            "rest": values[7].strip() if len(values) > 7 else None,
            "team_rest": rest_team,
            "opponent_rest": rest_opp,
            "moneyline": _parse_int(values[8]) if len(values) > 8 else None,
            "total": _parse_float(values[9]) if len(values) > 9 else None,
            "side_result": _normalize_result(values[10]) if len(values) > 10 else None,
            "total_result": _normalize_result(values[11]) if len(values) > 11 else None,
            "overtime": _parse_overtime(values[12]) if len(values) > 12 else None,
            "row_class": " ".join(row.get("class", [])),
            "is_future": "future-game" in row.get("class", []),
        }
        records.append(record)

    if not records:
        return pd.DataFrame()

    df = pd.DataFrame(records)
    df["retrieved_at"] = datetime.utcnow().isoformat()
    return df


def _load_credentials() -> Tuple[Optional[str], Optional[str]]:
    config = load_env()
    return config.get("KILLERSPORTS_USERNAME"), config.get("KILLERSPORTS_PASSWORD")


def ingest(
    *,
    league: str = "NBA",
    sdql: Optional[str] = None,
    season: Optional[int] = None,
    future: int = 10,
    show: int = 500,
    query_type: str = "games",
    timeout: int = 30,
    date: Optional[str] = None,
    params: Optional[Dict[str, str]] = None,
) -> str:
    """Ingest Killersports odds for the requested league/situation."""
    league_upper = league.upper()
    league_lower = league_upper.lower()
    sdql_statement = _resolve_sdql(league_upper, sdql=sdql, season=season, date_filter=date)
    fields = DEFAULT_FIELDS.get(league_upper)
    definition = SourceDefinition(
        key=f"killersports_{league_lower}",
        name=f"Killersports {league_upper} odds",
        league=league_upper,
        category="odds",
        url=f"{QUERY_ENDPOINT}?filter={league_upper}",
        default_frequency="hourly",
        storage_subdir=f"{league_lower}/killersports",
    )

    username, password = _load_credentials()
    client = KillersportsClient(timeout=timeout, username=username, password=password)
    query = KillersportsQuery(
        league=league_upper,
        sdql=sdql_statement,
        query_type=query_type,
        show=show,
        future=future,
        fields=fields,
        extra_params=params,
    )
    if date:
        query.future = 0

    with source_run(definition) as run:
        LOGGER.info("Fetching Killersports %s odds with SDQL: %s", league_upper, sdql_statement)
        html = client.fetch_query(query)
        html_path = run.make_path("query.html")
        write_text(html, html_path)
        run.record_file(html_path, metadata={"league": league_upper, "sdql": sdql_statement})

        df = _extract_rows(html, league_upper)
        if df.empty:
            run.set_message("No odds rows parsed")
            run.set_records(0)
            run.set_raw_path(run.storage_dir)
            return str(run.storage_dir)

        csv_path = run.make_path("odds.csv")
        write_dataframe(df, csv_path)
        run.record_file(
            csv_path,
            metadata={
                "league": league_upper,
                "rows": len(df),
                "sdql": sdql_statement,
            },
            records=len(df),
        )
        load_message = ""
        if league_upper == "NHL":
            stats = loaders.import_killersports_odds(csv_path, league=league_upper)
            load_message = f" | Loaded {stats['matched']}/{stats['games']} games"
        run.set_records(len(df))
        run.set_message(f"Captured {len(df)} Killersports {league_upper} rows{load_message}")
        run.set_raw_path(run.storage_dir)

    return str(run.storage_dir)


__all__ = ["ingest"]
