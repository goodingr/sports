"""Fetch moneyline/spread data from ESPN scoreboards."""

from __future__ import annotations

import logging
import time
from datetime import datetime
from typing import Dict, List, Optional

import pandas as pd
import requests
from requests import RequestException

from .utils import DEFAULT_HEADERS, SourceDefinition, source_run

LOGGER = logging.getLogger(__name__)

SPORT_MAP = {
    "nfl": "football/nfl",
    "nba": "basketball/nba",
    "cfb": "football/college-football",
    "epl": "soccer/eng.1",
    "laliga": "soccer/esp.1",
    "bundesliga": "soccer/ger.1",
    "seriea": "soccer/ita.1",
    "ligue1": "soccer/fra.1",
}


def _scoreboard_url(league: str) -> str:
    return f"https://site.api.espn.com/apis/site/v2/sports/{SPORT_MAP[league]}/scoreboard"


def _request_with_retry(
    url: str,
    *,
    params: Optional[Dict[str, str]],
    headers: Dict[str, str],
    timeout: int,
    attempts: int = 3,
    backoff: float = 1.5,
) -> requests.Response:
    delay = 1.0
    last_error: Optional[BaseException] = None
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(url, params=params, timeout=timeout, headers=headers)
            response.raise_for_status()
            return response
        except RequestException as exc:  # pragma: no cover - network failures
            last_error = exc
            LOGGER.warning("ESPN request failed (attempt %s/%s): %s", attempt, attempts, exc)
            if attempt == attempts:
                raise
            time.sleep(delay)
            delay *= backoff
    assert last_error is not None
    raise last_error


def _fetch_scoreboard(league: str, date: Optional[str], *, timeout: int) -> dict:
    params = {"dates": date} if date else {}
    headers = dict(DEFAULT_HEADERS)
    headers.setdefault("Referer", "https://www.espn.com/")
    response = _request_with_retry(_scoreboard_url(league), params=params, timeout=timeout, headers=headers)
    return response.json()


def _extract_rows(payload: dict, league: str) -> pd.DataFrame:
    events = payload.get("events", [])
    rows: List[dict] = []

    for event in events:
        competitions = event.get("competitions") or []
        if not competitions:
            continue

        competition = competitions[0]
        odds_list = competition.get("odds") or []
        if not odds_list:
            continue

        odds_obj = odds_list[0]
        provider = odds_obj.get("provider", {}).get("name", "")
        start_time = competition.get("date") or event.get("date")
        start_dt = pd.to_datetime(start_time)
        game_id = event.get("id") or competition.get("id")

        moneyline = odds_obj.get("moneyline") or {}
        spread = odds_obj.get("pointSpread") or {}
        total = odds_obj.get("total") or {}

        competitors = competition.get("competitors") or []
        for team_entry in competitors:
            team = team_entry.get("team", {})
            short_name = team.get("abbreviation") or team.get("shortDisplayName") or team.get("name")
            is_home = team_entry.get("homeAway") == "home"

            team_ml = moneyline.get("home" if is_home else "away", {})
            team_spread = spread.get("home" if is_home else "away", {})

            rows.append(
                {
                    "league": league.upper(),
                    "event_id": game_id,
                    "start_time": start_dt.isoformat(),
                    "team": short_name,
                    "is_home": int(is_home),
                    "provider": provider,
                    "moneyline_open": team_ml.get("open", {}).get("odds"),
                    "moneyline_close": team_ml.get("close", {}).get("odds"),
                    "spread_open": team_spread.get("open", {}).get("line"),
                    "spread_open_price": team_spread.get("open", {}).get("odds"),
                    "spread_close": team_spread.get("close", {}).get("line"),
                    "spread_close_price": team_spread.get("close", {}).get("odds"),
                    "total_open": total.get("over", {}).get("open", {}).get("line"),
                    "total_close": total.get("over", {}).get("close", {}).get("line"),
                }
            )

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    df["retrieved_at"] = datetime.utcnow().isoformat()
    return df


def _ingest(
    league: str,
    *,
    date: Optional[str],
    timeout: int,
) -> str:
    definition = SourceDefinition(
        key=f"espn_odds_{league}",
        name=f"ESPN odds {league.upper()}",
        league=league.upper(),
        category="odds",
        url=_scoreboard_url(league),
        default_frequency="hourly",
        storage_subdir=f"{league}/espn_odds",
    )

    output_dir = ""
    with source_run(definition) as run:
        output_dir = str(run.storage_dir)
        payload = _fetch_scoreboard(league, date, timeout=timeout)
        df = _extract_rows(payload, league)

        if df.empty:
            run.set_message("No odds data returned")
            run.set_raw_path(run.storage_dir)
            return output_dir

        csv_path = run.make_path("odds.csv")
        csv_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(csv_path, index=False)
        run.record_file(csv_path, metadata={"rows": len(df)}, records=len(df))

        run.set_records(len(df))
        run.set_message(f"Captured {len(df)} ESPN odds rows")
        run.set_raw_path(run.storage_dir)

    return output_dir


def ingest_nfl(*, date: Optional[str] = None, timeout: int = 30) -> str:
    return _ingest("nfl", date=date, timeout=timeout)


def ingest_nba(*, date: Optional[str] = None, timeout: int = 30) -> str:
    return _ingest("nba", date=date, timeout=timeout)


def ingest_cfb(*, date: Optional[str] = None, timeout: int = 30) -> str:
    return _ingest("cfb", date=date, timeout=timeout)


def ingest_epl(*, date: Optional[str] = None, timeout: int = 30) -> str:
    return _ingest("epl", date=date, timeout=timeout)


def ingest_laliga(*, date: Optional[str] = None, timeout: int = 30) -> str:
    return _ingest("laliga", date=date, timeout=timeout)


def ingest_bundesliga(*, date: Optional[str] = None, timeout: int = 30) -> str:
    return _ingest("bundesliga", date=date, timeout=timeout)


def ingest_seriea(*, date: Optional[str] = None, timeout: int = 30) -> str:
    return _ingest("seriea", date=date, timeout=timeout)


def ingest_ligue1(*, date: Optional[str] = None, timeout: int = 30) -> str:
    return _ingest("ligue1", date=date, timeout=timeout)


__all__ = ["ingest_nfl", "ingest_nba", "ingest_cfb", "ingest_epl", "ingest_laliga", "ingest_bundesliga", "ingest_seriea", "ingest_ligue1"]
