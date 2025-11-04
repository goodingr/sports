"""Fetch live NBA injury reports from the NBA CDN."""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
import requests
from requests import HTTPError

from src.db.loaders import store_injury_reports

from .utils import DEFAULT_HEADERS, SourceDefinition, source_run, write_json


LOGGER = logging.getLogger(__name__)

INJURY_URL = "https://cdn.nba.com/static/json/liveData/injuries/injuries_00.json"


def _nba_season_from_date(dt: pd.Timestamp | None) -> int:
    if dt is None or pd.isna(dt):
        now = datetime.utcnow()
        return now.year if now.month >= 7 else now.year - 1
    year = dt.year
    return year if dt.month >= 7 else year - 1


def _parse_payload(payload: Dict[str, Any]) -> pd.DataFrame:
    result_sets = payload.get("resultSets") or payload.get("resultSet")
    rows: List[Dict[str, Any]] = []

    if isinstance(result_sets, list):
        for result in result_sets:
            headers = result.get("headers") or []
            rowset = result.get("rowSet") or []
            if not headers or not rowset:
                continue
            for row in rowset:
                if isinstance(row, list):
                    rows.append(dict(zip(headers, row)))
    elif isinstance(result_sets, dict):
        headers = result_sets.get("headers") or []
        for row in result_sets.get("rowSet") or []:
            rows.append(dict(zip(headers, row)))

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    rename_map = {
        "TEAM_ABBREVIATION": "team_code",
        "TEAM_CITY": "team_name",
        "TEAM_NAME": "team_full_name",
        "PLAYER_NAME": "player_name",
        "PLAYER_POSITION": "position",
        "INJURY": "status",
        "DESCRIPTION": "detail",
        "UPDATE_DATE": "report_date",
        "STATUS": "practice_status",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    if "report_date" in df.columns:
        report_dates = pd.to_datetime(df["report_date"], errors="coerce")
    else:
        report_dates = pd.Series(datetime.utcnow(), index=df.index)
    df["report_date"] = report_dates.dt.strftime("%Y-%m-%d")
    df["season"] = report_dates.apply(_nba_season_from_date)

    if "game_date" in df.columns:
        df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce").dt.strftime("%Y-%m-%d")

    df["team_name"] = df.get("team_full_name", df.get("team_name"))
    return df


def ingest(*, timeout: int = 30) -> str:
    definition = SourceDefinition(
        key="nba_injuries",
        name="NBA live injuries",
        league="NBA",
        category="injuries",
        url=INJURY_URL,
        default_frequency="daily",
        storage_subdir="nba/injuries",
    )

    output_dir = ""
    with source_run(definition) as run:
        output_dir = str(run.storage_dir)
        LOGGER.info("Fetching NBA injury JSON payload")
        headers = dict(DEFAULT_HEADERS)
        headers.update(
            {
                "Referer": "https://www.nba.com/",
                "Origin": "https://www.nba.com",
                "x-nba-stats-token": "true",
                "x-nba-stats-origin": "stats",
                "Accept": "application/json, text/plain, */*",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
            }
        )
        try:
            response = requests.get(INJURY_URL, timeout=timeout, headers=headers)
            response.raise_for_status()
        except HTTPError as exc:  # pragma: no cover - network guard
            run.set_message(f"Failed to fetch NBA injuries: {exc}")
            run.set_raw_path(run.storage_dir)
            LOGGER.warning("NBA injuries request failed: %s", exc)
            return output_dir

        payload = response.json()
        json_path = run.make_path("injuries.json")
        write_json(payload, json_path)
        run.record_file(json_path, metadata={"url": INJURY_URL})

        df = _parse_payload(payload)
        if df.empty:
            run.set_message("No NBA injuries reported")
            run.set_raw_path(run.storage_dir)
            return output_dir

        parquet_path = run.make_path("injuries.parquet")
        df.to_parquet(parquet_path, index=False)
        run.record_file(parquet_path, metadata={"rows": len(df)}, records=len(df))

        store_injury_reports(
            df[[
                col
                for col in df.columns
                if col
                in {
                    "team_code",
                    "team_name",
                    "player_name",
                    "position",
                    "status",
                    "practice_status",
                    "report_date",
                    "game_date",
                    "detail",
                    "season",
                }
            ]].copy(),
            league="NBA",
            source_key="nba_injuries",
        )

        run.set_records(len(df))
        run.set_message(f"Captured {len(df)} NBA injury rows")
        run.set_raw_path(run.storage_dir)

    return output_dir


__all__ = ["ingest"]

