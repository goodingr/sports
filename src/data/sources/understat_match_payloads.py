"""Fetch Understat match-level rosters and shots for lineup-aware features."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import logging
import re
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Tuple

import pandas as pd
import requests

from src.data.config import PROCESSED_DATA_DIR, RAW_DATA_DIR
from .utils import (
    DEFAULT_HEADERS,
    SourceDefinition,
    source_run,
    write_dataframe,
)

LOGGER = logging.getLogger(__name__)

UNDERSTAT_LEAGUE_CODES: Dict[str, str] = {
    "EPL": "EPL",
    "LALIGA": "La_Liga",
    "BUNDESLIGA": "Bundesliga",
    "SERIEA": "Serie_A",
    "LIGUE1": "Ligue_1",
}
UNDERSTAT_PROCESSED_DIR: Dict[str, str] = {
    "EPL": "EPL",
    "LALIGA": "La_liga",
    "BUNDESLIGA": "Bundesliga",
    "SERIEA": "Serie_A",
    "LIGUE1": "Ligue_1",
}
UNDERSTAT_LEAGUE_ALIASES: Dict[str, str] = {
    "LA_LIGA": "LALIGA",
    "LALIGA": "LALIGA",
    "LA LIGA": "LALIGA",
    "SERIE_A": "SERIEA",
    "SERIE A": "SERIEA",
    "LIGUE_1": "LIGUE1",
    "LIGUE 1": "LIGUE1",
    "LA-LIGA": "LALIGA",
    "SERIE-A": "SERIEA",
    "LIGUE-1": "LIGUE1",
    "BUNDES-LIGA": "BUNDESLIGA",
    "BUNDES_LIGA": "BUNDESLIGA",
    "BUNDESLIGA": "BUNDESLIGA",
    "SERIEA": "SERIEA",
    "LIGUE1": "LIGUE1",
    "EPL": "EPL",
}
MATCH_PAYLOAD_BASE = RAW_DATA_DIR / "sources" / "soccer" / "understat_matches"

VAR_TEMPLATE = r"var\s+{name}\s*=\s*JSON\.parse\('([^']+)'\);"
DATES_PATTERN = re.compile(VAR_TEMPLATE.format(name="datesData"))
ROSTERS_PATTERN = re.compile(VAR_TEMPLATE.format(name="rostersData"))
SHOTS_PATTERN = re.compile(VAR_TEMPLATE.format(name="shotsData"))


def _normalize_league_key(league: str) -> str:
    cleaned = (league or "").strip()
    if not cleaned:
        return ""
    normalized = cleaned.replace("-", "_").upper()
    return UNDERSTAT_LEAGUE_ALIASES.get(normalized, normalized)


def _has_cached_match_data(league: str, season: int) -> bool:
    if not MATCH_PAYLOAD_BASE.exists():
        return False
    directories = sorted([path for path in MATCH_PAYLOAD_BASE.iterdir() if path.is_dir()])
    for directory in reversed(directories):
        meta_path = directory / "match_metadata.parquet"
        if not meta_path.exists():
            continue
        try:
            df = pd.read_parquet(meta_path, columns=["league", "season"])
        except Exception:  # noqa: BLE001
            continue
        if df.empty:
            continue
        leagues = df["league"].astype(str)
        seasons = pd.to_numeric(df["season"], errors="coerce")
        if ((leagues == league) & (seasons == season)).any():
            return True
    return False


@dataclass(slots=True)
class MatchMeta:
    league: str
    season: int
    match_id: str
    match_datetime: str
    home_team: str
    away_team: str


def _decode_json_blob(blob: str) -> object:
    decoded = bytes(blob, "utf-8").decode("unicode_escape")
    return json.loads(decoded)


def _extract_json(html: str, pattern: re.Pattern[str]) -> object | None:
    match = pattern.search(html)
    if not match:
        return None
    try:
        return _decode_json_blob(match.group(1))
    except json.JSONDecodeError as exc:  # pragma: no cover - defensive
        LOGGER.warning("Failed to decode Understat JSON: %s", exc)
        return None


def _fetch_league_matches(league: str, season: int, *, timeout: int = 30) -> List[MatchMeta]:
    folder = UNDERSTAT_PROCESSED_DIR.get(league)
    if folder:
        path = PROCESSED_DATA_DIR / "external" / "understat" / folder / f"{season}_dates.parquet"
        if path.exists():
            df = pd.read_parquet(path)
            if "isResult" in df.columns:
                df = df[df["isResult"] == True]  # noqa: E712
            matches: List[MatchMeta] = []
            for row in df.itertuples():
                home = (getattr(row, "h", None) or {}).get("title", "")
                away = (getattr(row, "a", None) or {}).get("title", "")
                matches.append(
                    MatchMeta(
                        league=league,
                        season=int(season),
                        match_id=str(row.id),
                        match_datetime=str(getattr(row, "datetime", "")),
                        home_team=str(home),
                        away_team=str(away),
                    )
                )
            if matches:
                return matches

    code = UNDERSTAT_LEAGUE_CODES.get(league)
    if not code:
        raise ValueError(f"No Understat mapping for league {league}")
    url = f"https://understat.com/league/{code}/{season}"
    LOGGER.info("Fetching Understat dates for %s %s", league, season)
    response = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
    response.raise_for_status()
    dates = _extract_json(response.text, DATES_PATTERN)
    if not dates:
        LOGGER.warning("No datesData found for %s %s", league, season)
        return []
    matches: List[MatchMeta] = []
    for entry in dates:
        match_id = str(entry.get("id"))
        home = entry.get("h", {}).get("title", "")
        away = entry.get("a", {}).get("title", "")
        matches.append(
            MatchMeta(
                league=league,
                season=int(season),
                match_id=match_id,
                match_datetime=str(entry.get("datetime") or ""),
                home_team=str(home),
                away_team=str(away),
            )
        )
    return matches


def _safe_float(value: object) -> float:
    try:
        if value in ("", None):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _safe_int(value: object) -> int:
    try:
        if value in ("", None):
            return 0
        return int(float(value))
    except (TypeError, ValueError):
        return 0


def _parse_rosters(html: str, meta: MatchMeta) -> List[Dict[str, object]]:
    parsed = _extract_json(html, ROSTERS_PATTERN) or {}
    rows: List[Dict[str, object]] = []
    for side_key in ("h", "a"):
        side_payload = parsed.get(side_key) or {}
        team_title = meta.home_team if side_key == "h" else meta.away_team
        for entry_id, payload in side_payload.items():
            rows.append(
                {
                    "league": meta.league,
                    "season": meta.season,
                    "match_id": meta.match_id,
                    "team_side": side_key,
                    "team_id": payload.get("team_id"),
                    "team_title": team_title,
                    "player_entry_id": entry_id,
                    "player_id": payload.get("player_id"),
                    "player_name": payload.get("player"),
                    "position": payload.get("position"),
                    "position_order": _safe_int(payload.get("positionOrder")),
                    "minutes": _safe_float(payload.get("time")),
                    "goals": _safe_float(payload.get("goals")),
                    "own_goals": _safe_float(payload.get("own_goals")),
                    "shots": _safe_float(payload.get("shots")),
                    "xg": _safe_float(payload.get("xG")),
                    "xa": _safe_float(payload.get("xA")),
                    "assists": _safe_float(payload.get("assists")),
                    "key_passes": _safe_float(payload.get("key_passes")),
                    "yellow_cards": _safe_int(payload.get("yellow_card")),
                    "red_cards": _safe_int(payload.get("red_card")),
                    "roster_in": payload.get("roster_in"),
                    "roster_out": payload.get("roster_out"),
                }
            )
    return rows


def _parse_shots(html: str, meta: MatchMeta) -> List[Dict[str, object]]:
    parsed = _extract_json(html, SHOTS_PATTERN) or {}
    rows: List[Dict[str, object]] = []
    for side_key in ("h", "a"):
        side_shots = parsed.get(side_key) or []
        team_title = meta.home_team if side_key == "h" else meta.away_team
        for payload in side_shots:
            rows.append(
                {
                    "league": meta.league,
                    "season": meta.season,
                    "match_id": meta.match_id,
                    "team_side": side_key,
                    "team_title": team_title,
                    "shot_id": payload.get("id"),
                    "minute": _safe_int(payload.get("minute")),
                    "result": payload.get("result"),
                    "x": _safe_float(payload.get("X")),
                    "y": _safe_float(payload.get("Y")),
                    "xg": _safe_float(payload.get("xG")),
                    "player_id": payload.get("player_id"),
                    "player_name": payload.get("player"),
                    "player_assisted": payload.get("player_assisted"),
                    "situation": payload.get("situation"),
                    "shot_type": payload.get("shotType"),
                    "last_action": payload.get("lastAction"),
                    "h_goals": _safe_int(payload.get("h_goals")),
                    "a_goals": _safe_int(payload.get("a_goals")),
                    "event_date": payload.get("date"),
                }
            )
    return rows


def _download_match_payload(
    meta: MatchMeta,
    *,
    timeout: int,
    max_retries: int = 3,
    retry_wait: float = 1.5,
) -> Tuple[List[Dict[str, object]], List[Dict[str, object]]] | None:
    url = f"https://understat.com/match/{meta.match_id}"
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout)
            response.raise_for_status()
            rosters = _parse_rosters(response.text, meta)
            shots = _parse_shots(response.text, meta)
            return rosters, shots
        except requests.RequestException as exc:  # pragma: no cover - network guard
            LOGGER.warning(
                "Understat match fetch failed for %s (attempt %s/%s): %s",
                meta.match_id,
                attempt,
                max_retries,
                exc,
            )
            if attempt >= max_retries:
                return None
            time.sleep(retry_wait * attempt)
    return None


def _ingest_matches(
    matches: List[MatchMeta],
    *,
    timeout: int,
    max_workers: int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if not matches:
        return pd.DataFrame(), pd.DataFrame()

    roster_rows: List[Dict[str, object]] = []
    shot_rows: List[Dict[str, object]] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(
                _download_match_payload,
                meta,
                timeout=timeout,
            ): meta
            for meta in matches
        }
        for future in concurrent.futures.as_completed(future_map):
            meta = future_map[future]
            try:
                result = future.result()
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Failed to process Understat match %s: %s", meta.match_id, exc)
                continue
            if not result:
                continue
            rosters, shots = result
            roster_rows.extend(rosters)
            shot_rows.extend(shots)

    rosters_df = pd.DataFrame(roster_rows)
    shots_df = pd.DataFrame(shot_rows)
    return rosters_df, shots_df


def ingest(
    *,
    leagues: Iterable[str] | None = None,
    seasons: Iterable[int] | None = None,
    timeout: int = 30,
    max_workers: int = 6,
    force: bool = False,
) -> str:
    requested_leagues = leagues or UNDERSTAT_LEAGUE_CODES.keys()
    league_list: List[str] = []
    for raw in requested_leagues:
        canonical = _normalize_league_key(raw)
        if canonical not in UNDERSTAT_LEAGUE_CODES:
            LOGGER.warning("Unknown Understat league %s; skipping", raw)
            continue
        league_list.append(canonical)
    if not league_list:
        LOGGER.warning("No valid leagues supplied for Understat match payload ingestion")
        return ""
    season_list = sorted({int(season) for season in (seasons or [])}) or [2024]

    definition = SourceDefinition(
        key="understat_match_payloads",
        name="Understat match rosters & shots",
        league=None,
        category="lineups",
        url="https://understat.com/",
        default_frequency="daily",
        storage_subdir="soccer/understat_matches",
    )

    all_matches: List[MatchMeta] = []
    for league in league_list:
        for season in season_list:
            if not force and _has_cached_match_data(league, season):
                LOGGER.info("Skipping Understat match payloads for %s %s (cached)", league, season)
                continue
            try:
                all_matches.extend(_fetch_league_matches(league, season, timeout=timeout))
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Skipping %s %s due to error: %s", league, season, exc)

    if not all_matches:
        LOGGER.info("No new Understat match payloads to download (use --force to override)")
        return ""

    with source_run(definition) as run:
        run.set_raw_path(run.storage_dir)
        metadata_records = [
            {
                "league": meta.league,
                "season": meta.season,
                "match_id": meta.match_id,
                "match_datetime": meta.match_datetime,
                "home_team": meta.home_team,
                "away_team": meta.away_team,
            }
            for meta in all_matches
        ]

        rosters_df, shots_df = _ingest_matches(
            all_matches,
            timeout=timeout,
            max_workers=max_workers,
        )

        meta_df = pd.DataFrame(metadata_records)

        roster_path = write_dataframe(rosters_df, run.make_path("match_players.parquet"))
        shots_path = write_dataframe(shots_df, run.make_path("match_shots.parquet"))
        meta_path = write_dataframe(meta_df, run.make_path("match_metadata.parquet"))

        run.record_file(roster_path, records=len(rosters_df))
        run.record_file(shots_path, records=len(shots_df))
        run.record_file(meta_path, records=len(meta_df))
        run.set_records(len(rosters_df) + len(shots_df))
        run.set_message(
            f"Downloaded {len(metadata_records)} matches "
            f"({len(rosters_df)} roster rows, {len(shots_df)} shot rows)"
        )
        return str(run.storage_dir)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch Understat match-level rosters and shots")
    parser.add_argument(
        "--leagues",
        default=None,
        help="Comma-separated league codes (EPL,LALIGA,BUNDESLIGA,SERIEA,LIGUE1)",
    )
    parser.add_argument(
        "--seasons",
        default=None,
        help="Comma-separated season start years (e.g., 2021,2022)",
    )
    parser.add_argument("--timeout", type=int, default=30, help="Per-request timeout in seconds")
    parser.add_argument("--max-workers", type=int, default=6, help="Concurrent match fetch workers")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-download match payloads even if cached data already exists",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO)
    leagues = (
        [item.strip() for item in args.leagues.split(",") if item.strip()]
        if args.leagues
        else None
    )
    seasons = (
        [int(item.strip()) for item in args.seasons.split(",") if item.strip()]
        if args.seasons
        else None
    )
    ingest(
        leagues=leagues,
        seasons=seasons,
        timeout=args.timeout,
        max_workers=args.max_workers,
        force=args.force,
    )


if __name__ == "__main__":  # pragma: no cover - CLI entry
    main()
