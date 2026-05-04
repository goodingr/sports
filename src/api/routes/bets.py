from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import numpy as np
import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from src.api.auth import get_current_user, is_user_premium
from src.dashboard.data import (
    _expand_totals,
    filter_by_version,
    get_batch_game_odds,
    get_game_odds,
    get_totals_odds_for_recommended,
    load_forward_test_data,
)
from src.data.sportsbook_urls import get_sportsbook_url
from src.data.team_mappings import get_full_team_name
from src.predict.publishable_bets import DEFAULT_OUTPUT as PUBLISHABLE_BETS_PATH

router = APIRouter(prefix="/api/bets", tags=["bets"])


def _load_gated_publishable_bets() -> dict[tuple[str, str, str], dict[str, Any]]:
    """Read the gated paid-pick output if a passing approved rule produced it.

    Returns an empty mapping when the file is missing, malformed, or the gate
    has not been satisfied. Keys are (game_id, market, side) tuples; the value
    is the serialized bet record from src.predict.publishable_bets.
    """
    path: Path = PUBLISHABLE_BETS_PATH
    if not path.exists() or not path.is_file():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(payload, dict):
        return {}
    if not payload.get("publishable_profitable_list_exists"):
        return {}
    bets_list = payload.get("bets") or []
    keyed: dict[tuple[str, str, str], dict[str, Any]] = {}
    for bet in bets_list:
        if not isinstance(bet, dict):
            continue
        game_id = bet.get("game_id")
        market = bet.get("market")
        side = bet.get("side")
        if game_id is None or market is None or side is None:
            continue
        keyed[(str(game_id), str(market).lower(), str(side).lower())] = bet
    return keyed


def _gated_keys_for_market(
    gated: dict[tuple[str, str, str], dict[str, Any]], market: str
) -> set[tuple[str, str]]:
    market = market.lower()
    return {(game_id, side) for (game_id, m, side) in gated if m == market}


def _publishable_bets_frame(
    gated: dict[tuple[str, str, str], dict[str, Any]],
    *,
    market: str | None = None,
) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    market_filter = market.lower() if market else None
    for (game_id, bet_market, side), bet in gated.items():
        if market_filter and bet_market != market_filter:
            continue
        record = dict(bet)
        record.setdefault("game_id", game_id)
        record.setdefault("market", bet_market)
        record.setdefault("side", side)
        if "commence_time" not in record and record.get("start_time_utc") is not None:
            record["commence_time"] = record.get("start_time_utc")
        if "moneyline" not in record and record.get("odds") is not None:
            record["moneyline"] = record.get("odds")
        record.setdefault("status", "Pending")
        records.append(record)
    if not records:
        return pd.DataFrame()
    frame = pd.DataFrame(records)
    if "commence_time" in frame.columns:
        frame["commence_time"] = pd.to_datetime(frame["commence_time"], utc=True, errors="coerce")
    return frame


class OddsRecord(BaseModel):
    game_id: str | None = None
    market: str | None = None
    outcome: str | None = None
    line: float | None = None
    moneyline: float | None = None
    book: str | None = None
    book_url: str | None = None
    fetched_at_utc: datetime | str | None = None


class PublicBet(BaseModel):
    game_id: str
    commence_time: datetime | None = None
    league: str | None = None
    home_team: str | None = None
    away_team: str | None = None
    status: str = "Pending"
    is_live: bool = False
    is_locked: bool = False
    result: str | None = None
    home_score: float | None = None
    away_score: float | None = None
    profit: float | None = None


class PremiumBet(PublicBet):
    predicted_total_points: float | None = None
    edge: float | None = None
    side: str | None = None
    description: str | None = None
    moneyline: float | None = None
    total_line: float | None = None
    book: str | None = None
    book_url: str | None = None
    recommended_bet: str | None = None
    odds_data: list[OddsRecord] | None = None


class StatsResponse(BaseModel):
    roi: float
    win_rate: float
    total_profit: float
    total_bets: int


class HistoryResponse(BaseModel):
    data: list[PremiumBet | PublicBet]
    total: int
    page: int
    limit: int


class UpcomingResponse(BaseModel):
    data: list[PremiumBet | PublicBet]
    count: int
    is_premium: bool


class PredictionInfo(BaseModel):
    predicted_total_points: float | None = None
    recommended_bet: str | None = None
    edge: float | None = None
    home_score: float | None = None
    away_score: float | None = None
    profit: float | None = None
    won: bool | None = None
    status: str = "Pending"


class GameOddsResponse(BaseModel):
    data: list[OddsRecord]
    count: int
    prediction: PredictionInfo


def _clean_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, pd.Timestamp):
        if pd.isna(value):
            return None
        return value.to_pydatetime()
    if isinstance(value, np.generic):
        value = value.item()
    if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    if value == "":
        return None
    return value


def _row_value(row: pd.Series, column: str) -> Any:
    return _clean_value(row.get(column))


def _string_value(row: pd.Series, column: str) -> str | None:
    value = _row_value(row, column)
    return str(value) if value is not None else None


def _float_value(row: pd.Series, column: str) -> float | None:
    value = _row_value(row, column)
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _bool_value(value: Any) -> bool | None:
    value = _clean_value(value)
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    return None


def _model_dump(model: BaseModel) -> dict[str, Any]:
    return model.model_dump(mode="json", exclude_none=True, exclude_defaults=True)


def _ensure_utc_column(df: pd.DataFrame, column: str = "commence_time") -> pd.DataFrame:
    if df.empty or column not in df.columns:
        return df
    result = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(result[column]):
        result[column] = pd.to_datetime(result[column], utc=True, errors="coerce")
    elif result[column].dt.tz is None:
        result[column] = result[column].dt.tz_localize("UTC")
    else:
        result[column] = result[column].dt.tz_convert("UTC")
    return result


def _odds_record_from_mapping(record: dict[str, Any]) -> OddsRecord:
    book = _clean_value(record.get("book"))
    book_url = _clean_value(record.get("book_url"))
    if book and not book_url:
        book_url = get_sportsbook_url(str(book))
    return OddsRecord(
        game_id=_clean_value(record.get("game_id")),
        market=_clean_value(record.get("market")),
        outcome=_clean_value(record.get("outcome")),
        line=_clean_value(record.get("line")),
        moneyline=_clean_value(record.get("moneyline")),
        book=book,
        book_url=book_url,
        fetched_at_utc=_clean_value(record.get("fetched_at_utc")),
    )


def _odds_records_from_df(df: pd.DataFrame) -> list[OddsRecord]:
    if df.empty:
        return []
    return [_odds_record_from_mapping(dict(row)) for _, row in df.iterrows()]


def _build_batch_odds_map(game_ids: list[str]) -> dict[str, list[OddsRecord]]:
    if not game_ids:
        return {}
    df_all_odds = get_batch_game_odds(game_ids)
    if df_all_odds.empty or "game_id" not in df_all_odds.columns:
        return {}

    odds_map: dict[str, list[OddsRecord]] = {}
    for game_id, group in df_all_odds.groupby("game_id"):
        odds_map[str(game_id)] = _odds_records_from_df(group)
    return odds_map


def _build_sportsbook_map(recommended: pd.DataFrame) -> dict[str, dict[str, Any]]:
    if recommended.empty:
        return {}
    side_by_game = {
        str(row["game_id"]): str(row.get("side", "")).lower()
        for _, row in recommended.iterrows()
        if _clean_value(row.get("game_id")) is not None
    }
    try:
        odds_df = get_totals_odds_for_recommended(recommended)
    except Exception as exc:
        print(f"ERROR: Deferred sportsbook fetch failed: {exc}")
        return {}
    if odds_df.empty:
        return {}

    sportsbook_map: dict[str, dict[str, Any]] = {}
    for _, odds_row in odds_df.iterrows():
        game_id = _clean_value(odds_row.get("forward_game_id"))
        if game_id is None:
            continue
        key = str(game_id)
        outcome = str(_clean_value(odds_row.get("outcome")) or "").lower()
        preferred = outcome == side_by_game.get(key)
        if key not in sportsbook_map or preferred:
            sportsbook_map[key] = {
                column: _clean_value(odds_row.get(column)) for column in odds_df.columns
            }
    return sportsbook_map


def _recommended_bet(side: str | None, line: Any) -> str | None:
    clean_line = _clean_value(line)
    if not side or clean_line is None:
        return None
    return f"{side.title()} {clean_line}"


def _public_bet(row: pd.Series, *, is_locked: bool = False) -> PublicBet:
    status = _string_value(row, "status") or "Pending"
    return PublicBet(
        game_id=str(_row_value(row, "game_id") or ""),
        commence_time=_row_value(row, "commence_time"),
        league=_string_value(row, "league"),
        home_team=_string_value(row, "home_team"),
        away_team=_string_value(row, "away_team"),
        status=status,
        is_live=bool(_row_value(row, "is_live") or False),
        is_locked=is_locked,
        result=_string_value(row, "result"),
        home_score=_float_value(row, "home_score"),
        away_score=_float_value(row, "away_score"),
        profit=_float_value(row, "profit"),
    )


def _premium_bet(
    row: pd.Series,
    *,
    odds_map: dict[str, list[OddsRecord]],
    sportsbook_map: dict[str, dict[str, Any]],
) -> PremiumBet:
    base = _public_bet(row)
    game_id = base.game_id
    sportsbook = sportsbook_map.get(game_id, {})
    side = _string_value(row, "side")
    line = (
        sportsbook.get("line")
        if sportsbook.get("line") is not None
        else _row_value(row, "total_line")
    )
    book = sportsbook.get("book") or _row_value(row, "book")
    book_url = _row_value(row, "book_url")
    if book and not book_url:
        book_url = get_sportsbook_url(str(book))

    return PremiumBet(
        **base.model_dump(),
        predicted_total_points=_float_value(row, "predicted_total_points"),
        edge=_float_value(row, "edge"),
        side=side,
        description=_string_value(row, "description"),
        moneyline=sportsbook.get("moneyline") or _float_value(row, "moneyline"),
        total_line=_clean_value(line),
        book=_clean_value(book),
        book_url=_clean_value(book_url),
        recommended_bet=_string_value(row, "recommended_bet") or _recommended_bet(side, line),
        odds_data=odds_map.get(game_id, []),
    )


def _serialize_bets(
    rows: pd.DataFrame,
    *,
    premium: bool,
    odds_map: dict[str, list[OddsRecord]] | None = None,
    sportsbook_map: dict[str, dict[str, Any]] | None = None,
    lock_unstarted: bool = False,
) -> list[dict[str, Any]]:
    odds_map = odds_map or {}
    sportsbook_map = sportsbook_map or {}
    records: list[dict[str, Any]] = []
    for _, row in rows.iterrows():
        if premium:
            records.append(
                _model_dump(_premium_bet(row, odds_map=odds_map, sportsbook_map=sportsbook_map))
            )
        else:
            is_locked = lock_unstarted or bool(_row_value(row, "is_live"))
            records.append(_model_dump(_public_bet(row, is_locked=is_locked)))
    return records


def _filter_rows_to_gated(rows: pd.DataFrame, gated_keys: set[tuple[str, str]]) -> pd.DataFrame:
    """Keep only rows whose (game_id, side) is in the gated key set."""
    if rows.empty:
        return rows
    if not gated_keys:
        return rows.iloc[0:0].copy()
    mask = rows.apply(
        lambda row: (
            str(_clean_value(row.get("game_id")) or ""),
            str(_clean_value(row.get("side")) or "").lower(),
        )
        in gated_keys,
        axis=1,
    )
    return rows[mask].copy()


def get_totals_data(model_type: str = "ensemble", version: Optional[str] = "all") -> pd.DataFrame:
    """Load and filter data for Over/Under bets."""
    t0 = time.time()
    raw_df = load_forward_test_data(model_type=model_type)
    t1 = time.time()
    print(f"DEBUG: load_forward_test_data took {t1 - t0:.4f}s")

    raw_df = filter_by_version(raw_df, version)
    df = _expand_totals(raw_df)
    t2 = time.time()
    print(f"DEBUG: _expand_totals took {t2 - t1:.4f}s")

    if df.empty:
        return df

    df["status"] = np.where(df["profit"].notna() | df["won"].notna(), "Completed", "Pending")

    if "edge" in df.columns:
        df = df[df["edge"] >= 0.06].copy()

    if "league" in df.columns and "home_team" in df.columns and "away_team" in df.columns:

        def fix_team_name(row: pd.Series, team_col: str) -> str:
            team_name = row[team_col]
            if isinstance(team_name, str) and team_name.startswith("Sv "):
                team_name = team_name[3:]
            return get_full_team_name(row["league"], team_name)

        df["home_team"] = df.apply(lambda row: fix_team_name(row, "home_team"), axis=1)
        df["away_team"] = df.apply(lambda row: fix_team_name(row, "away_team"), axis=1)

    required_dedupe_cols = ["home_team", "away_team", "commence_time", "league", "side"]
    if not df.empty and all(col in df.columns for col in required_dedupe_cols):
        before_count = len(df)
        df = df.drop_duplicates(subset=required_dedupe_cols, keep="first")
        after_count = len(df)
        if before_count > after_count:
            print(f"DEBUG: Removed {before_count - after_count} duplicate game records")

    return df


@router.get("/stats", response_model=StatsResponse)
async def get_stats(model_type: str = "ensemble") -> dict[str, Any]:
    """Get aggregate statistics for Over/Under bets."""
    df = get_totals_data(model_type)

    empty_response = StatsResponse(roi=0.0, win_rate=0.0, total_profit=0.0, total_bets=0)
    if df.empty:
        return _model_dump(empty_response)

    completed = df[df["status"] == "Completed"].copy()
    if completed.empty:
        return _model_dump(empty_response)

    total_bets = len(completed)
    wins = int(completed["won"].eq(True).sum())
    win_rate = (wins / total_bets) * 100 if total_bets > 0 else 0.0
    total_profit = completed["profit"].sum() if "profit" in completed.columns else 0.0
    total_staked = completed["stake"].sum() if "stake" in completed.columns else (total_bets * 100)
    roi = (total_profit / total_staked) * 100 if total_staked > 0 else 0.0

    return _model_dump(
        StatsResponse(
            roi=round(roi, 2),
            win_rate=round(win_rate, 2),
            total_profit=round(total_profit, 2),
            total_bets=total_bets,
        )
    )


@router.get(
    "/history",
    response_model=HistoryResponse,
    response_model_exclude_none=True,
    response_model_exclude_defaults=True,
)
async def get_history(
    page: int = Query(1, ge=1),
    limit: int = Query(20, ge=1, le=100),
    model_type: str = "ensemble",
    user: Optional[dict[str, Any]] = Depends(get_current_user),
) -> dict[str, Any]:
    """Get completed and live bets. Prediction details require premium."""
    premium = is_user_premium(user)
    df = get_totals_data(model_type)

    if df.empty:
        return _model_dump(HistoryResponse(data=[], total=0, page=page, limit=limit))

    df = _ensure_utc_column(df)
    now = pd.Timestamp.now(tz="UTC")

    if "commence_time" in df.columns:
        df["is_live"] = False
        mask_live = (df["status"] == "Pending") & (df["commence_time"] <= now)
        df.loc[mask_live, "is_live"] = True
        history = df[(df["status"] == "Completed") | mask_live].copy()
        history = history.sort_values(["is_live", "commence_time"], ascending=[False, False])
    else:
        history = df[df["status"] == "Completed"].copy()

    if premium and not history.empty:
        gated_keys = _gated_keys_for_market(_load_gated_publishable_bets(), "totals")
        completed_mask = history["status"] == "Completed"
        live_rows = _filter_rows_to_gated(history[~completed_mask], gated_keys)
        completed_rows = history[completed_mask]
        history = pd.concat([live_rows, completed_rows])
        if "is_live" in history.columns and "commence_time" in history.columns:
            history = history.sort_values(["is_live", "commence_time"], ascending=[False, False])

    start = (page - 1) * limit
    paginated = history.iloc[start : start + limit]

    odds_map: dict[str, list[OddsRecord]] = {}
    sportsbook_map: dict[str, dict[str, Any]] = {}
    if premium and not paginated.empty:
        game_ids = [
            str(row["game_id"])
            for _, row in paginated.iterrows()
            if _clean_value(row.get("game_id")) is not None
        ]
        odds_map = _build_batch_odds_map(game_ids)
        sportsbook_map = _build_sportsbook_map(paginated)

    response = HistoryResponse(
        data=_serialize_bets(
            paginated, premium=premium, odds_map=odds_map, sportsbook_map=sportsbook_map
        ),
        total=len(history),
        page=page,
        limit=limit,
    )
    return _model_dump(response)


@router.get(
    "/upcoming",
    response_model=UpcomingResponse,
    response_model_exclude_none=True,
    response_model_exclude_defaults=True,
)
async def get_upcoming(
    model_type: str = "ensemble",
    user: Optional[dict[str, Any]] = Depends(get_current_user),
) -> dict[str, Any]:
    """Get upcoming active bets.

    Premium users only receive paid recommendations sourced from the gated
    publishable-picks output. If no approved rule passes the launch gate, the
    response contains an empty data array. Free users see locked-card metadata
    for upcoming games (no edge/side/odds/recommendation fields).
    """
    premium = is_user_premium(user)
    if premium:
        gated = _load_gated_publishable_bets()
        upcoming = _publishable_bets_frame(gated)
        if upcoming.empty:
            return _model_dump(UpcomingResponse(data=[], count=0, is_premium=True))
        now = pd.Timestamp.now(tz="UTC")
        if "commence_time" in upcoming.columns:
            upcoming = upcoming[upcoming["commence_time"] > now]
            upcoming = upcoming.sort_values("commence_time", ascending=True)
        odds_map: dict[str, list[OddsRecord]] = {}
        sportsbook_map: dict[str, dict[str, Any]] = {}
        if not upcoming.empty:
            game_ids = [
                str(row["game_id"])
                for _, row in upcoming.iterrows()
                if _clean_value(row.get("game_id")) is not None
            ]
            odds_map = _build_batch_odds_map(game_ids)
        response = UpcomingResponse(
            data=_serialize_bets(
                upcoming,
                premium=True,
                odds_map=odds_map,
                sportsbook_map=sportsbook_map,
            ),
            count=len(upcoming),
            is_premium=True,
        )
        return _model_dump(response)

    df = get_totals_data(model_type)

    if df.empty:
        return _model_dump(UpcomingResponse(data=[], count=0, is_premium=premium))

    upcoming = df[df["status"] == "Pending"].copy()
    upcoming = _ensure_utc_column(upcoming)

    now = pd.Timestamp.now(tz="UTC")
    if "commence_time" in upcoming.columns:
        upcoming = upcoming[upcoming["commence_time"] > now]
        upcoming = upcoming.sort_values("commence_time", ascending=True)

    odds_map: dict[str, list[OddsRecord]] = {}
    sportsbook_map: dict[str, dict[str, Any]] = {}
    if premium and not upcoming.empty:
        game_ids = [
            str(row["game_id"])
            for _, row in upcoming.iterrows()
            if _clean_value(row.get("game_id")) is not None
        ]
        odds_map = _build_batch_odds_map(game_ids)
        sportsbook_map = _build_sportsbook_map(upcoming)

    response = UpcomingResponse(
        data=_serialize_bets(
            upcoming,
            premium=premium,
            odds_map=odds_map,
            sportsbook_map=sportsbook_map,
            lock_unstarted=not premium,
        ),
        count=len(upcoming),
        is_premium=premium,
    )
    return _model_dump(response)


@router.get(
    "/game/{game_id}/odds",
    response_model=GameOddsResponse,
    response_model_exclude_none=True,
    response_model_exclude_defaults=True,
)
async def get_odds_for_game(
    game_id: str,
    model_type: str = "ensemble",
    user: Optional[dict[str, Any]] = Depends(get_current_user),
) -> dict[str, Any]:
    """Get all sportsbook odds for a specific game. Restricted to premium users."""
    if not is_user_premium(user):
        raise HTTPException(
            status_code=403,
            detail="Premium subscription required to view detailed odds.",
        )

    odds_data = _odds_records_from_df(get_game_odds(game_id))
    gated = _load_gated_publishable_bets()
    gated_rows = _publishable_bets_frame(gated)
    if not gated_rows.empty and "game_id" in gated_rows.columns:
        gated_rows = gated_rows[gated_rows["game_id"].astype(str) == str(game_id)].copy()

    prediction_info = PredictionInfo()
    if not gated_rows.empty:
        if "edge" in gated_rows.columns:
            gated_rows = gated_rows.sort_values("edge", ascending=False)
        best_bet = gated_rows.iloc[0]
        side = _string_value(best_bet, "side")
        prediction_info = PredictionInfo(
            predicted_total_points=_float_value(best_bet, "predicted_total_points"),
            recommended_bet=_string_value(best_bet, "recommended_bet")
            or _recommended_bet(side, _row_value(best_bet, "total_line")),
            edge=_float_value(best_bet, "edge"),
            home_score=_float_value(best_bet, "home_score"),
            away_score=_float_value(best_bet, "away_score"),
            profit=_float_value(best_bet, "profit"),
            won=_bool_value(best_bet.get("won")),
            status=_string_value(best_bet, "status") or "Pending",
        )

    response = GameOddsResponse(data=odds_data, count=len(odds_data), prediction=prediction_info)
    return _model_dump(response)
