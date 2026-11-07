"""Storage utilities for the prediction system."""

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import AbstractSet, Any, Optional

import numpy as np
import pandas as pd

from src.db.core import PREDICTION_METADATA_COLUMNS, connect, ensure_prediction_metadata_columns
from src.predict.quote_order import latest_market_snapshot_id

LOGGER = logging.getLogger(__name__)


BOOK_PRIORITY = (
    "draftkings",
    "fanduel",
    "betmgm",
    "caesars",
    "betrivers",
    "pointsbet",
    "pinnacle",
    "bovada",
    "betonline.ag",
    "lowvig.ag",
)


def _book_rank(book_name: Any) -> int:
    normalized = str(book_name or "").strip().lower()
    try:
        return BOOK_PRIORITY.index(normalized)
    except ValueError:
        return len(BOOK_PRIORITY)


def _clean_value(value: Any) -> Any:
    if pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    return value


def _normalize_outcome(outcome: Any, home_team: str, away_team: str) -> str:
    value = str(outcome or "").strip().lower()
    home_norm = home_team.lower().strip()
    away_norm = away_team.lower().strip()
    if value in {"home", "away", "over", "under", "draw"}:
        return value
    if value in {"home", home_norm} or (value and (value in home_norm or home_norm in value)):
        return "home"
    if value in {"away", away_norm} or (value and (value in away_norm or away_norm in value)):
        return "away"
    return value


def _empty_game_odds_columns(games_df: pd.DataFrame) -> pd.DataFrame:
    games_df = games_df.copy()
    for column in [
        "home_moneyline",
        "away_moneyline",
        "total_line",
        "over_moneyline",
        "under_moneyline",
        "spread_line",
        "selected_h2h_book",
        "selected_h2h_snapshot_id",
        "selected_h2h_book_id",
        "selected_h2h_fetched_at",
        "selected_totals_book",
        "selected_totals_snapshot_id",
        "selected_totals_book_id",
        "selected_totals_fetched_at",
        "selected_odds_fetched_at",
    ]:
        games_df[column] = None
    return games_df


def _apply_deterministic_odds(games_df: pd.DataFrame, odds_df: pd.DataFrame) -> pd.DataFrame:
    games_df = _empty_game_odds_columns(games_df)
    if odds_df.empty:
        return games_df

    odds_df = odds_df.copy()
    odds_df["fetched_at_utc"] = pd.to_datetime(
        odds_df["fetched_at_utc"], format="mixed", utc=True, errors="coerce"
    )
    numeric_prices = pd.to_numeric(odds_df["price_american"], errors="coerce")
    odds_df["_valid_price"] = np.isfinite(numeric_prices) & numeric_prices.abs().ge(100)
    odds_df["book_rank"] = odds_df["book_name"].map(_book_rank)
    odds_df["_sort_book"] = odds_df["book_name"].astype(str).str.lower()
    odds_df = odds_df.sort_values(
        ["fetched_at_utc", "book_rank", "_sort_book", "snapshot_id"],
        ascending=[False, True, True, False],
    )

    for idx, row in games_df.iterrows():
        game_id = row["game_id"]
        home_team = str(row["home_team"])
        away_team = str(row["away_team"])
        game_odds = odds_df[odds_df["game_id"] == game_id]
        if game_odds.empty:
            continue

        h2h_odds = game_odds[game_odds["market"] == "h2h"].copy()
        h2h_odds["side"] = h2h_odds["outcome"].map(
            lambda value: _normalize_outcome(value, home_team, away_team)
        )
        paired_h2h = h2h_odds[
            h2h_odds["side"].isin(["home", "away"])
            & h2h_odds["fetched_at_utc"].notna()
            & h2h_odds["_valid_price"]
        ]
        for _, group in paired_h2h.groupby(["snapshot_id", "book_id"], sort=False):
            if {"home", "away"} <= set(group["side"]):
                home = group[group["side"] == "home"].iloc[0]
                away = group[group["side"] == "away"].iloc[0]
                games_df.at[idx, "home_moneyline"] = _clean_value(home["price_american"])
                games_df.at[idx, "away_moneyline"] = _clean_value(away["price_american"])
                games_df.at[idx, "selected_h2h_book"] = _clean_value(home["book_name"])
                games_df.at[idx, "selected_h2h_snapshot_id"] = home["snapshot_id"]
                games_df.at[idx, "selected_h2h_book_id"] = int(home["book_id"])
                fetched_at = home["fetched_at_utc"].isoformat()
                games_df.at[idx, "selected_h2h_fetched_at"] = fetched_at
                games_df.at[idx, "selected_odds_fetched_at"] = fetched_at
                break
        else:
            for _, odd in h2h_odds.iterrows():
                side = odd["side"]
                if side in {"home", "away"} and pd.isna(games_df.at[idx, f"{side}_moneyline"]):
                    games_df.at[idx, f"{side}_moneyline"] = _clean_value(odd["price_american"])

        spread_odds = game_odds[game_odds["market"] == "spreads"]
        for _, odd in spread_odds.iterrows():
            side = _normalize_outcome(odd["outcome"], home_team, away_team)
            if side == "home" and pd.notna(odd["line"]):
                games_df.at[idx, "spread_line"] = _clean_value(odd["line"])
                break
            if side == "away" and pd.notna(odd["line"]):
                games_df.at[idx, "spread_line"] = -float(odd["line"])
                break

        totals = game_odds[game_odds["market"] == "totals"].copy()
        if not totals.empty:
            totals["side"] = totals["outcome"].map(lambda value: _normalize_outcome(value, home_team, away_team))
            paired_totals = totals[
                totals["side"].isin(["over", "under"])
                & totals["line"].notna()
                & totals["_valid_price"]
                & totals["fetched_at_utc"].notna()
            ]
            selected_pair = None
            for (_, _, line_value), group in paired_totals.groupby(["snapshot_id", "book_id", "line"], sort=False):
                sides = set(group["side"])
                if {"over", "under"} <= sides:
                    over = group[group["side"] == "over"].iloc[0]
                    under = group[group["side"] == "under"].iloc[0]
                    selected_pair = (line_value, over, under)
                    break

            if selected_pair is not None:
                line_value, over, under = selected_pair
                games_df.at[idx, "total_line"] = _clean_value(line_value)
                games_df.at[idx, "over_moneyline"] = _clean_value(over["price_american"])
                games_df.at[idx, "under_moneyline"] = _clean_value(under["price_american"])
                games_df.at[idx, "selected_totals_book"] = _clean_value(over["book_name"])
                games_df.at[idx, "selected_totals_snapshot_id"] = over["snapshot_id"]
                games_df.at[idx, "selected_totals_book_id"] = int(over["book_id"])
                games_df.at[idx, "selected_totals_fetched_at"] = over["fetched_at_utc"].isoformat()
            else:
                for _, odd in totals.iterrows():
                    side = odd["side"]
                    if pd.isna(games_df.at[idx, "total_line"]) and pd.notna(odd["line"]):
                        games_df.at[idx, "total_line"] = _clean_value(odd["line"])
                    if side == "over" and pd.isna(games_df.at[idx, "over_moneyline"]):
                        games_df.at[idx, "over_moneyline"] = _clean_value(odd["price_american"])
                    elif side == "under" and pd.isna(games_df.at[idx, "under_moneyline"]):
                        games_df.at[idx, "under_moneyline"] = _clean_value(odd["price_american"])

    return games_df


def _apply_paper_odds(
    conn: sqlite3.Connection, games: pd.DataFrame, sport_id: int
) -> pd.DataFrame:
    """Keep both paper model inputs on the latest trial-source totals snapshot."""
    rows = []
    for game_id in games["game_id"]:
        snapshot_id = latest_market_snapshot_id(
            conn, game_id=game_id, sport_id=sport_id, source="the-odds-api", market="totals"
        )
        if snapshot_id is None:
            continue
        rows.extend(conn.execute(
            """
            SELECT o.game_id, o.market, o.outcome, o.price_american, o.line, o.snapshot_id,
                   o.book_id, b.name AS book_name, s.fetched_at_utc
            FROM odds o JOIN odds_snapshots s USING (snapshot_id)
            JOIN books b USING (book_id)
            WHERE o.game_id = ? AND o.snapshot_id = ? AND lower(b.name) = 'draftkings'
              AND o.market IN ('h2h', 'totals') AND o.price_american IS NOT NULL
            """,
            (game_id, snapshot_id),
        ))
    odds = pd.DataFrame.from_records(rows, columns=[
        "game_id", "market", "outcome", "price_american", "line", "snapshot_id",
        "book_id", "book_name", "fetched_at_utc",
    ])
    return _apply_deterministic_odds(games, odds)


def load_games_from_database(
    league: str,
    days_ahead: int = 7,
    *,
    paper_quotes: bool = False,
    fixture_ids: AbstractSet[str] | None = None,
    db_path: Path | None = None,
) -> pd.DataFrame:
    """
    Load upcoming games from the database for a specific league.
    
    Args:
        league: League code (e.g., "NFL", "NBA").
        days_ahead: Number of days into the future to look for games.
        
    Returns:
        DataFrame containing game details.
    """
    league = league.upper()
    with connect(db_path) as conn:
        sport_row = conn.execute("SELECT sport_id FROM sports WHERE league = ?", (league,)).fetchone()
        if not sport_row:
            LOGGER.warning("League %s not found in database", league)
            return pd.DataFrame()
        sport_id = sport_row[0]

        query = """
            SELECT 
                g.game_id,
                g.start_time_utc as commence_time,
                ht.name as home_team,
                at.name as away_team,
                g.status
            FROM games g
            JOIN teams ht ON g.home_team_id = ht.team_id
            JOIN teams at ON g.away_team_id = at.team_id
            WHERE g.sport_id = ?
              AND g.start_time_utc IS NOT NULL
              AND julianday(g.start_time_utc) >= julianday('now')
              AND julianday(g.start_time_utc) <= julianday('now', ?)
              AND COALESCE(g.status, 'scheduled') NOT IN ('final', 'voided')
            ORDER BY g.start_time_utc
        """
        games_df = pd.read_sql_query(query, conn, params=(sport_id, f"+{days_ahead} days"))
        if fixture_ids is not None:
            games_df = games_df[games_df["game_id"].isin(fixture_ids)]

        if games_df.empty:
            return games_df

        if paper_quotes:
            games_df = _apply_paper_odds(conn, games_df, sport_id)
        else:
            game_ids = games_df["game_id"].tolist()
            placeholders = ",".join("?" * len(game_ids))

            odds_query = f"""
                SELECT
                    o.game_id,
                    o.market,
                    o.outcome,
                    o.price_american,
                    o.line,
                    o.snapshot_id,
                    o.book_id,
                    b.name AS book_name,
                    s.fetched_at_utc
                FROM odds o
                JOIN odds_snapshots s ON o.snapshot_id = s.snapshot_id
                JOIN books b ON o.book_id = b.book_id
                WHERE o.game_id IN ({placeholders})
                  AND o.market IN ('h2h', 'totals', 'spreads')
                  AND o.price_american IS NOT NULL
            """
            odds_df = pd.read_sql_query(odds_query, conn, params=game_ids)
            games_df = _apply_deterministic_odds(games_df, odds_df)

    games_df["commence_time"] = pd.to_datetime(games_df["commence_time"])
    games_df["league"] = league

    return games_df

def save_predictions(df: pd.DataFrame, model_type: str, timestamp: Optional[datetime] = None) -> None:
    """
    Save predictions to the database.
    
    Args:
        df: DataFrame containing predictions.
        model_type: Type of model (e.g., "ensemble", "random_forest").
        timestamp: Timestamp for the prediction (defaults to now).
    """
    if df.empty:
        return

    if timestamp is None:
        timestamp = datetime.now(timezone.utc)
        
    records = []
    for _, row in df.iterrows():
        game_id = row.get("game_id")
        if not game_id:
            continue
        record = {
            "game_id": game_id,
            "model_type": model_type,
            "predicted_at": timestamp.isoformat(),
            "home_prob": row.get("home_predicted_prob"),
            "away_prob": row.get("away_predicted_prob"),
            "home_moneyline": row.get("home_moneyline"),
            "away_moneyline": row.get("away_moneyline"),
            "home_edge": row.get("home_edge"),
            "away_edge": row.get("away_edge"),
            "home_implied_prob": row.get("home_implied_prob"),
            "away_implied_prob": row.get("away_implied_prob"),
            # Totals
            "total_line": row.get("total_line"),
            "over_prob": row.get("over_predicted_prob"),
            "under_prob": row.get("under_predicted_prob"),
            "over_moneyline": row.get("over_moneyline"),
            "under_moneyline": row.get("under_moneyline"),
            "over_edge": row.get("over_edge"),
            "under_edge": row.get("under_edge"),
            "over_implied_prob": row.get("over_implied_prob"),
            "under_implied_prob": row.get("under_implied_prob"),
            "predicted_total_points": row.get("predicted_total_points"),
        }
        record.update({column: row.get(column) for column in PREDICTION_METADATA_COLUMNS})
        for market in ("totals", "h2h"):
            quote_fields = [
                f"selected_{market}_{suffix}"
                for suffix in ("snapshot_id", "book_id", "fetched_at")
            ]
            if any(pd.isna(record[field]) for field in quote_fields):
                for field in [*quote_fields, f"selected_{market}_book"]:
                    record[field] = None
        records.append({key: _clean_value(value) for key, value in record.items()})

    if not records:
        return

    records_df = pd.DataFrame(records).drop_duplicates(subset=["game_id", "model_type"], keep="last")
    records = records_df.to_dict("records")
        
    with connect() as conn:
        ensure_prediction_metadata_columns(conn)
        game_ids = [record["game_id"] for record in records]
        placeholders = ",".join("?" * len(game_ids))
        conn.execute(
            f"DELETE FROM predictions WHERE model_type = ? AND game_id IN ({placeholders})",
            [model_type, *game_ids],
        )
        cursor = conn.cursor()
        cursor.executemany("""
            INSERT INTO predictions (
                game_id, model_type, predicted_at, 
                home_prob, away_prob, home_moneyline, away_moneyline, 
                home_edge, away_edge, home_implied_prob, away_implied_prob,
                total_line, over_prob, under_prob, over_moneyline, under_moneyline,
                over_edge, under_edge, over_implied_prob, under_implied_prob,
                predicted_total_points,
                selected_totals_snapshot_id, selected_totals_book_id,
                selected_totals_book, selected_totals_fetched_at,
                selected_h2h_snapshot_id, selected_h2h_book_id,
                selected_h2h_book, selected_h2h_fetched_at,
                moneyline_model_sha256, totals_model_sha256
            ) VALUES (
                :game_id, :model_type, :predicted_at, 
                :home_prob, :away_prob, :home_moneyline, :away_moneyline, 
                :home_edge, :away_edge, :home_implied_prob, :away_implied_prob,
                :total_line, :over_prob, :under_prob, :over_moneyline, :under_moneyline,
                :over_edge, :under_edge, :over_implied_prob, :under_implied_prob,
                :predicted_total_points,
                :selected_totals_snapshot_id, :selected_totals_book_id,
                :selected_totals_book, :selected_totals_fetched_at,
                :selected_h2h_snapshot_id, :selected_h2h_book_id,
                :selected_h2h_book, :selected_h2h_fetched_at,
                :moneyline_model_sha256, :totals_model_sha256
            )
        """, records)
        
        LOGGER.info("Saved %d current predictions to database for %s", cursor.rowcount, model_type)


def load_current_predictions(model_type: str, league: Optional[str] = None) -> pd.DataFrame:
    """Load canonical current predictions from SQLite."""
    query = """
        SELECT p.*
        FROM predictions p
        JOIN games g ON g.game_id = p.game_id
        JOIN sports s ON s.sport_id = g.sport_id
        WHERE p.model_type = ?
    """
    params: list[Any] = [model_type]
    if league:
        query += " AND s.league = ?"
        params.append(league.upper())
    with connect() as conn:
        return pd.read_sql_query(query, conn, params=params)
