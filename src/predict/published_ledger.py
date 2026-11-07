"""Persist the exact subscriber picks and grade their original quotes."""

from __future__ import annotations

import json
import math
import sqlite3
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from src.db.core import connect, resolve_db_path
from src.models.prediction_quality import (
    american_profit,
    expected_value,
    no_vig_pair,
    settle_moneyline_side,
    settle_total_side,
)

SCHEMA_PATH = Path(__file__).with_name("published_schema.sql")
VOID_STATUSES = {"cancelled", "canceled", "voided", "no_contest", "postponed"}
QUOTE_FIELDS = ("rule_id", "side", "moneyline", "total_line", "book", "start_time_utc")


def _utc(value: str) -> datetime:
    timestamp = datetime.fromisoformat(value)
    if timestamp.utcoffset() is None:
        raise ValueError("Published picks require timezone-aware timestamps")
    return timestamp.astimezone(timezone.utc)


def _withdraw_settlement(conn: sqlite3.Connection, row: sqlite3.Row, now: datetime) -> None:
    if row["previous_outcome"] is None:
        return
    conn.execute("DELETE FROM published_settlements WHERE pick_id = ?", (row["pick_id"],))
    conn.execute(
        """
        INSERT INTO published_settlement_history (
            pick_id, settled_at, outcome, profit_units, home_score, away_score, reason
        ) VALUES (?, ?, 'pending', 0, ?, ?, 'result_withdrawn')
        """,
        (row["pick_id"], now.isoformat(), row["home_score"], row["away_score"]),
    )


def _settle(conn: sqlite3.Connection, now: datetime) -> None:
    pending = conn.execute(
        """
        SELECT p.*, g.status, g.start_time_utc, g.home_team_id AS current_home,
               g.away_team_id AS current_away, r.home_score, r.away_score,
               settled.outcome AS previous_outcome, settled.profit_units AS previous_profit,
               settled.home_score AS previous_home, settled.away_score AS previous_away,
               settled.reason AS previous_reason, sport.league
        FROM published_picks p
        LEFT JOIN published_settlements settled USING (pick_id)
        LEFT JOIN games g USING (game_id)
        LEFT JOIN sports sport ON sport.sport_id = g.sport_id
        LEFT JOIN game_results r USING (game_id)
        """
    ).fetchall()
    for row in pending:
        pick = json.loads(row["payload"])
        start = _utc(pick["start_time_utc"])
        status = str(row["status"] or "").lower()
        reason = None
        if row["current_home"] != row["home_team_id"] or row["current_away"] != row["away_team_id"]:
            reason = "fixture_identity_changed"
        elif not row["start_time_utc"] or abs(_utc(row["start_time_utc"]) - start) > timedelta(minutes=10):
            reason = "fixture_rescheduled"
        elif _utc(row["published_at"]) >= min(start, _utc(row["start_time_utc"])):
            reason = "publication_after_corrected_start"
        elif status in VOID_STATUSES:
            reason = "fixture_voided"
        home, away = row["home_score"], row["away_score"]
        if reason:
            outcome, profit = "void", 0.0
        elif status != "final" or now < max(start, _utc(row["start_time_utc"])):
            _withdraw_settlement(conn, row, now)
            continue
        elif (
            not all(isinstance(score, int) and score >= 0 for score in (home, away))
            or (row["league"] in {"NBA", "NHL", "NCAAB"} and home == away)
        ):
            _withdraw_settlement(conn, row, now)
            continue
        else:
            if row["market"] == "totals":
                won = settle_total_side(home + away, pick["total_line"], pick["side"])
            else:
                won = (
                    None if home == away and "draw" not in pick["quote_outcomes"]
                    else settle_moneyline_side(home, away, pick["side"])
                )
            outcome = "push" if won is None else "win" if won else "loss"
            profit = 0.0 if won is None else american_profit(pick["moneyline"], won, stake=1)
            reason = "final_score"
        grade = (outcome, profit, home, away, reason)
        previous = tuple(row[f"previous_{key}"] for key in ("outcome", "profit", "home", "away", "reason"))
        if grade != previous:
            values = (row["pick_id"], now.isoformat(), *grade)
            conn.execute(
                "INSERT OR REPLACE INTO published_settlements VALUES (?, ?, ?, ?, ?, ?, ?)",
                values,
            )
            conn.execute(
                """
                INSERT INTO published_settlement_history (
                    pick_id, settled_at, outcome, profit_units, home_score, away_score, reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                values,
            )


def _valid_pick(pick: dict[str, Any], now: datetime) -> bool:
    market = pick.get("market")
    side = pick.get("side")
    if market not in {"totals", "moneyline"}:
        return False
    if side not in ({"over", "under"} if market == "totals" else {"home", "away"}):
        return False
    price = pick.get("moneyline")
    if not isinstance(price, (int, float)) or not math.isfinite(price) or abs(price) < 100:
        return False
    probability = pick.get("model_probability")
    if (
        not isinstance(probability, (int, float)) or not math.isfinite(probability)
        or not 0 <= probability <= 1 or expected_value(probability, price, stake=1) <= 0
    ):
        return False
    if market == "totals":
        line = pick.get("total_line")
        if (
            not isinstance(line, (int, float)) or not math.isfinite(line) or line <= 0
            or not float(line * 2).is_integer()
        ):
            return False
    try:
        return _utc(pick["start_time_utc"]) > now
    except (ValueError, KeyError, TypeError):
        return False


def _verified_quote(
    conn: sqlite3.Connection, pick: dict[str, Any], now: datetime
) -> dict[str, Any] | None:
    if not pick.get("quote_snapshot_id") or not pick.get("quote_book_id"):
        return None
    market = "totals" if pick["market"] == "totals" else "h2h"
    rows = conn.execute(
        """
        SELECT lower(o.outcome) AS outcome, o.line, o.price_american, sn.fetched_at_utc,
               sn.source, b.name AS book
        FROM odds o JOIN odds_snapshots sn USING (snapshot_id) JOIN books b USING (book_id)
        JOIN games g ON g.game_id = o.game_id AND g.sport_id = sn.sport_id
        WHERE o.game_id = ? AND o.snapshot_id = ? AND o.book_id = ? AND o.market = ?
          AND sn.snapshot_id = (
              SELECT newest.snapshot_id FROM odds_snapshots newest
              WHERE newest.sport_id = g.sport_id AND newest.source = sn.source
                AND EXISTS (
                    SELECT 1 FROM odds recent WHERE recent.snapshot_id = newest.snapshot_id
                      AND recent.game_id = g.game_id AND recent.market = o.market
                )
              ORDER BY julianday(newest.fetched_at_utc) DESC, newest.snapshot_id DESC LIMIT 1
          )
        """,
        (pick["game_id"], pick["quote_snapshot_id"], pick["quote_book_id"], market),
    ).fetchall()
    expected = {"over", "under"} if market == "totals" else {"home", "away"}
    if len(rows) != 2 or {row["outcome"] for row in rows} != expected:
        return None
    quote = rows[0]
    contract = pick.get("execution_contract") or {}
    max_age = min(15, contract.get("max_quote_age_minutes", 15))
    try:
        fetched = _utc(quote["fetched_at_utc"])
        predicted = _utc(pick["predicted_at"])
        if not fetched <= predicted <= now or now - fetched > timedelta(minutes=max_age):
            return None
        if contract:
            lead = _utc(pick["start_time_utc"]) - now
            if not timedelta(minutes=contract["min_minutes_before_start"]) <= lead <= timedelta(
                hours=contract["max_hours_before_start"]
            ):
                return None
    except (ValueError, TypeError, KeyError):
        return None
    for row in rows:
        if (
            row["source"] != "the-odds-api"
            or not isinstance(row["price_american"], (int, float))
            or not math.isfinite(row["price_american"]) or abs(row["price_american"]) < 100
            or (market == "totals" and row["line"] != pick["total_line"])
            or (row["outcome"] == pick["side"] and row["price_american"] != pick["moneyline"])
        ):
            return None
    if contract:
        if quote["book"].lower() != contract["book"] or quote["source"] != contract["odds_source"]:
            return None
        prices = {row["outcome"]: row["price_american"] for row in rows}
        first, second = ("over", "under") if market == "totals" else ("home", "away")
        fair = dict(zip((first, second), no_vig_pair(prices[first], prices[second])))
        if pick["model_probability"] - fair[pick["side"]] < contract["min_edge"]:
            return None
        if contract.get("same_snapshot_moneyline_inputs"):
            if (
                pick.get("input_h2h_snapshot_id") != pick["quote_snapshot_id"]
                or pick.get("input_h2h_book_id") != pick["quote_book_id"]
            ):
                return None
            inputs = conn.execute(
                """
                SELECT lower(outcome) AS outcome, price_american FROM odds
                WHERE game_id = ? AND snapshot_id = ? AND book_id = ? AND market = 'h2h'
                """,
                (pick["game_id"], pick["quote_snapshot_id"], pick["quote_book_id"]),
            ).fetchall()
            expected_inputs = {"home": pick.get("input_home_moneyline"), "away": pick.get("input_away_moneyline")}
            if (
                not all(isinstance(price, (int, float)) and math.isfinite(price) and abs(price) >= 100
                        for price in expected_inputs.values())
                or len(inputs) != 2
                or {row["outcome"]: row["price_american"] for row in inputs} != expected_inputs
            ):
                return None
    decision_close = _utc(pick["start_time_utc"]) - timedelta(
        minutes=contract.get("min_minutes_before_start", 0)
    )
    return {
        "book": quote["book"], "quote_fetched_at": fetched.isoformat(),
        "quote_expires_at": min(fetched + timedelta(minutes=max_age), decision_close).isoformat(),
        "quote_source": quote["source"], "quote_outcomes": sorted(expected),
    }


def publish_release(
    payload: dict[str, Any], db_path: Path | None = None
) -> dict[str, Any]:
    """Commit the active release and its immutable first picks together."""
    now = _utc(payload["generated_at"])
    picks = payload.get("bets", [])
    passing = payload.get("passing_approved_rule_ids", [])
    with connect(db_path) as conn:
        conn.executescript(SCHEMA_PATH.read_text(encoding="utf-8"))
        conn.execute("BEGIN IMMEDIATE")
        _settle(conn, now)
        release_id = conn.execute(
            "INSERT INTO published_releases (published_at, payload) VALUES (?, '{}')",
            (now.isoformat(),),
        ).lastrowid
        active = []
        seen = set()
        for pick in sorted(picks, key=lambda value: (-value.get("edge", 0.0), str(value["rule_id"]))):
            key = (pick["game_id"], pick["market"])
            quality = pick.get("quality_summary") or {}
            if (
                key in seen or pick.get("rule_id") not in passing
                or quality.get("passes_launch_gate") is not True
                or not _valid_pick(pick, now)
            ):
                continue
            seen.add(key)
            game = conn.execute(
                """
                SELECT g.home_team_id, g.away_team_id, g.start_time_utc, g.status
                FROM games g LEFT JOIN game_results r USING (game_id)
                WHERE game_id = ? AND r.home_score IS NULL AND r.away_score IS NULL
                """,
                (pick["game_id"],),
            ).fetchone()
            if game is None or str(game["status"]).lower() not in {"scheduled", "pre"}:
                continue
            if _utc(game["start_time_utc"]) != _utc(pick["start_time_utc"]):
                continue
            verified = _verified_quote(conn, pick, now)
            if verified is None:
                continue
            pick = {**pick, **verified}
            existing = conn.execute(
                """
                SELECT p.payload, settled.pick_id AS settled
                FROM published_picks p LEFT JOIN published_settlements settled USING (pick_id)
                WHERE p.market = ? AND (
                    p.game_id = ? OR (p.home_team_id = ? AND p.away_team_id = ?
                    AND abs(julianday(json_extract(p.payload, '$.start_time_utc')) - julianday(?)) < 2.0 / 24)
                )
                LIMIT 1
                """,
                (pick["market"], pick["game_id"], game["home_team_id"],
                 game["away_team_id"], pick["start_time_utc"]),
            ).fetchone()
            if existing:
                original = json.loads(existing["payload"])
                if existing["settled"] is None and all(
                    original.get(field) == pick.get(field) for field in QUOTE_FIELDS
                ) and original["game_id"] == pick["game_id"]:
                    active.append({
                        **original,
                        **{field: pick[field] for field in (
                            "quote_snapshot_id", "quote_book_id", "quote_fetched_at",
                            "quote_expires_at", "predicted_at", "input_h2h_snapshot_id",
                            "input_h2h_book_id", "input_home_moneyline", "input_away_moneyline",
                        ) if field in pick},
                    })
                continue
            published = {**pick, "published_at": now.isoformat()}
            conn.execute(
                """
                INSERT INTO published_picks (
                    first_release_id, game_id, market, home_team_id, away_team_id,
                    published_at, payload
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (release_id, *key, game["home_team_id"], game["away_team_id"],
                 now.isoformat(), json.dumps(published, allow_nan=False)),
            )
            active.append(published)
        result = {
            **payload, "release_id": release_id, "bets": active, "bet_count": len(active),
            "publishable_profitable_list_exists": bool(active),
        }
        conn.execute(
            "UPDATE published_releases SET payload = ? WHERE release_id = ?",
            (json.dumps(result, allow_nan=False), release_id),
        )
    return result


def load_active_release(db_path: Path | None = None) -> dict[str, Any]:
    path = resolve_db_path(db_path)
    with closing(sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN")
        if not conn.execute(
            "SELECT 1 FROM sqlite_master WHERE name = 'published_releases' AND type = 'table'"
        ).fetchone():
            return {}
        row = conn.execute(
            "SELECT payload FROM published_releases ORDER BY release_id DESC LIMIT 1"
        ).fetchone()
        if row is None:
            return {}
        payload = json.loads(row[0])
        now = datetime.now(timezone.utc)
        active = []
        for pick in payload.get("bets", []):
            current = conn.execute(
                """
                SELECT g.status, g.start_time_utc, g.home_team_id, g.away_team_id,
                       p.home_team_id AS published_home, p.away_team_id AS published_away
                FROM games g JOIN published_picks p USING (game_id)
                LEFT JOIN game_results r USING (game_id)
                WHERE g.game_id = ? AND p.market = ?
                  AND r.home_score IS NULL AND r.away_score IS NULL
                """,
                (pick["game_id"], pick["market"]),
            ).fetchone()
            if (
                current and str(current["status"]).lower() in {"scheduled", "pre"}
                and current["home_team_id"] == current["published_home"]
                and current["away_team_id"] == current["published_away"]
                and _utc(current["start_time_utc"]) == _utc(pick["start_time_utc"])
                and _valid_pick(pick, now)
                and _verified_quote(conn, pick, now) is not None
            ):
                active.append(pick)
        return {**payload, "bets": active, "bet_count": len(active),
                "publishable_profitable_list_exists": bool(active)}


def published_record(
    db_path: Path | None = None, *, page: int = 1, limit: int = 20
) -> dict[str, Any]:
    result: dict[str, Any] = {
        "data": [], "total": 0, "page": page, "limit": limit,
        "published_picks": 0, "pending_picks": 0, "last_processed_at": None, "grading_corrections": 0,
        "graded_picks": 0, "wins": 0, "losses": 0, "pushes": 0, "voids": 0,
        "profit_units": 0.0, "roi_percent": None, "win_rate_percent": None,
        "basis": "One unit per published pick at its original quote",
    }
    with closing(sqlite3.connect(resolve_db_path(db_path).resolve().as_uri() + "?mode=ro", uri=True)) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("BEGIN")
        if not conn.execute(
            "SELECT 1 FROM sqlite_master WHERE name = 'published_settlements' AND type = 'table'"
        ).fetchone():
            return result
        result["published_picks"] = conn.execute("SELECT COUNT(*) FROM published_picks").fetchone()[0]
        result["pending_picks"] = conn.execute(
            """
            SELECT COUNT(*) FROM published_picks p
            LEFT JOIN published_settlements s USING (pick_id) WHERE s.pick_id IS NULL
            """
        ).fetchone()[0]
        latest = conn.execute(
            "SELECT published_at FROM published_releases ORDER BY release_id DESC LIMIT 1"
        ).fetchone()
        result["last_processed_at"] = latest[0] if latest else None
        result["grading_corrections"] = conn.execute(
            "SELECT COUNT(*) - COUNT(DISTINCT pick_id) FROM published_settlement_history"
        ).fetchone()[0]
        counts = conn.execute(
            """
            SELECT s.outcome, COUNT(*) AS picks, SUM(s.profit_units) AS profit
            FROM published_settlements s GROUP BY s.outcome
            """
        ).fetchall()
        names = {"win": "wins", "loss": "losses", "push": "pushes", "void": "voids"}
        for row in counts:
            result[names[row["outcome"]]] = row["picks"]
            result["profit_units"] += row["profit"]
        graded = result["wins"] + result["losses"] + result["pushes"]
        result["graded_picks"] = graded
        result["roi_percent"] = 100 * result["profit_units"] / graded if graded else None
        decisive = result["wins"] + result["losses"]
        result["win_rate_percent"] = 100 * result["wins"] / decisive if decisive else None
        result["total"] = graded + result["voids"]
        rows = conn.execute(
            """
            SELECT p.payload, s.settled_at, s.outcome, s.profit_units, s.home_score, s.away_score, s.reason
            FROM published_picks p JOIN published_settlements s USING (pick_id)
            ORDER BY s.settled_at DESC, p.pick_id DESC LIMIT ? OFFSET ?
            """,
            (limit, (page - 1) * limit),
        ).fetchall()
        result["data"] = [
            {**json.loads(row["payload"]), **{key: row[key] for key in row.keys() if key != "payload"}}
            for row in rows
        ]
    return result
