"""Verify the economics of grade-linked same-line NBA paper closes."""

from __future__ import annotations

import math
import sqlite3
from collections import Counter
from datetime import datetime

from src.models.prediction_quality import american_to_decimal, no_vig_pair
from src.predict.paper_settlement import (
    _START_TOLERANCE,
    _decision_rows,
    _material_state,
    _minimum_lead,
    _normalized_h2h_prices,
    _quote_source_start,
    _utc_timestamp,
    _valid_american_odds,
    _valid_total_line,
    expected_grade,
)
from src.predict.paper_settlement_history import grades_as_of

_ENTRY_FIELDS = {"book", "snapshot_id", "odds_fetched_at", "opposite_price"}


def _verified_prices(
    conn: sqlite3.Connection, decision: sqlite3.Row,
    snapshot_id: str, fetched_at: datetime, line: float, *, entry: bool = False,
) -> tuple[dict[str, float], datetime] | None:
    rows = [
        row for row in conn.execute(
            """
            SELECT o.snapshot_id, lower(o.outcome) AS outcome, o.price_american, o.line,
                   s.fetched_at_utc
            FROM odds o JOIN odds_snapshots s USING (snapshot_id)
            JOIN books b USING (book_id)
            WHERE o.game_id = ? AND o.book_id = ? AND o.market = 'totals'
              AND lower(trim(b.name)) = 'draftkings' AND s.sport_id = ? AND s.source = ?
            """,
            (decision["game_id"], decision["book_id"], decision["sport_id"], decision["odds_source"]),
        )
        if _utc_timestamp(row["fetched_at_utc"]) == fetched_at
    ]
    if len(rows) != 2 or any(
        row["snapshot_id"] != snapshot_id or row["line"] != line
        or not _valid_american_odds(row["price_american"]) for row in rows
    ):
        return None
    prices = {row["outcome"]: row["price_american"] for row in rows}
    if prices.keys() != {"over", "under"}:
        return None
    quote = {
        "snapshot_id": snapshot_id,
        "game_id": decision["game_id"],
        "sport_id": decision["sport_id"],
        "book_id": decision["book_id"],
        "source": decision["odds_source"],
        "line": line,
        "over_price": prices["over"],
        "under_price": prices["under"],
    }
    moneylines = _normalized_h2h_prices(conn, quote) if entry else None
    if entry and moneylines is None:
        return None
    raw_start = _quote_source_start(
        conn, quote, fetched_at, require_original=True, moneylines=moneylines
    )
    if raw_start is None:
        return None
    return prices, raw_start


def _verified_comparison(
    conn: sqlite3.Connection, decision: sqlite3.Row, grade: sqlite3.Row, observed_at: datetime,
) -> tuple[float, float, float] | None:
    if (
        not _ENTRY_FIELDS.issubset(decision.keys())
        or str(decision["book"]).lower() != "draftkings"
        or decision["side"] not in {"over", "under"}
        or not _valid_total_line(decision["line"])
        or not _valid_total_line(grade["close_line"])
    ):
        return None
    entry_at = _utc_timestamp(decision["odds_fetched_at"])
    close_at = _utc_timestamp(grade["close_fetched_at"])
    captured_at = _utc_timestamp(decision["captured_at"])
    start = _utc_timestamp(decision["start_time_utc"])
    minimum_lead = _minimum_lead(decision)
    if (
        entry_at is None or close_at is None or captured_at is None or start is None
        or minimum_lead is None or not entry_at <= captured_at < close_at < start
        or _material_state(grade) != _material_state(expected_grade(conn, decision, observed_at))
    ):
        return None
    entry = _verified_prices(
        conn, decision, decision["snapshot_id"], entry_at, decision["line"], entry=True
    )
    close = _verified_prices(conn, decision, grade["close_snapshot_id"], close_at, grade["close_line"])
    if entry is None or close is None:
        return None
    entry_prices, provider_start = entry
    close_prices, _ = close
    side = decision["side"]
    opposite = "under" if side == "over" else "over"
    if (
        abs(provider_start - start) > _START_TOLERANCE
        or min(provider_start, start) - captured_at < minimum_lead
        or entry_prices[side] != decision["price"]
        or entry_prices[opposite] != decision["opposite_price"]
        or close_prices[side] != grade["close_price"]
    ):
        return None
    entry_decimal = american_to_decimal(entry_prices[side])
    close_decimal = american_to_decimal(close_prices[side])
    if entry_decimal is None or close_decimal is None:
        return None
    fair, _ = no_vig_pair(close_prices[side], close_prices[opposite])
    return entry_decimal, close_decimal, fair


def closing_price_diagnostics(
    conn: sqlite3.Connection, trial_id: str, review_at: datetime,
    evidence_cutoff: datetime, observed_at: datetime,
) -> dict[str, int | float | None]:
    """Summarize price comparisons without changing the frozen grade or review."""
    grades = grades_as_of(conn, trial_id, evidence_cutoff)
    current = grades_as_of(conn, trial_id, observed_at) if observed_at > evidence_cutoff else grades
    counts: Counter[str] = Counter()
    price_clv: list[float] = []
    closing_ev: list[float] = []
    for decision in _decision_rows(conn, trial_id) if grades else []:
        captured_at = _utc_timestamp(decision["captured_at"])
        grade = grades.get(decision["game_id"])
        if (
            captured_at is None or captured_at >= review_at or captured_at > evidence_cutoff
            or grade is None or grade["outcome"] not in {"win", "loss", "push"}
        ):
            continue
        counts["graded"] += 1
        if grade["close_snapshot_id"] is None:
            counts["missing"] += 1
            continue
        latest = current.get(decision["game_id"])
        comparison = (
            _verified_comparison(conn, decision, grade, observed_at)
            if latest is not None and latest["revision_id"] == grade["revision_id"] else None
        )
        if comparison is None:
            counts["unverified"] += 1
            continue
        if decision["line"] != grade["close_line"]:
            counts["changed_line"] += 1
            continue
        entry_decimal, close_decimal, fair = comparison
        price_clv.append(entry_decimal / close_decimal - 1)
        closing_ev.append(entry_decimal * fair - 1)
        counts["integer_line"] += int(float(decision["line"]).is_integer())
        counts["push"] += int(grade["outcome"] == "push")

    matched = len(price_clv)
    graded = counts["graded"]
    return {
        "graded_decisions": graded,
        "missing_grade_linked_closes": counts["missing"],
        "unverified_grade_linked_closes": counts["unverified"],
        "verified_changed_line_closes": counts["changed_line"],
        "verified_same_line_closes": matched,
        "same_line_coverage": matched / graded if graded else None,
        "same_line_integer_totals": counts["integer_line"],
        "same_line_observed_pushes": counts["push"],
        "mean_same_line_price_clv": math.fsum(price_clv) / matched if matched else None,
        "positive_same_line_price_clv_share": (
            sum(value > 0 for value in price_clv) / matched if matched else None
        ),
        "mean_same_line_no_vig_close_ev_conditional_non_push": (
            math.fsum(closing_ev) / matched if matched else None
        ),
        "positive_same_line_no_vig_close_ev_share": (
            sum(value > 0 for value in closing_ev) / matched if matched else None
        ),
    }
