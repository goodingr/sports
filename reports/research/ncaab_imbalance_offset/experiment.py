"""Fit the preregistered NCAA basketball totals offset from bounded local evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import sqlite3
from collections import Counter, defaultdict
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np

from scripts.backup_evidence import read_raw_evidence
from src.data.team_mappings import normalize_team_code
from src.models.prediction_quality import no_vig_pair
from src.predict.paper_settlement import _utc_timestamp

PROTOCOL = Path(__file__).with_name("protocol.json")
EVENT_ID = re.compile(r"[a-zA-Z0-9]{32}")
SCORE_FILE = re.compile(r"scores_(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2}Z)\.json")
EASTERN = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class Quote:
    event_id: str
    teams: tuple[str, str]
    kickoff: datetime
    fetched: datetime
    line: float
    over: float
    home: float


def _teams(event: dict, league: str) -> tuple[str, str] | None:
    home, away = event.get("home_team"), event.get("away_team")
    if not isinstance(home, str) or not home.strip() or not isinstance(away, str) or not away.strip():
        return None
    codes = normalize_team_code(league, home), normalize_team_code(league, away)
    return codes if all(codes) and codes[0] != codes[1] else None


def _market(book: dict, key: str, names: tuple[str, str], fetched: datetime, max_age: timedelta):
    markets = book.get("markets")
    if not isinstance(markets, list):
        return None
    matches = [m for m in markets if isinstance(m, dict) and m.get("key") == key]
    if len(matches) != 1:
        return None
    market = matches[0]
    updated = _utc_timestamp(market.get("last_update"))
    if updated is None or not timedelta(0) <= fetched - updated <= max_age:
        return None
    outcomes = market.get("outcomes")
    if not isinstance(outcomes, list) or len(outcomes) != 2 or not all(
        isinstance(o, dict) and isinstance(o.get("name"), str) for o in outcomes
    ):
        return None
    paired = {o["name"].strip().casefold(): o for o in outcomes}
    if paired.keys() != set(names):
        return None
    first, second = (paired[name] for name in names)
    prices = first.get("price"), second.get("price")
    if not all(type(price) in (int, float) and math.isfinite(price) and abs(price) >= 100 for price in prices):
        return None
    probability, _ = no_vig_pair(*prices)
    if not 0 < probability < 1:
        return None
    point = first.get("point") if key == "totals" else None
    if key == "totals" and (
        not isinstance(point, (int, float)) or isinstance(point, bool) or not math.isfinite(point) or point <= 0
        or point != second.get("point") or not (point * 2.0).is_integer()
    ):
        return None
    return probability, point


def _paired_quote(event: dict, protocol: dict, fetched: datetime, kickoff: datetime, teams: tuple[str, str]):
    bookmakers = event.get("bookmakers")
    if not isinstance(bookmakers, list):
        return None
    books = [b for b in bookmakers if isinstance(b, dict) and b.get("key") == protocol["book"]]
    if len(books) != 1:
        return None
    max_age = timedelta(minutes=protocol["selection"]["maximum_market_update_age_minutes_inclusive"])
    names = tuple(event[key].strip().casefold() for key in ("home_team", "away_team"))
    moneyline = _market(books[0], "h2h", (names[0], names[1]), fetched, max_age)
    totals = _market(books[0], "totals", ("over", "under"), fetched, max_age)
    if moneyline is None or totals is None:
        return None
    return Quote(event["id"], teams, kickoff, fetched, totals[1], totals[0], moneyline[0])


def _quotes(
    conn: sqlite3.Connection, data_dir: Path, protocol: dict, start: datetime, end: datetime,
    *, first_capture: datetime | None = None,
):
    minimum, maximum = (timedelta(hours=value) for value in protocol["selection"]["decision_lead_hours_inclusive"])
    capture_start = first_capture if first_capture is not None else start - maximum
    tolerance = timedelta(minutes=protocol["selection"]["maximum_observed_kickoff_drift_minutes_inclusive"])
    expected_url = f"https://api.the-odds-api.com/v4/sports/{protocol['sport_key']}/odds"
    captures = conn.execute(
        """
        SELECT o.snapshot_id, o.fetched_at_utc, o.raw_path
        FROM odds_snapshots o JOIN sports s USING(sport_id)
        WHERE s.league = ? AND o.source = ? AND o.raw_path IS NOT NULL
          AND julianday(o.fetched_at_utc) BETWEEN julianday(?) AND julianday(?)
        """,
        (protocol["league"], protocol["source"], capture_start.isoformat(), end.isoformat()),
    )
    ordered = [
        (fetched, snapshot_id, path)
        for snapshot_id, stamp, path in captures
        if (fetched := _utc_timestamp(stamp)) is not None and capture_start <= fetched < end
    ]
    diagnostics: Counter[str] = Counter()
    receipts: set[tuple[datetime, bytes]] = set()
    selected: dict[str, Quote] = {}
    histories: dict[str, set[tuple[tuple[str, str], datetime]]] = defaultdict(set)
    for fetched, _, path in sorted(ordered):
        try:
            raw = read_raw_evidence(path, data_dir)
            payload = json.loads(raw)
            if (
                not isinstance(payload, dict) or payload.get("error")
                or payload.get("source", protocol["source"]) != protocol["source"]
                or _utc_timestamp(payload.get("fetched_at")) != fetched
                or payload["request"]["url"] != expected_url
                or not isinstance(payload.get("results"), list)
            ):
                raise ValueError
        except (KeyError, OSError, TypeError, ValueError):
            diagnostics["unverified_odds_registrations"] += 1
            continue
        identity = fetched, hashlib.sha256(raw).digest()
        if identity in receipts:
            diagnostics["duplicate_odds_registrations"] += 1
            continue
        receipts.add(identity)
        diagnostics["verified_odds_receipts"] += 1
        ids = Counter(e.get("id") for e in payload["results"] if isinstance(e, dict) and isinstance(e.get("id"), str))
        for event in payload["results"]:
            if not isinstance(event, dict) or event.get("sport_key") != protocol["sport_key"]:
                continue
            event_id = event.get("id")
            if not isinstance(event_id, str) or not EVENT_ID.fullmatch(event_id) or ids[event_id] != 1:
                continue
            kickoff = _utc_timestamp(event.get("commence_time"))
            teams = _teams(event, protocol["league"])
            if kickoff is None or teams is None:
                continue
            histories[event_id].add((teams, kickoff))
            if event_id in selected or not (start <= kickoff < end and minimum <= kickoff - fetched <= maximum):
                continue
            quote = _paired_quote(event, protocol, fetched, kickoff, teams)
            if quote is not None:
                selected[event_id] = quote

    ambiguous: set[str] = set()
    matchups = defaultdict(list)
    for quote in selected.values():
        history = histories[quote.event_id]
        starts = [kickoff for _, kickoff in history]
        if len({teams for teams, _ in history}) != 1 or max(starts) - min(starts) > tolerance:
            ambiguous.add(quote.event_id)
        matchups[quote.teams].append(quote)
    for games in matchups.values():
        games.sort(key=lambda quote: quote.kickoff)
        for first, second in zip(games, games[1:]):
            if second.kickoff - first.kickoff <= tolerance:
                ambiguous.update((first.event_id, second.event_id))
    diagnostics["ambiguous_quote_fixtures"] = len(ambiguous)
    return {key: quote for key, quote in selected.items() if key not in ambiguous}, diagnostics


def _score_timestamp(path: Path) -> datetime | None:
    match = SCORE_FILE.fullmatch(path.name)
    if match is None:
        return None
    try:
        return datetime.strptime(match[1], "%Y-%m-%dT%H-%M-%SZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _score_evidence(score_dir: Path, quotes: dict[str, Quote], protocol: dict, start: datetime, end: datetime):
    tolerance = timedelta(minutes=protocol["selection"]["maximum_observed_kickoff_drift_minutes_inclusive"])
    grades: dict[str, set[tuple[int, int]]] = defaultdict(set)
    reopened: set[str] = set()
    diagnostics: Counter[str] = Counter()
    first_receipt = None
    for path in sorted(score_dir.glob("scores_*.json")):
        observed = _score_timestamp(path)
        if observed is None or not start <= observed < end:
            continue
        try:
            payload = json.loads(path.read_bytes())
            if not isinstance(payload, list):
                raise ValueError
        except (OSError, ValueError):
            diagnostics["unverified_score_receipts"] += 1
            continue
        diagnostics["development_score_receipts"] += 1
        if first_receipt is None:
            first_receipt = observed
        ids = Counter(e.get("id") for e in payload if isinstance(e, dict) and isinstance(e.get("id"), str))
        for event in payload:
            if not isinstance(event, dict) or not isinstance(event.get("id"), str):
                continue
            event_id = event["id"]
            quote = quotes.get(event_id)
            if quote is None or ids[event_id] != 1 or event.get("sport_key") != protocol["sport_key"]:
                continue
            kickoff = _utc_timestamp(event.get("commence_time"))
            if (
                kickoff is None or max(kickoff, quote.kickoff) >= observed
                or abs(kickoff - quote.kickoff) > tolerance
                or _teams(event, protocol["league"]) != quote.teams
            ):
                continue
            if event.get("completed") is not True:
                if event.get("completed") is False and event_id in grades:
                    reopened.add(event_id)
                continue
            scores = event.get("scores")
            if not isinstance(scores, list) or len(scores) != 2 or not all(isinstance(row, dict) for row in scores):
                continue
            by_team = {row.get("name"): row.get("score") for row in scores if isinstance(row.get("name"), str)}
            if by_team.keys() != {event["home_team"], event["away_team"]}:
                continue
            try:
                home, away = (int(by_team[event[key]]) for key in ("home_team", "away_team"))
            except (TypeError, ValueError, OverflowError):
                continue
            if (
                not all(re.fullmatch(r"[0-9]+", str(score)) for score in by_team.values())
                or home == away or not (0 <= home <= 200 and 0 <= away <= 200)
            ):
                continue
            grades[event_id].add((home, away))
    unresolved = reopened | {key for key, pairs in grades.items() if len(pairs) > 1}
    diagnostics["ambiguous_score_fixtures"] = len(unresolved)
    diagnostics["missing_score_fixtures"] = len(quotes) - len(grades)
    first_receipt_fields = {
        "first_development_score_receipt_at_utc": first_receipt.isoformat() if first_receipt else None,
        "missing_score_fixtures_before_first_receipt": sum(
            key not in grades and (first_receipt is None or quote.kickoff < first_receipt)
            for key, quote in quotes.items()
        ),
    }
    totals = {event_id: sum(next(iter(pairs))) for event_id, pairs in grades.items() if event_id not in unresolved}
    return totals, {**diagnostics, **first_receipt_fields}


def _fit_beta(offset, imbalance, labels, lower: float, upper: float, l2: float) -> float:
    def gradient(beta: float) -> float:
        forecast = np.exp(-np.logaddexp(0, -(offset + beta * imbalance)))
        return float((forecast - labels) @ imbalance + l2 * beta)

    if gradient(upper) <= 0:
        return upper
    if gradient(lower) >= 0:
        return lower
    for _ in range(60):
        mid = (lower + upper) / 2
        if gradient(mid) > 0:
            upper = mid
        else:
            lower = mid
    return (lower + upper) / 2


def fit_development(db: Path, score_dir: Path, protocol: dict) -> dict:
    development = protocol["development"]
    start = datetime.fromisoformat(development["start_utc"])
    end = datetime.fromisoformat(development["end_exclusive_utc"])
    lag = timedelta(hours=development["label_availability_lag_hours"])
    with closing(sqlite3.connect(db.as_uri() + "?mode=ro", uri=True)) as conn:
        quotes, quote_counts = _quotes(conn, db.parent, protocol, start, end)
    settled_window = {key: quote for key, quote in quotes.items() if quote.kickoff + lag < end}
    grades, grade_counts = _score_evidence(score_dir, settled_window, protocol, start, end)
    nonpush = [
        (quote, grades[key] > quote.line)
        for key, quote in settled_window.items() if key in grades and grades[key] != quote.line
    ]
    if not nonpush:
        raise ValueError("No eligible development outcomes with exact score evidence")
    offset = np.array([math.log(q.over / (1 - q.over)) for q, _ in nonpush])
    imbalance = np.array([abs(math.log(q.home / (1 - q.home))) for q, _ in nonpush])
    labels = np.array([float(y) for _, y in nonpush])
    lower, upper = protocol["model"]["beta_bounds_inclusive"]
    beta = _fit_beta(offset, imbalance, labels, lower, upper, protocol["model"]["l2"])
    cohort = [
        (key, quote.teams, quote.kickoff.isoformat(), quote.fetched.isoformat(),
         quote.line, quote.over, quote.home, grades.get(key))
        for key, quote in sorted(settled_window.items())
    ]
    return {
        "status": "zero_coefficient_holdout_closed" if beta == 0 else "development_fit_nonzero",
        "protocol_id": protocol["protocol_id"],
        "development_start_utc": start.isoformat(),
        "development_end_exclusive_utc": end.isoformat(),
        "paired_quote_fixtures": len(quotes),
        "purged_at_development_cutoff": len(quotes) - len(settled_window),
        "matched_final_fixtures": len(grades),
        "push_fixtures": len(grades) - len(nonpush),
        "development_nonpush_fixtures": len(nonpush),
        "development_et_dates": len({q.kickoff.astimezone(EASTERN).date() for q, _ in nonpush}),
        "development_cohort_sha256": hashlib.sha256(json.dumps(cohort).encode()).hexdigest(),
        **quote_counts,
        **grade_counts,
        "beta": beta,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--score-dir", type=Path)
    args = parser.parse_args()
    db = args.db.resolve()
    protocol = json.loads(PROTOCOL.read_text(encoding="utf-8"))
    try:
        result = fit_development(db, args.score_dir or db.parent / "raw" / "scores" / protocol["league"], protocol)
    except (OSError, ValueError, sqlite3.Error) as exc:
        parser.exit(1, f"{exc}\n")
    for field, path in [("protocol_sha256", PROTOCOL), ("experiment_sha256", Path(__file__))]:
        result[field] = hashlib.sha256(path.read_text(encoding="utf-8").encode()).hexdigest()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
