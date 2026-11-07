"""Check the daily NBA paper capture using the local warehouse and raw receipts."""

from __future__ import annotations

import argparse
import hashlib
import json
import sqlite3
import zipfile
import zlib
from collections import defaultdict
from collections.abc import Iterator
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

from scripts.backup_evidence import EVIDENCE_ZIP, _zip_manifest
from src.data.config import RAW_DATA_DIR
from src.db.core import resolve_db_path
from src.predict.paper_coverage import _FULLY_EVALUATED
from src.predict.paper_ledger import DEFAULT_RULES, PaperTrial, load_trial
from src.predict.paper_schedule import _START_TOLERANCE
from src.predict.paper_settlement import _utc_timestamp

_EASTERN = ZoneInfo("America/New_York")
_EXPECTED_TIME = time(18, 4)
_GRACE_MINUTES = 30
_NBA_ODDS = Path("odds") / "basketball_nba"
_NBA_RAW_DIR = RAW_DATA_DIR / _NBA_ODDS
_ARCHIVED_NBA_RAW_DIR = PurePosixPath("raw") / _NBA_ODDS.as_posix()
_MAX_RECEIPT_BYTES = 16 * 1024**2


@dataclass(frozen=True)
class _Receipt:
    path: Path
    fetched_at: datetime
    status: str
    eligible_events: frozenset[str] = frozenset()


def _latest_elapsed_tick(
    now: datetime, registered: datetime, review: datetime, clock: time, grace: timedelta
) -> datetime | None:
    elapsed = min(now.astimezone(timezone.utc) - grace, review - timedelta(microseconds=1))
    day = elapsed.astimezone(_EASTERN).date()
    tick = datetime.combine(day, clock, _EASTERN).astimezone(timezone.utc)
    if tick > elapsed:
        tick = datetime.combine(day - timedelta(days=1), clock, _EASTERN).astimezone(timezone.utc)
    return tick if tick >= registered else None


def _is_daily_nba_request(payload: dict, trial: PaperTrial) -> bool:
    request = payload.get("request")
    if not isinstance(request, dict) or not isinstance(request.get("url"), str):
        return False
    try:
        url = urlparse(request["url"])
        host = url.hostname
    except ValueError:
        return False
    params = request.get("params")
    return (
        isinstance(params, dict)
        and url.scheme == "https"
        and host == "api.the-odds-api.com"
        and url.path.rstrip("/") == "/v4/sports/basketball_nba/odds"
        and payload.get("source", trial.odds_source) == trial.odds_source
        and set(str(params.get("markets", "")).split(",")) == {"h2h", "totals"}
        and "us" in str(params.get("regions", "")).split(",")
        and params.get("oddsFormat") == "american"
        and not params.get("eventIds")
        and not params.get("bookmakers")
    )


def _receipt_inputs(
    raw_dir: Path, data_dir: Path | None, start: datetime, end: datetime
) -> Iterator[tuple[Path, bytes | None]]:
    prefixes = tuple(
        f"odds_{start.date() + timedelta(days=offset)}T"
        for offset in range((end.date() - start.date()).days + 1)
    )
    local_names = set()
    for prefix in prefixes:
        for path in raw_dir.glob(f"{prefix}*.json"):
            local_names.add(path.name)
            try:
                contents = path.read_bytes()
            except OSError:
                contents = None
            yield path, contents
    if data_dir is None:
        return
    bundle = data_dir / EVIDENCE_ZIP
    if not bundle.is_file():
        return
    try:
        with zipfile.ZipFile(bundle) as archive:
            manifest = _zip_manifest(archive)
            for name, entry in manifest["files"].items():
                member_path = PurePosixPath(name)
                if (
                    member_path.parent != _ARCHIVED_NBA_RAW_DIR
                    or not member_path.name.startswith(prefixes)
                    or member_path.suffix != ".json"
                    or member_path.name in local_names
                ):
                    continue
                contents = None
                if entry["size_bytes"] <= _MAX_RECEIPT_BYTES:
                    try:
                        candidate = archive.read(name)
                        if hashlib.sha256(candidate).hexdigest() == entry.get("sha256"):
                            contents = candidate
                    except (zipfile.BadZipFile, zlib.error):
                        pass
                yield Path(name), contents
    except (zipfile.BadZipFile, KeyError, json.JSONDecodeError) as error:
        raise ValueError("invalid restored raw evidence ZIP") from error


def _read_receipt(
    raw_dir: Path, trial: PaperTrial, start: datetime, end: datetime, data_dir: Path | None
) -> _Receipt | None:
    daily = []
    invalid = []
    minimum_lead = timedelta(minutes=trial.min_minutes_before_start)
    maximum_lead = timedelta(hours=trial.max_hours_before_start)
    try:
        # Daily receipts live at this level. Supplemental archives are in subdirectories.
        for path, contents in _receipt_inputs(raw_dir, data_dir, start, end):
            try:
                named_at = datetime.strptime(path.name[5:25], "%Y-%m-%dT%H-%M-%SZ").replace(
                    tzinfo=timezone.utc
                )
            except ValueError:
                continue
            if not start.date() <= named_at.date() <= end.date():
                continue
            try:
                payload = json.loads(contents.decode("utf-8-sig")) if contents is not None else None
            except ValueError:
                payload = None
            fetched = _utc_timestamp(payload.get("fetched_at")) if isinstance(payload, dict) else None
            if not isinstance(payload, dict) or fetched is None:
                if start <= named_at <= end:
                    invalid.append(_Receipt(path, named_at, "receipt_invalid"))
                continue
            if not start <= fetched <= end:
                continue
            if not _is_daily_nba_request(payload, trial):
                invalid.append(_Receipt(path, fetched, "receipt_invalid"))
                continue
            if payload.get("error"):
                daily.append(_Receipt(path, fetched, "receipt_error"))
                continue
            events = payload.get("results")
            if not isinstance(events, list):
                daily.append(_Receipt(path, fetched, "receipt_invalid"))
                continue
            all_events = set()
            eligible = set()
            for event in events:
                if not isinstance(event, dict):
                    break
                event_id = event.get("id")
                tip = _utc_timestamp(event.get("commence_time"))
                if (
                    not isinstance(event_id, str) or not event_id or event_id in all_events
                    or event.get("sport_key") != "basketball_nba" or tip is None
                ):
                    break
                all_events.add(event_id)
                if minimum_lead <= tip - fetched <= maximum_lead:
                    eligible.add(event_id)
            else:
                daily.append(_Receipt(path, fetched, "valid", frozenset(eligible)))
                continue
            daily.append(_Receipt(path, fetched, "receipt_invalid"))
    except (OSError, ValueError):
        if not daily and not invalid:
            raise
    return max(daily or invalid, key=lambda item: (item.fetched_at, item.path.name), default=None)


def _matches_receipt_path(stored_path: object, selected: Path) -> bool:
    if not isinstance(stored_path, str):
        return False
    original = stored_path.replace("\\", "/")
    if ".." in PurePosixPath(original).parts:
        return False
    canonical = (_ARCHIVED_NBA_RAW_DIR / selected.name).as_posix()
    return (
        original in {selected.as_posix(), selected.absolute().as_posix(), canonical}
        or original.endswith(f"/{canonical}")
    )


def _known_slate_gaps(
    conn: sqlite3.Connection, trial: PaperTrial, registered: datetime, receipt: _Receipt
) -> tuple[int | None, list[str], list[str]]:
    """Compare independently observed schedules with the eligible receipt IDs."""
    if conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'paper_fixture_schedule'"
    ).fetchone() is None:
        return None, [], []

    histories: dict[str, list[tuple[datetime, datetime, int, int, str | None]]] = defaultdict(list)
    for row in conn.execute(
        """
        SELECT h.game_id, h.observed_at, h.start_time_utc, h.home_team_id, h.away_team_id,
               CASE WHEN sport.league = ? AND g.home_team_id = h.home_team_id
                        AND g.away_team_id = h.away_team_id
                        AND NOT EXISTS (
                            SELECT 1 FROM games other
                            WHERE other.sport_id = g.sport_id AND other.odds_api_id = g.odds_api_id
                              AND other.game_id <> g.game_id
                        )
                    THEN NULLIF(g.odds_api_id, '') END AS event_id
        FROM paper_fixture_schedule h
        LEFT JOIN games g ON g.game_id = h.game_id
        LEFT JOIN sports sport ON sport.sport_id = g.sport_id
        WHERE h.trial_id = ?
        """,
        (trial.league, trial.trial_id),
    ):
        seen = _utc_timestamp(row["observed_at"])
        tip = _utc_timestamp(row["start_time_utc"])
        if seen is not None and tip is not None and registered <= seen <= receipt.fetched_at:
            histories[row["game_id"]].append(
                (seen, tip, row["home_team_id"], row["away_team_id"], row["event_id"])
            )

    minimum_lead = timedelta(minutes=trial.min_minutes_before_start)
    maximum_lead = timedelta(hours=trial.max_hours_before_start)
    eligible = 0
    missing = []
    unresolved = []
    for game_id, versions in sorted(histories.items()):
        latest_seen = max(version[0] for version in versions)
        latest = [version for version in versions if version[0] == latest_seen]
        if not any(minimum_lead <= tip - receipt.fetched_at <= maximum_lead for _, tip, *_ in latest):
            continue
        eligible += 1
        identities = {(home, away) for _, _, home, away, _ in versions}
        starts = [tip for _, tip, *_ in versions]
        event_id = latest[0][4]
        if len(latest) != 1 or len(identities) != 1 or max(starts) - min(starts) > _START_TOLERANCE or not event_id:
            unresolved.append(game_id)
        elif event_id not in receipt.eligible_events:
            missing.append(game_id)
    return eligible, missing, unresolved


def assess_capture_health(
    conn: sqlite3.Connection,
    trial: PaperTrial,
    raw_dir: Path,
    now: datetime,
    *,
    expected_time: time = _EXPECTED_TIME,
    grace_minutes: int = _GRACE_MINUTES,
    data_dir: Path | None = None,
) -> dict[str, Any]:
    """Return the status of the latest daily tick whose grace period elapsed."""
    if now.utcoffset() is None or grace_minutes <= 0 or expected_time.tzinfo is not None:
        raise ValueError("Use an aware observation time, a local daily clock, and positive grace")
    base: dict[str, Any] = {"trial_id": trial.trial_id}
    registered_row = conn.execute(
        "SELECT registered_at, specification FROM paper_trials WHERE trial_id = ?", (trial.trial_id,)
    ).fetchone()
    if registered_row is None:
        return {**base, "status": "trial_unregistered"}
    registered = _utc_timestamp(registered_row["registered_at"])
    review = _utc_timestamp(trial.review_at)
    if registered is None or review is None or registered_row["specification"] != trial.specification:
        return {**base, "status": "trial_mismatch"}
    grace = timedelta(minutes=grace_minutes)
    tick = _latest_elapsed_tick(now, registered, review, expected_time, grace)
    if tick is None:
        return {**base, "status": "waiting"}
    end = min(tick + grace, review - timedelta(microseconds=1))
    base.update(expected_at=tick.isoformat(), window_ends_at=end.isoformat())
    try:
        receipt = _read_receipt(raw_dir, trial, tick, end, data_dir)
    except (OSError, ValueError):
        return {**base, "status": "receipt_invalid"}
    if receipt is None:
        return {**base, "status": "receipt_missing"}
    base.update(receipt_at=receipt.fetched_at.isoformat(), receipt_file=receipt.path.name)
    if receipt.status != "valid":
        return {**base, "status": receipt.status}
    known_eligible, known_missing, known_unresolved = _known_slate_gaps(
        conn, trial, registered, receipt
    )
    base.update(
        known_eligible_fixtures=known_eligible,
        known_missing_fixture_ids=known_missing,
        known_unresolved_fixture_ids=known_unresolved,
    )
    known_gap = bool(known_missing or known_unresolved)

    runs = []
    for row in conn.execute(
        "SELECT run_id, observed_at, status FROM paper_capture_runs WHERE trial_id = ?",
        (trial.trial_id,),
    ):
        observed = _utc_timestamp(row["observed_at"])
        if observed is not None and receipt.fetched_at <= observed <= end:
            runs.append((observed, row["run_id"], row["status"]))
    if not runs:
        return {**base, "status": "capture_missing"}
    runs.sort(reverse=True)
    latest = runs[0]

    evaluated_by_run: dict[str, dict[str, str]] = defaultdict(dict)
    markers = ", ".join("?" for _ in runs)
    for row in conn.execute(
        f"""
        SELECT o.run_id, g.odds_api_id, o.outcome, s.source, s.fetched_at_utc, s.raw_path
        FROM paper_fixture_observations o
        JOIN games g ON g.game_id = o.game_id
        JOIN sports sport ON sport.sport_id = g.sport_id AND sport.league = ?
        LEFT JOIN odds_snapshots s ON s.snapshot_id = o.snapshot_id AND s.sport_id = g.sport_id
        WHERE o.run_id IN ({markers})
          AND NOT EXISTS (
              SELECT 1 FROM games other
              WHERE other.sport_id = g.sport_id AND other.odds_api_id = g.odds_api_id
                AND other.game_id <> g.game_id
          )
        """,
        (trial.league, *(run_id for _, run_id, _ in runs)),
    ):
        if (
            row["odds_api_id"] in receipt.eligible_events
            and row["outcome"] in _FULLY_EVALUATED
            and row["source"] == trial.odds_source
            and _utc_timestamp(row["fetched_at_utc"]) == receipt.fetched_at
            and _matches_receipt_path(row["raw_path"], receipt.path)
        ):
            evaluated_by_run[row["run_id"]][row["odds_api_id"]] = row["outcome"]

    selected = latest
    if receipt.eligible_events and not known_gap:
        # Supplemental or manual captures can run during the same grace window.
        selected = next(
            (
                run
                for run in runs
                if run[2] == "evaluated" and evaluated_by_run[run[1]].keys() == receipt.eligible_events
            ),
            latest,
        )
    captured, run_id, status = selected
    base.update(capture_at=captured.isoformat(), eligible_fixtures=len(receipt.eligible_events))
    if selected != latest:
        base.update(latest_attempt_at=latest[0].isoformat(), latest_attempt_status=latest[2])
    if status == "failed":
        return {**base, "status": "capture_failed"}
    if not receipt.eligible_events and not known_gap:
        return {**base, "status": "empty_slate_unverified"}

    evaluated = evaluated_by_run[run_id]
    base.update(
        evaluated_fixtures=len(evaluated),
        below_edge_fixtures=sum(outcome == "below_edge_threshold" for outcome in evaluated.values()),
    )
    healthy = not known_gap and status == "evaluated" and evaluated.keys() == receipt.eligible_events
    return {**base, "status": "healthy" if healthy else "capture_incomplete"}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path)
    parser.add_argument("--raw-dir", type=Path)
    parser.add_argument("--rules", type=Path, default=DEFAULT_RULES)
    parser.add_argument("--expected-time", type=time.fromisoformat, default=_EXPECTED_TIME)
    parser.add_argument("--grace-minutes", type=int, default=_GRACE_MINUTES)
    parser.add_argument("--as-of", type=_utc_timestamp, default=datetime.now(timezone.utc))
    args = parser.parse_args()
    if args.as_of is None or args.grace_minutes <= 0 or args.expected_time.tzinfo is not None:
        parser.error("Use an aware --as-of, a local --expected-time and positive --grace-minutes")
    db_path = resolve_db_path(args.db).resolve()
    raw_dir = args.raw_dir
    if raw_dir is None:
        raw_dir = db_path.parent / "raw" / _NBA_ODDS if args.db is not None else _NBA_RAW_DIR
    try:
        with closing(sqlite3.connect(f"{db_path.as_uri()}?mode=ro", uri=True)) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("BEGIN")
            result = assess_capture_health(
                conn, load_trial(args.rules), raw_dir, args.as_of,
                expected_time=args.expected_time, grace_minutes=args.grace_minutes,
                data_dir=db_path.parent,
            )
    except sqlite3.Error:
        result = {"status": "warehouse_unavailable"}
    print(json.dumps(result))
    exit_code = {"healthy": 0, "waiting": 0, "empty_slate_unverified": 2}.get(result["status"], 1)
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
