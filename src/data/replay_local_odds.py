"""Replay a reviewed local archive plan without changing schedules or results."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from scripts.backup_evidence import read_raw_evidence
from src.data.team_mappings import normalize_team_code
from src.db.loaders import _american_to_decimal, _get_or_create_book, _implied_probability


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.utcoffset() is None:
        raise ValueError("Archive timestamps must include a timezone")
    return parsed.astimezone(timezone.utc)


def _protected_hashes(conn: sqlite3.Connection) -> dict[str, str]:
    hashes = {}
    for table in ("games", "game_results"):
        digest = hashlib.sha256()
        for row in conn.execute(f"SELECT * FROM {table} ORDER BY game_id"):
            digest.update(json.dumps(dict(row), sort_keys=True).encode())
            digest.update(b"\n")
        hashes[table] = digest.hexdigest()
    return hashes


def _validate_fixtures(conn: sqlite3.Connection, fixtures: list[dict]) -> dict[str, dict]:
    verified = {}
    for expected in fixtures:
        rows = conn.execute(
            """
            SELECT g.game_id, g.sport_id, g.start_time_utc, g.status, s.league,
                   h.name AS home, a.name AS away, r.home_score, r.away_score
            FROM games g
            JOIN sports s USING (sport_id)
            JOIN teams h ON h.team_id = g.home_team_id
            JOIN teams a ON a.team_id = g.away_team_id
            JOIN game_results r USING (game_id)
            WHERE s.league = ? AND date(g.start_time_utc) = ? AND g.status = 'final'
              AND r.home_score IS NOT NULL AND r.away_score IS NOT NULL
            """,
            (expected["league"], _utc(expected["start_time_utc"]).date().isoformat()),
        ).fetchall()
        league = expected["league"]
        identity = tuple(normalize_team_code(league, expected[side]) for side in ("home", "away"))
        matches = [
            dict(row) for row in rows
            if tuple(normalize_team_code(league, row[side]) for side in ("home", "away")) == identity
        ]
        if len(matches) != 1 or matches[0] != expected:
            raise ValueError(f"Settled fixture identity changed or is ambiguous: {expected['game_id']}")
        verified[expected["game_id"]] = matches[0]
    return verified


def _validate_raw(snapshot: dict, fixtures: dict[str, dict], data_dir: Path) -> None:
    raw = read_raw_evidence(snapshot["raw_path"], data_dir)
    if hashlib.sha256(raw).hexdigest() != snapshot["raw_sha256"]:
        raise ValueError("Archive file changed after its recovery plan was reviewed")
    data = json.loads(raw.decode("utf-8-sig"))
    captured = _utc(data["fetched_at"])
    source = data.get("source", "the-odds-api")
    if source != snapshot["source"] or captured != _utc(snapshot["fetched_at_utc"]):
        raise ValueError("Archive source or observation time differs from the recovery plan")
    if source != "the-odds-api":
        raise ValueError("This recovery plan accepts only original Odds API captures")
    for quote in snapshot["rows"]:
        game = fixtures[quote["game_id"]]
        if game["sport_id"] != snapshot["sport_id"] or game["league"] != snapshot["league"]:
            raise ValueError("Archive league differs from the verified fixture")
        kickoff = _utc(game["start_time_utc"])
        matches = []
        for event in data["results"]:
            observed_start = _utc(event["commence_time"])
            same_teams = all(
                normalize_team_code(game["league"], event[f"{side}_team"])
                == normalize_team_code(game["league"], game[side])
                for side in ("home", "away")
            )
            if same_teams and observed_start.date() == kickoff.date():
                matches.append((event, observed_start))
        if len(matches) != 1:
            raise ValueError("Archive fixture identity is ambiguous")
        event, observed_start = matches[0]
        if abs((observed_start - kickoff).total_seconds()) > 600 or captured >= min(kickoff, observed_start):
            raise ValueError("Archive quote was not observed before a compatible kickoff")
        values = set()
        for book in event.get("bookmakers", []):
            if (book.get("title") or book.get("key")) != quote["book"]:
                continue
            for market in book.get("markets", []):
                if market.get("key") != quote["market"]:
                    continue
                for outcome in market.get("outcomes", []):
                    side = str(outcome.get("name") or "").strip()
                    if side.lower() == event["home_team"].lower():
                        side = "home"
                    elif side.lower() == event["away_team"].lower():
                        side = "away"
                    if side.lower() == quote["outcome"].lower():
                        line = None if quote["market"] == "h2h" else outcome.get("point")
                        values.add((outcome.get("price"), line))
        price, line = quote["price_american"], quote["line"]
        if values != {(price, line)} or not math.isfinite(price) or abs(price) < 100:
            raise ValueError("Planned price or line does not uniquely match the captured quote")


def replay_plan(
    plan_path: Path,
    *,
    db_path: Path,
    write: bool = False,
    manifest_path: Path | None = None,
) -> dict[str, Any]:
    """Validate a reviewed plan and append missing quote rows atomically."""
    plan_bytes = plan_path.read_bytes()
    plan = json.loads(plan_bytes)
    if write and manifest_path is None:
        raise ValueError("Writing archived quotes requires a recovery manifest path")
    mode = "rw" if write else "ro"
    conn = sqlite3.connect(db_path.resolve().as_uri() + f"?mode={mode}", uri=True, timeout=10)
    conn.row_factory = sqlite3.Row
    try:
        conn.execute("PRAGMA foreign_keys = ON")
        if write:
            conn.execute("BEGIN IMMEDIATE")
        fixtures = _validate_fixtures(conn, plan["fixtures"])
        before = _protected_hashes(conn)
        actions = []
        headers = {}
        for snapshot in plan["snapshots"]:
            _validate_raw(snapshot, fixtures, db_path.resolve().parent)
            snapshot_id = snapshot["snapshot_id"]
            if snapshot_id is None:
                existing = conn.execute(
                    """
                    SELECT snapshot_id FROM odds_snapshots
                    WHERE sport_id = ? AND source = ? AND fetched_at_utc = ? AND raw_path = ?
                    ORDER BY snapshot_id LIMIT 1
                    """,
                    (snapshot["sport_id"], snapshot["source"], snapshot["fetched_at_utc"], snapshot["raw_path"]),
                ).fetchone()
                identity = json.dumps([
                    snapshot["sport_id"], snapshot["source"], snapshot["fetched_at_utc"], snapshot["raw_sha256"]
                ])
                snapshot_id = existing[0] if existing else "replay-" + hashlib.sha256(identity.encode()).hexdigest()[:32]
            current = conn.execute("SELECT * FROM odds_snapshots WHERE snapshot_id = ?", (snapshot_id,)).fetchone()
            if current and (
                current["sport_id"] != snapshot["sport_id"]
                or current["source"] != snapshot["source"]
                or _utc(current["fetched_at_utc"]) != _utc(snapshot["fetched_at_utc"])
            ):
                raise ValueError("Target snapshot identity differs from the recovery plan")
            if not current:
                headers[snapshot_id] = snapshot
            for quote in snapshot["rows"]:
                book = conn.execute("SELECT book_id FROM books WHERE name = ?", (quote["book"],)).fetchone()
                previous = conn.execute(
                    """
                    SELECT * FROM odds WHERE snapshot_id = ? AND game_id = ? AND book_id = ?
                      AND market = ? AND LOWER(outcome) = LOWER(?)
                    """,
                    (snapshot_id, quote["game_id"], book[0] if book else None, quote["market"], quote["outcome"]),
                ).fetchall()
                if previous:
                    if any((row["price_american"], row["line"]) != (quote["price_american"], quote["line"]) for row in previous):
                        raise ValueError("An existing quote conflicts with the planned recovery")
                    continue
                actions.append({**quote, "snapshot_id": snapshot_id, "region": snapshot["region"]})
        result = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "plan_path": str(plan_path.resolve()),
            "plan_sha256": hashlib.sha256(plan_bytes).hexdigest(),
            "db_path": str(db_path.resolve()),
            "write": write,
            "new_snapshots": list(headers),
            "new_quote_rows": len(actions),
            "protected_hashes_before": before,
            "inserted_quotes": actions,
        }
        if write:
            assert manifest_path is not None
            manifest_path.parent.mkdir(parents=True, exist_ok=True)
            with manifest_path.open("x", encoding="utf-8") as output:
                json.dump({**result, "status": "prepared"}, output, indent=2)
            for snapshot_id, snapshot in headers.items():
                conn.execute(
                    "INSERT INTO odds_snapshots (snapshot_id, fetched_at_utc, sport_id, source, raw_path) VALUES (?, ?, ?, ?, ?)",
                    (snapshot_id, snapshot["fetched_at_utc"], snapshot["sport_id"], snapshot["source"], snapshot["raw_path"]),
                )
            for action in actions:
                book_id = _get_or_create_book(conn, action["book"], action["region"])
                price = action["price_american"]
                conn.execute(
                    """
                    INSERT INTO odds (snapshot_id, game_id, book_id, market, outcome,
                                      price_american, price_decimal, implied_prob, line)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (action["snapshot_id"], action["game_id"], book_id, action["market"], action["outcome"],
                     price, _american_to_decimal(price), _implied_probability(price), action["line"]),
                )
                action["book_id"] = book_id
            after = _protected_hashes(conn)
            if before != after:
                raise RuntimeError("Archive replay changed protected schedules or results")
            conn.commit()
            result.update({"status": "committed", "protected_hashes_after": after})
            manifest_path.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        return result
    finally:
        if conn.in_transaction:
            conn.rollback()
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--db-path", type=Path, default=Path("data/betting.db"))
    parser.add_argument("--write", action="store_true")
    parser.add_argument("--manifest", type=Path)
    args = parser.parse_args()
    result = replay_plan(args.plan, db_path=args.db_path, write=args.write, manifest_path=args.manifest)
    print(json.dumps({key: value for key, value in result.items() if key != "inserted_quotes"}, indent=2))


if __name__ == "__main__":
    main()
