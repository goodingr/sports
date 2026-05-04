#!/usr/bin/env bash
#
# Restore drill — verifies the backup chain end-to-end on a clean directory.
# Run this monthly. The whole point of a backup is the restore; if this drill
# stops passing, you have no backup.
#
# Steps:
#   1. Spin up a temp work dir.
#   2. Pull the latest snapshot from S3 into that dir.
#   3. Run `PRAGMA integrity_check` on the restored DB.
#   4. Verify required tables exist with the expected columns.
#   5. Require non-trivial row counts on warehouse-critical tables.
#   6. Verify model artefact files were also restored.
#   7. Clean up.
#
# Run from inside the worker container (it has aws + python + the codebase):
#   docker compose exec worker /app/scripts/restore_drill.sh
#
# Optional first arg: explicit snapshot date (YYYY-MM-DD). Defaults to the
# `latest.txt` pointer maintained by scripts/backup_to_s3.sh.
#
# Exit codes:
#   0  drill passed
#   1  any sanity check failed (treat as a paging incident)
#   2  restore step failed before checks could run

set -uo pipefail

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

DATE=${1:-}

ARGS=(
	--target-data "$WORK/betting.db"
	--target-models "$WORK/models"
	--force
)
if [[ -n "$DATE" ]]; then
	ARGS+=(--date "$DATE")
fi

echo "[drill] running restore into $WORK"
if ! /app/scripts/restore_from_s3.sh "${ARGS[@]}"; then
	echo "[drill] FAIL: restore_from_s3.sh exited non-zero" >&2
	exit 2
fi

if [[ ! -s "$WORK/betting.db" ]]; then
	echo "[drill] FAIL: restored DB is missing or empty" >&2
	exit 2
fi

echo "[drill] sanity-checking restored DB"
export DRILL_DB="$WORK/betting.db"
export DRILL_MODELS="$WORK/models"

if ! python - <<'PY'
import os
import sqlite3
import sys
from pathlib import Path

db = os.environ["DRILL_DB"]
models_dir = Path(os.environ["DRILL_MODELS"])
failures: list[str] = []
warnings: list[str] = []

con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
con.row_factory = sqlite3.Row
cur = con.cursor()

# 1. Integrity check. SQLite reports "ok" when every page parses cleanly.
integrity = cur.execute("PRAGMA integrity_check").fetchone()
integrity_value = integrity[0] if integrity else "no result"
print(f"[drill] integrity_check: {integrity_value}")
if integrity_value != "ok":
    failures.append(f"integrity_check returned {integrity_value!r}")

# 2. Required tables present.
required_tables = {
    "sports", "teams", "games", "books",
    "odds_snapshots", "odds", "predictions", "models",
}
present_tables = {
    row[0]
    for row in cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
}
missing_tables = sorted(required_tables - present_tables)
print(f"[drill] tables present: {len(present_tables)} (required: {len(required_tables)})")
if missing_tables:
    failures.append(f"missing required tables: {missing_tables}")

# 3. Schema column sanity on the high-value tables.
required_columns = {
    "predictions": {"game_id", "model_type", "predicted_at"},
    "games": {"game_id", "sport_id", "home_team_id", "away_team_id"},
    "odds_snapshots": {"snapshot_id", "fetched_at_utc", "sport_id"},
}
for table, expected in required_columns.items():
    if table not in present_tables:
        continue
    cols = {row[1] for row in cur.execute(f"PRAGMA table_info({table})").fetchall()}
    missing_cols = sorted(expected - cols)
    if missing_cols:
        failures.append(f"{table} missing columns: {missing_cols}")

# 4. Row counts. Anything below the floor is suspicious — a fresh DB with
# only schema would pass the table check but be useless for serving traffic.
row_floors = {
    "sports": 1,
    "teams": 1,
    "games": 1,
    "predictions": 1,
}
for table, floor in row_floors.items():
    if table not in present_tables:
        continue
    n = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"[drill]   {table}: {n} rows")
    if n < floor:
        failures.append(f"{table} has {n} rows (< {floor})")

# 5. Predictions should not be ancient. We don't fail the drill on this
# because a backup taken during a long quiet period is technically valid,
# but it's worth surfacing.
try:
    latest = cur.execute(
        "SELECT MAX(predicted_at) FROM predictions"
    ).fetchone()[0]
    print(f"[drill]   latest prediction: {latest}")
except sqlite3.Error as exc:
    warnings.append(f"could not read latest prediction: {exc}")

# 6. DB size sanity — a corruption that resets the file to near-empty
# wouldn't show up in integrity_check if the page is structurally valid.
size_mb = os.path.getsize(db) / 1024 / 1024
print(f"[drill]   db size: {size_mb:.1f} MB")
if size_mb < 0.1:
    failures.append(f"db too small ({size_mb:.2f} MB) — likely truncated")

con.close()

# 7. Models tree should contain *something*. We don't try to load every
# pickle (that pulls in the runtime); existence + non-zero sizes are enough
# to know the s3 sync didn't silently produce empty files.
if models_dir.exists():
    files = [p for p in models_dir.rglob("*") if p.is_file()]
    print(f"[drill] models files restored: {len(files)}")
    if not files:
        failures.append("models tree restored but contains no files")
    else:
        empty = [p for p in files if p.stat().st_size == 0]
        if empty:
            failures.append(f"{len(empty)} model files restored at size 0")
else:
    warnings.append("models dir not present after restore")

if warnings:
    print("[drill] warnings:")
    for w in warnings:
        print(f"  - {w}")

if failures:
    print("[drill] FAIL", file=sys.stderr)
    for f in failures:
        print(f"  - {f}", file=sys.stderr)
    sys.exit(1)

print("[drill] all checks PASS")
PY
then
	echo "[drill] FAIL" >&2
	exit 1
fi

echo "[drill] PASS"
