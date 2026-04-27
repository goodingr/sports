#!/usr/bin/env bash
#
# Restore drill — verifies the backup chain end-to-end on a clean directory.
# Run this monthly. The whole point of a backup is the restore; if this drill
# stops passing, you have no backup.
#
# Steps:
#   1. Spin up a temp work dir.
#   2. Pull the latest snapshot from S3 into that dir.
#   3. Open the restored SQLite DB read-only and run a sanity SELECT.
#   4. Print the row counts of a couple of high-value tables.
#   5. Clean up.
#
# Run from inside the worker container (it has aws + python + the codebase):
#   docker compose exec worker /app/scripts/restore_drill.sh

set -euo pipefail

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
/app/scripts/restore_from_s3.sh "${ARGS[@]}"

echo "[drill] sanity-checking restored DB"
export DRILL_DB="$WORK/betting.db"
python - <<'PY'
import os, sqlite3, sys

db = os.environ["DRILL_DB"]
con = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
cur = con.cursor()
cur.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
tables = [r[0] for r in cur.fetchall()]
print(f"[drill] tables: {len(tables)}")

interesting = ("games", "odds", "predictions", "models")
for t in interesting:
    if t in tables:
        n = cur.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
        print(f"[drill]   {t}: {n} rows")
    else:
        print(f"[drill]   {t}: MISSING")

if not any(t in tables for t in interesting):
    print("[drill] FAIL: none of the expected tables are present", file=sys.stderr)
    sys.exit(1)

con.close()
PY

python -c "import os; print('[drill] db size MB:', round(os.path.getsize(os.environ['DRILL_DB'])/1024/1024, 1))"

if [[ -d "$WORK/models" ]]; then
	count=$(find "$WORK/models" -type f | wc -l)
	echo "[drill] models files restored: $count"
fi

echo "[drill] PASS"
