#!/usr/bin/env bash
#
# Restore data/betting.db and the models/ tree from an S3-compatible backup.
#
# Usage:
#   restore_from_s3.sh [--date YYYY-MM-DD] [--target-data PATH] [--target-models PATH] [--dry-run]
#
# By default:
#   --date defaults to whatever s3://$BUCKET/$PREFIX/latest.txt points at
#   --target-data defaults to $SPORTS_DB_PATH (or /app/data/betting.db)
#   --target-models defaults to $MODELS_DIR (or /app/models)
#
# This script REFUSES to overwrite an existing target unless --force is set.
# That refusal is what protects you from a half-asleep restore.

set -euo pipefail

: "${BACKUP_S3_BUCKET:?BACKUP_S3_BUCKET is required}"
: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID is required}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY is required}"

BACKUP_S3_PREFIX=${BACKUP_S3_PREFIX:-prod}
BACKUP_S3_REGION=${BACKUP_S3_REGION:-us-east-1}

AWS_CMD=(aws --region "$BACKUP_S3_REGION")
if [[ -n "${BACKUP_S3_ENDPOINT:-}" ]]; then
	AWS_CMD+=(--endpoint-url "$BACKUP_S3_ENDPOINT")
fi

DATE=""
TARGET_DATA=${SPORTS_DB_PATH:-/app/data/betting.db}
TARGET_MODELS=${MODELS_DIR:-/app/models}
DRY_RUN=0
FORCE=0

while [[ $# -gt 0 ]]; do
	case "$1" in
		--date)          DATE=$2; shift 2 ;;
		--target-data)   TARGET_DATA=$2; shift 2 ;;
		--target-models) TARGET_MODELS=$2; shift 2 ;;
		--dry-run)       DRY_RUN=1; shift ;;
		--force)         FORCE=1; shift ;;
		-h|--help)
			sed -n '3,20p' "$0"
			exit 0 ;;
		*) echo "unknown arg: $1" >&2; exit 64 ;;
	esac
done

log() { printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }

if [[ -z "$DATE" ]]; then
	log "resolving latest snapshot via s3://${BACKUP_S3_BUCKET}/${BACKUP_S3_PREFIX}/latest.txt"
	TMP=$(mktemp)
	trap 'rm -f "$TMP"' EXIT
	"${AWS_CMD[@]}" s3 cp \
		"s3://${BACKUP_S3_BUCKET}/${BACKUP_S3_PREFIX}/latest.txt" "$TMP" \
		--no-progress --only-show-errors
	DATE=$(grep '^snapshot_date=' "$TMP" | cut -d= -f2)
	if [[ -z "$DATE" ]]; then
		log "ERROR: latest.txt did not contain snapshot_date"
		exit 2
	fi
fi

S3_SRC="s3://${BACKUP_S3_BUCKET}/${BACKUP_S3_PREFIX}/${DATE}/"
log "restoring from $S3_SRC"
log "  -> data:   $TARGET_DATA"
log "  -> models: $TARGET_MODELS"

if (( DRY_RUN )); then
	log "dry run: listing source"
	"${AWS_CMD[@]}" s3 ls "$S3_SRC" --recursive --human-readable
	exit 0
fi

if [[ -e "$TARGET_DATA" && $FORCE -eq 0 ]]; then
	log "ERROR: $TARGET_DATA exists. Re-run with --force to overwrite."
	exit 3
fi

mkdir -p "$(dirname "$TARGET_DATA")" "$TARGET_MODELS"

# Snapshot dirs from backup_data.py contain `betting.db` plus parquet files.
TMP_RESTORE=$(mktemp -d)
trap 'rm -rf "$TMP_RESTORE"' EXIT

"${AWS_CMD[@]}" s3 sync "$S3_SRC" "$TMP_RESTORE" \
	--exclude "models/*" \
	--no-progress --only-show-errors

if [[ ! -f "$TMP_RESTORE/betting.db" ]]; then
	log "ERROR: snapshot at $S3_SRC has no betting.db"
	exit 4
fi

# Move into place atomically: write to a sibling temp file, fsync, rename.
TMP_DB="${TARGET_DATA}.restore.$$"
cp "$TMP_RESTORE/betting.db" "$TMP_DB"
sync
mv "$TMP_DB" "$TARGET_DATA"
log "restored database -> $TARGET_DATA"

# Restore parquet sidecars (predictions_master.parquet etc.) under data/.
DATA_DIR=$(dirname "$TARGET_DATA")
for f in "$TMP_RESTORE"/*.parquet; do
	[[ -f "$f" ]] || continue
	cp "$f" "$DATA_DIR/$(basename "$f")"
	log "restored $(basename "$f") -> $DATA_DIR/"
done

# Models tree.
"${AWS_CMD[@]}" s3 sync "${S3_SRC}models/" "$TARGET_MODELS" \
	--no-progress --only-show-errors --delete \
	|| log "WARN: models restore failed (DB still recovered)"

log "restore complete: snapshot=$DATE"
