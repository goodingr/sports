#!/usr/bin/env bash
#
# Push the latest local backup snapshot to S3-compatible object storage.
#
# Reads from $BACKUP_DIR (default /app/backups) — the directory layout
# created by scripts/backup_data.py: one timestamped subdir per day plus
# raw .db copies for ad-hoc snapshots.
#
# Writes to:
#   s3://$BACKUP_S3_BUCKET/$BACKUP_S3_PREFIX/<UTC date>/<filename>
#
# Required env:
#   BACKUP_S3_BUCKET, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
#
# Optional env:
#   BACKUP_S3_PREFIX     (default: prod)
#   BACKUP_S3_ENDPOINT   (set for B2/R2/MinIO; omit for AWS S3)
#   BACKUP_S3_REGION     (default: us-east-1)
#   BACKUP_DIR           (default: /app/backups)
#   REMOTE_BACKUP_RETENTION_DAYS (default: 30)
#
# Exit codes:
#   0  success
#   1  config error
#   2  backup snapshot missing
#   3  upload failed

set -euo pipefail

: "${BACKUP_S3_BUCKET:?BACKUP_S3_BUCKET is required}"
: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID is required}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY is required}"

BACKUP_DIR=${BACKUP_DIR:-/app/backups}
BACKUP_S3_PREFIX=${BACKUP_S3_PREFIX:-prod}
BACKUP_S3_REGION=${BACKUP_S3_REGION:-us-east-1}
REMOTE_BACKUP_RETENTION_DAYS=${REMOTE_BACKUP_RETENTION_DAYS:-30}

AWS_CMD=(aws --region "$BACKUP_S3_REGION")
if [[ -n "${BACKUP_S3_ENDPOINT:-}" ]]; then
	AWS_CMD+=(--endpoint-url "$BACKUP_S3_ENDPOINT")
fi

log() { printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }

if [[ ! -d "$BACKUP_DIR" ]]; then
	log "ERROR: backup dir $BACKUP_DIR does not exist"
	exit 2
fi

# Pick the newest timestamped subdir created by scripts/backup_data.py.
LATEST_SNAPSHOT=$(find "$BACKUP_DIR" -maxdepth 1 -mindepth 1 -type d \
	-printf '%T@ %p\n' 2>/dev/null \
	| sort -nr | head -n1 | awk '{print $2}')

if [[ -z "$LATEST_SNAPSHOT" ]]; then
	log "ERROR: no snapshot directory found under $BACKUP_DIR"
	exit 2
fi

UTC_DATE=$(date -u +%Y-%m-%d)
S3_DEST="s3://${BACKUP_S3_BUCKET}/${BACKUP_S3_PREFIX}/${UTC_DATE}/"

log "uploading $LATEST_SNAPSHOT -> $S3_DEST"
if ! "${AWS_CMD[@]}" s3 sync "$LATEST_SNAPSHOT" "$S3_DEST" \
	--no-progress --only-show-errors --storage-class STANDARD_IA; then
	log "ERROR: upload failed"
	exit 3
fi

# Also push the model artifacts so a clean machine can fully restore from S3.
MODELS_DIR_LOCAL=${MODELS_DIR:-/app/models}
if [[ -d "$MODELS_DIR_LOCAL" ]]; then
	S3_MODELS_DEST="s3://${BACKUP_S3_BUCKET}/${BACKUP_S3_PREFIX}/${UTC_DATE}/models/"
	log "uploading models -> $S3_MODELS_DEST"
	if ! "${AWS_CMD[@]}" s3 sync "$MODELS_DIR_LOCAL" "$S3_MODELS_DEST" \
		--no-progress --only-show-errors --delete; then
		log "WARN: models upload failed (snapshot still pushed)"
	fi
fi

# Write a manifest so restore can resolve "latest" without listing dates.
MANIFEST=$(mktemp)
trap 'rm -f "$MANIFEST"' EXIT
{
	printf 'snapshot_date=%s\n' "$UTC_DATE"
	printf 'snapshot_dir=%s\n' "$(basename "$LATEST_SNAPSHOT")"
	printf 'uploaded_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
	printf 'host=%s\n' "$(hostname)"
} > "$MANIFEST"

"${AWS_CMD[@]}" s3 cp "$MANIFEST" \
	"s3://${BACKUP_S3_BUCKET}/${BACKUP_S3_PREFIX}/latest.txt" \
	--no-progress --only-show-errors

log "remote backup complete: ${UTC_DATE}"

# ----------------------------------------------------------------------------
# Retention enforcement (defence-in-depth — prefer bucket lifecycle policies).
# ----------------------------------------------------------------------------
CUTOFF=$(date -u -d "${REMOTE_BACKUP_RETENTION_DAYS} days ago" +%Y-%m-%d 2>/dev/null \
	|| date -u -v-"${REMOTE_BACKUP_RETENTION_DAYS}d" +%Y-%m-%d)

log "pruning snapshots older than $CUTOFF"
"${AWS_CMD[@]}" s3 ls "s3://${BACKUP_S3_BUCKET}/${BACKUP_S3_PREFIX}/" \
	| awk '{print $2}' | sed 's:/$::' \
	| while read -r entry; do
		# Only consider entries that look like dates.
		if [[ "$entry" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] && [[ "$entry" < "$CUTOFF" ]]; then
			log "  pruning $entry"
			"${AWS_CMD[@]}" s3 rm "s3://${BACKUP_S3_BUCKET}/${BACKUP_S3_PREFIX}/${entry}/" \
				--recursive --only-show-errors || log "  WARN: prune failed for $entry"
		fi
	done

log "done"
