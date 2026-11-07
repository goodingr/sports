#!/usr/bin/env bash
# Upload a completed snapshot to an immutable S3 generation, then publish pointers.
set -euo pipefail

: "${BACKUP_S3_BUCKET:?BACKUP_S3_BUCKET is required}"
: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID is required}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY is required}"

BACKUP_DIR=${BACKUP_DIR:-/app/backups}
BACKUP_S3_PREFIX=${BACKUP_S3_PREFIX:-prod}
BACKUP_S3_REGION=${BACKUP_S3_REGION:-us-east-1}
REMOTE_BACKUP_RETENTION_DAYS=${REMOTE_BACKUP_RETENTION_DAYS:-30}
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
ARCHIVE="$SCRIPT_DIR/backup_archive.py"
AWS_CMD=(aws --region "$BACKUP_S3_REGION")
if [[ -n "${BACKUP_S3_ENDPOINT:-}" ]]; then
    AWS_CMD+=(--endpoint-url "$BACKUP_S3_ENDPOINT")
fi

log() { printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }

if [[ ! "$REMOTE_BACKUP_RETENTION_DAYS" =~ ^[1-9][0-9]*$ ]]; then
    log "ERROR: retention must be a positive number of days"
    exit 1
fi
if ! LATEST_SNAPSHOT=$(python "$ARCHIVE" latest "$BACKUP_DIR"); then
    exit 2
fi

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
PAYLOAD="$LATEST_SNAPSHOT"
if [[ ! -f "$PAYLOAD/manifest.json" ]]; then
    PAYLOAD="$WORK/payload"
    python "$ARCHIVE" prepare-legacy "$LATEST_SNAPSHOT" "$PAYLOAD" "${MODELS_DIR:-/app/models}"
fi
python "$ARCHIVE" verify "$PAYLOAD"

UTC_DATE=$(date -u +%Y-%m-%d)
GENERATION="$(basename "$LATEST_SNAPSHOT")-$(python -c 'import uuid; print(uuid.uuid4().hex)')"
SNAPSHOT_PREFIX="${UTC_DATE}/${GENERATION}"
S3_ROOT="s3://${BACKUP_S3_BUCKET}/${BACKUP_S3_PREFIX}"
S3_DEST="${S3_ROOT}/${SNAPSHOT_PREFIX}/"

log "uploading $PAYLOAD -> $S3_DEST"
if ! "${AWS_CMD[@]}" s3 sync "$PAYLOAD" "$S3_DEST" \
    --no-progress --only-show-errors --storage-class STANDARD_IA; then
    log "ERROR: upload failed; previous pointers retained"
    exit 3
fi

{
    printf 'snapshot_date=%s\n' "$UTC_DATE"
    printf 'snapshot_prefix=%s\n' "$SNAPSHOT_PREFIX"
    printf 'snapshot_dir=%s\n' "$(basename "$LATEST_SNAPSHOT")"
    printf 'uploaded_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'host=%s\n' "$(hostname)"
} > "$WORK/latest.txt"

"${AWS_CMD[@]}" s3 cp "$WORK/latest.txt" "${S3_ROOT}/${UTC_DATE}/latest.txt" \
    --no-progress --only-show-errors
"${AWS_CMD[@]}" s3 cp "$WORK/latest.txt" "${S3_ROOT}/latest.txt" \
    --no-progress --only-show-errors
log "remote backup complete: $SNAPSHOT_PREFIX"

CUTOFF=$(python -c 'import datetime,sys; print((datetime.datetime.now(datetime.timezone.utc)-datetime.timedelta(days=int(sys.argv[1]))).date())' "$REMOTE_BACKUP_RETENTION_DAYS")
"${AWS_CMD[@]}" s3 ls "${S3_ROOT}/" | awk '{print $2}' | sed 's:/$::' |
while read -r entry; do
    if [[ "$entry" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ && "$entry" < "$CUTOFF" ]]; then
        log "pruning $entry"
        "${AWS_CMD[@]}" s3 rm "${S3_ROOT}/${entry}/" --recursive --only-show-errors ||
            log "WARN: prune failed for $entry"
    fi
done
