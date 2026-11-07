#!/usr/bin/env bash
# Restore a verified DB, parquet sidecars and model archive. Stop API and worker first.
# Usage: restore_from_s3.sh [--date YYYY-MM-DD] [--target-data PATH]
#   [--target-models PATH] [--dry-run] [--force]
set -euo pipefail

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
    sed -n '2,4p' "$0"
    exit 0
fi
: "${BACKUP_S3_BUCKET:?BACKUP_S3_BUCKET is required}"
: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID is required}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY is required}"

BACKUP_S3_PREFIX=${BACKUP_S3_PREFIX:-prod}
BACKUP_S3_REGION=${BACKUP_S3_REGION:-us-east-1}
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
ARCHIVE="$SCRIPT_DIR/backup_archive.py"
AWS_CMD=(aws --region "$BACKUP_S3_REGION")
if [[ -n "${BACKUP_S3_ENDPOINT:-}" ]]; then
    AWS_CMD+=(--endpoint-url "$BACKUP_S3_ENDPOINT")
fi
DATE=""
TARGET_DATA=${SPORTS_DB_PATH:-/app/data/betting.db}
TARGET_MODELS=${MODELS_DIR:-/app/models}
DRY_RUN=0
FORCE_ARGS=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --date|--target-data|--target-models)
            if [[ $# -lt 2 || -z "$2" ]]; then echo "missing value for $1" >&2; exit 64; fi
            case "$1" in
                --date) DATE=$2 ;;
                --target-data) TARGET_DATA=$2 ;;
                --target-models) TARGET_MODELS=$2 ;;
            esac
            shift 2 ;;
        --dry-run) DRY_RUN=1; shift ;;
        --force) FORCE_ARGS=(--force); shift ;;
        *) echo "unknown arg: $1" >&2; exit 64 ;;
    esac
done
if [[ -n "$DATE" && ! "$DATE" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
    echo "invalid snapshot date" >&2
    exit 64
fi
if [[ -e "$TARGET_DATA" && ${#FORCE_ARGS[@]} -eq 0 && "$DRY_RUN" -eq 0 ]]; then
    echo "target database exists; use --force for an offline restore" >&2
    exit 3
fi

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
S3_ROOT="s3://${BACKUP_S3_BUCKET}/${BACKUP_S3_PREFIX}"
if [[ -z "$DATE" ]]; then
    "${AWS_CMD[@]}" s3 cp "${S3_ROOT}/latest.txt" "$WORK/pointer" --no-progress --only-show-errors
    SNAPSHOT_PREFIX=$(python "$ARCHIVE" pointer "$WORK/pointer")
elif "${AWS_CMD[@]}" s3 cp "${S3_ROOT}/${DATE}/latest.txt" "$WORK/pointer" --no-progress --only-show-errors; then
    SNAPSHOT_PREFIX=$(python "$ARCHIVE" pointer "$WORK/pointer")
    if [[ "$SNAPSHOT_PREFIX" != "${DATE}/"* ]]; then
        echo "snapshot day pointer has a different date" >&2
        exit 2
    fi
else
    SNAPSHOT_PREFIX="$DATE"
fi
S3_SRC="${S3_ROOT}/${SNAPSHOT_PREFIX}/"
printf 'restoring from %s\n' "$S3_SRC"
if (( DRY_RUN )); then
    "${AWS_CMD[@]}" s3 ls "$S3_SRC" --recursive --human-readable
    exit 0
fi

"${AWS_CMD[@]}" s3 sync "$S3_SRC" "$WORK/archive" --no-progress --only-show-errors
LEGACY_ARGS=()
if [[ "$SNAPSHOT_PREFIX" != */* ]]; then LEGACY_ARGS=(--allow-legacy); fi
python "$ARCHIVE" install "$WORK/archive" "$TARGET_DATA" "$TARGET_MODELS" \
    "${FORCE_ARGS[@]}" "${LEGACY_ARGS[@]}"
printf 'restore complete: %s\n' "$SNAPSHOT_PREFIX"
