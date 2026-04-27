#!/usr/bin/env bash
#
# Per-job wrapper invoked by cron inside the worker container.
# Loads the worker env, sets a unique log file, and dispatches to the right
# pipeline command. Exit code is preserved so cron's MAILTO / log surfaces it.
#
# Usage: run_job.sh <predict|train|backup_local|backup_remote>

set -euo pipefail

JOB=${1:?job name required: predict|train|backup_local|backup_remote}

# Load env that was captured by worker_entrypoint.sh.
ENV_FILE=/app/scripts/.worker_env
if [[ -r "$ENV_FILE" ]]; then
	set -a
	# shellcheck disable=SC1090
	. "$ENV_FILE"
	set +a
fi

LOG_DIR=${LOG_DIR:-/app/logs}
TS=$(date -u +%Y%m%d_%H%M%S)
LOG_FILE="${LOG_DIR}/${JOB}_${TS}.log"

mkdir -p "$LOG_DIR"

log() { printf '[%s] %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"; }

run() {
	log "==> $*"
	"$@"
}

case "$JOB" in
	predict)
		# Hourly fast path: ingest + predict, no retraining.
		{
			log "Starting hourly predict pipeline"
			run /app/scripts/pipeline.sh --skip-training
			log "Predict pipeline complete"
		} >> "$LOG_FILE" 2>&1
		;;

	train)
		# Daily full path: ingest + train + predict.
		{
			log "Starting daily training pipeline"
			run /app/scripts/pipeline.sh
			log "Training pipeline complete"
		} >> "$LOG_FILE" 2>&1
		;;

	backup_local)
		{
			log "Starting local backup snapshot"
			run python scripts/backup_data.py
			log "Local backup complete"
		} >> "$LOG_FILE" 2>&1
		;;

	backup_remote)
		{
			log "Starting external backup push"
			run /app/scripts/backup_to_s3.sh
			log "External backup complete"
		} >> "$LOG_FILE" 2>&1
		;;

	*)
		echo "unknown job: $JOB" >&2
		exit 64
		;;
esac
