#!/usr/bin/env bash
#
# Per-job wrapper invoked by cron inside the worker container.
# Loads the worker env, sets a unique log file, dispatches to the right
# pipeline command, and writes a heartbeat file so the container healthcheck
# can tell whether jobs are succeeding or silently failing.
#
# Usage: run_job.sh <predict|train|benchmark|backup_local|backup_remote>
#
# Heartbeats live in $HEARTBEAT_DIR (default /app/logs/heartbeat):
#   <job>.ok    — touched on success, body = ISO-8601 UTC timestamp
#   <job>.fail  — touched on failure, body = ISO-8601 UTC timestamp + exit code
#
# scripts/worker_health.sh reads these and reports a degraded healthcheck if
# any *.fail file is newer than its sibling *.ok file.

set -uo pipefail

JOB=${1:?job name required: predict|train|benchmark|backup_local|backup_remote}

# Load env that was captured by worker_entrypoint.sh.
ENV_FILE=/app/scripts/.worker_env
if [[ -r "$ENV_FILE" ]]; then
	set -a
	# shellcheck disable=SC1090
	. "$ENV_FILE"
	set +a
fi

LOG_DIR=${LOG_DIR:-/app/logs}
HEARTBEAT_DIR=${HEARTBEAT_DIR:-/app/logs/heartbeat}
TS=$(date -u +%Y%m%d_%H%M%S)
LOG_FILE="${LOG_DIR}/${JOB}_${TS}.log"

mkdir -p "$LOG_DIR" "$HEARTBEAT_DIR"

now_iso() { date -u +%Y-%m-%dT%H:%M:%SZ; }
log() { printf '[%s] %s\n' "$(now_iso)" "$*"; }

run() {
	log "==> $*"
	"$@"
}

write_heartbeat() {
	local kind=$1 status=$2
	# Atomic write: rename a sibling temp file so a partial write can never
	# be observed by the healthcheck.
	local target="${HEARTBEAT_DIR}/${JOB}.${kind}"
	local tmp="${target}.${$}.tmp"
	{
		printf 'timestamp=%s\n' "$(now_iso)"
		printf 'job=%s\n' "$JOB"
		printf 'status=%s\n' "$status"
		printf 'log=%s\n' "$LOG_FILE"
	} > "$tmp"
	mv "$tmp" "$target"
}

dispatch() {
	case "$JOB" in
		predict)
			# Hourly fast path: ingest + predict, no retraining.
			log "Starting hourly predict pipeline"
			run /app/scripts/pipeline.sh --skip-training
			;;

		train)
			# Daily full path: ingest + quality benchmark + predict + publish gate.
			log "Starting daily paid-picks readiness pipeline"
			run /app/scripts/pipeline.sh
			;;

		benchmark)
			# Daily benchmark-only path: data hygiene + rolling-origin portfolio
			# benchmark + publish gate, without the legacy prediction refresh.
			log "Starting daily paid-picks benchmark"
			run /app/scripts/pipeline.sh --skip-odds --skip-prediction
			;;

		backup_local)
			log "Starting local backup snapshot"
			run python scripts/backup_data.py
			;;

		backup_remote)
			log "Starting external backup push"
			run /app/scripts/backup_to_s3.sh
			;;

		*)
			echo "unknown job: $JOB" >&2
			exit 64
			;;
	esac
}

# Run dispatch under tee so cron logs and the per-run log both see the
# output. Don't `set -e` over the pipeline — we always want the heartbeat
# to be written, success or fail, so health stays observable.
status=0
{
	dispatch
} >> "$LOG_FILE" 2>&1 || status=$?

if (( status == 0 )); then
	log "${JOB} OK"
	write_heartbeat ok ok
else
	log "${JOB} FAILED exit=${status} (see $LOG_FILE)"
	write_heartbeat fail "exit=${status}"
fi

exit "$status"
