#!/usr/bin/env bash
#
# Worker container healthcheck.
#
# Returns 0 (healthy) only if:
#   1. cron is running (otherwise no jobs will ever fire).
#   2. No tracked job has a `.fail` heartbeat newer than its `.ok` heartbeat.
#
# Heartbeats are written by scripts/run_job.sh after every cron-driven job.
# Missing heartbeats (e.g. the container started <1h ago and predict hasn't
# fired yet) are treated as "unknown, give it time" rather than as failures —
# we do *not* want a freshly-restarted container to flap the healthcheck just
# because cron hasn't ticked yet.
#
# If you need to debug locally:
#   docker compose exec worker /app/scripts/worker_health.sh; echo exit=$?

set -uo pipefail

HEARTBEAT_DIR=${HEARTBEAT_DIR:-/app/logs/heartbeat}
TRACKED_JOBS=(predict train backup_local backup_remote)

log() { printf '[health] %s\n' "$*" >&2; }

if ! pgrep -x cron >/dev/null 2>&1; then
	log "FAIL: cron is not running"
	exit 1
fi

problems=0
for job in "${TRACKED_JOBS[@]}"; do
	ok="${HEARTBEAT_DIR}/${job}.ok"
	fail="${HEARTBEAT_DIR}/${job}.fail"

	if [[ -f "$fail" ]]; then
		# A failure is "stale" only if a newer success has happened since.
		if [[ ! -f "$ok" || "$fail" -nt "$ok" ]]; then
			log "FAIL: $job last run failed ($fail newer than ${ok:-<missing>})"
			problems=$((problems + 1))
		fi
	fi
done

if (( problems > 0 )); then
	exit 1
fi

# Optional warning: predict is the hot path. If we have an OK heartbeat at
# all and it's older than 3 hours, something has wedged cron without leaving
# a fail marker. Accept this as a soft signal — still warn but exit 0 so we
# don't kill the container during normal off-hours.
if [[ -f "${HEARTBEAT_DIR}/predict.ok" ]]; then
	# stat -c is GNU coreutils; the worker image is debian-bookworm so we have it.
	now=$(date -u +%s)
	last=$(stat -c %Y "${HEARTBEAT_DIR}/predict.ok" 2>/dev/null || echo "$now")
	age=$((now - last))
	if (( age > 10800 )); then
		log "WARN: predict heartbeat is ${age}s old"
	fi
fi

log "OK"
exit 0
