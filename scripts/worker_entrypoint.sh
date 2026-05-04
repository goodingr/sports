#!/usr/bin/env bash
#
# Worker entrypoint — runs as PID 1 inside the worker container.
# Seeds /etc/cron.d/sports with the schedules in the WORKER_*_CRON env vars
# and hands off to cron in the foreground so tini can supervise it.
#
# Each cron line drops privileges to the `app` user via su.

set -euo pipefail

LOG_DIR=${LOG_DIR:-/app/logs}
HEARTBEAT_DIR=${HEARTBEAT_DIR:-/app/logs/heartbeat}
mkdir -p "$LOG_DIR" "$HEARTBEAT_DIR"
chown app:app "$LOG_DIR" "$HEARTBEAT_DIR"

CRON_FILE=/etc/cron.d/sports
RELEASE_LEAGUES=${RELEASE_LEAGUES:-NBA,NFL,NCAAB,NHL,CFB,EPL,LALIGA,BUNDESLIGA,SERIEA,LIGUE1}

# Cron in Debian doesn't inherit the container's env. We dump only the env
# vars the pipeline actually needs to a file that each app job sources via
# `set -a; . /app/scripts/.worker_env; set +a` inside run_job.sh.
#
# The previous implementation dumped *every* exported variable. That meant
# transient shell internals, container-runtime metadata, and unrelated
# secrets all ended up in a 600-mode file inside the container. Narrowing
# this list keeps the blast radius small — anything not on the allow-list
# below stays in the entrypoint's process tree only.
#
# To add a new variable: add it (or a matching prefix) to WORKER_ENV_KEEP /
# WORKER_ENV_PREFIXES below.
ENV_FILE=/app/scripts/.worker_env

WORKER_ENV_KEEP=(
	PYTHONPATH PATH TZ LOG_LEVEL
	SPORTS_DB_PATH DATABASE_PATH DB_PATH MODELS_DIR MODEL_DIR
	BACKUP_DIR REMOTE_BACKUP_RETENTION_DAYS
	RELEASE_LEAGUES PAID_RELEASE_LEAGUES SCORE_BACKFILL_LOOKBACK_DAYS
	ODDS_FRESHNESS_MINUTES PREDICTIONS_FRESHNESS_MINUTES MIN_DISK_FREE_MB
	HEARTBEAT_DIR LOG_DIR
	NEXT_PUBLIC_API_URL NEXT_PUBLIC_APP_URL
)

WORKER_ENV_PREFIXES=(
	# Cloud creds + S3 backup config.
	AWS_ BACKUP_S3_
	# Per-source data-provider keys (the-odds-api, ESPN, etc.).
	ODDS_API_ KILLER_SPORTS_ ESPN_ NBA_ NFL_ NHL_ NCAA_
	# Worker schedule overrides.
	WORKER_
	# Sports-specific app config the pipeline can read.
	SPORTS_
)

worker_env_keep_var() {
	local key=$1 keeper prefix
	for keeper in "${WORKER_ENV_KEEP[@]}"; do
		if [[ "$key" == "$keeper" ]]; then return 0; fi
	done
	for prefix in "${WORKER_ENV_PREFIXES[@]}"; do
		if [[ "$key" == ${prefix}* ]]; then return 0; fi
	done
	return 1
}

{
	while IFS='=' read -r key value; do
		[[ -z "$key" ]] && continue
		if worker_env_keep_var "$key"; then
			# %q produces a shell-safe quoting so values with spaces, quotes,
			# or metacharacters survive `set -a; . file; set +a` in run_job.sh.
			printf '%s=%q\n' "$key" "$value"
		fi
	done < <(env)
} > "$ENV_FILE"
chmod 600 "$ENV_FILE"
chown app:app "$ENV_FILE"

# ----------------------------------------------------------------------------
# Build crontab
# ----------------------------------------------------------------------------
cat > "$CRON_FILE" <<EOF
SHELL=/bin/bash
PATH=/opt/venv/bin:/usr/local/bin:/usr/bin:/bin

${WORKER_PREDICT_CRON:-15 * * * *}       app /app/scripts/run_job.sh predict       >> $LOG_DIR/cron.log 2>&1
${WORKER_TRAIN_CRON:-0 9 * * *}          app /app/scripts/run_job.sh train         >> $LOG_DIR/cron.log 2>&1
${WORKER_BENCHMARK_CRON:-30 10 * * *}    app /app/scripts/run_job.sh benchmark     >> $LOG_DIR/cron.log 2>&1
${WORKER_BACKUP_CRON:-0 18 * * *}        app /app/scripts/run_job.sh backup_local  >> $LOG_DIR/cron.log 2>&1
${WORKER_REMOTE_BACKUP_CRON:-30 18 * * *} app /app/scripts/run_job.sh backup_remote >> $LOG_DIR/cron.log 2>&1
EOF
chmod 0644 "$CRON_FILE"

echo "[worker] crontab installed:"
sed 's/^/  /' "$CRON_FILE"

# Make sure the log file exists so `tail -f` works for ops.
touch "$LOG_DIR/cron.log"
chown app:app "$LOG_DIR/cron.log"

# Tail the cron log to the container's stdout so `docker logs` works.
tail -F "$LOG_DIR/cron.log" &

# Foreground cron with logging to stderr so tini sees a real PID 1 child.
exec cron -f -L 15
