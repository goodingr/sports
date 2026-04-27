#!/usr/bin/env bash
#
# Worker entrypoint — runs as PID 1 inside the worker container.
# Seeds /etc/cron.d/sports with the schedules in the WORKER_*_CRON env vars
# and hands off to cron in the foreground so tini can supervise it.
#
# Each cron line drops privileges to the `app` user via su.

set -euo pipefail

LOG_DIR=${LOG_DIR:-/app/logs}
mkdir -p "$LOG_DIR"
chown app:app "$LOG_DIR"

CRON_FILE=/etc/cron.d/sports
RELEASE_LEAGUES=${RELEASE_LEAGUES:-NBA,NFL,NCAAB,NHL,CFB,EPL,LALIGA,BUNDESLIGA,SERIEA,LIGUE1}

# Cron in Debian doesn't inherit the container's env. We dump the env to a
# file each app job reads via `set -a; . /app/scripts/.worker_env; set +a`
# inside the wrappers below. Done this way so we don't leak secrets through
# crontab itself (which is world-readable in some images).
ENV_FILE=/app/scripts/.worker_env
{
	while IFS='=' read -r key value; do
		case "$key" in
			''|PWD|HOME|OLDPWD|SHLVL|_) continue ;;
		esac
		# %q produces a shell-safe quoting so values with spaces, quotes,
		# or metacharacters survive `set -a; . file; set +a` in run_job.sh.
		printf '%s=%q\n' "$key" "$value"
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
