# Release Runbook

Operational procedures for the production stack. Each section is meant to be
runnable cold — no prior context required.

Conventions:
- Commands assume `cd <repo-root>` on the host and `docker compose` available.
- `${VAR}` placeholders come from `.env`.
- Times are UTC inside containers (`TZ=UTC`).

## Contents
1. [Deploy a new version](#1-deploy-a-new-version)
2. [Roll back a bad deploy](#2-roll-back-a-bad-deploy)
3. [Run the pipeline manually](#3-run-the-pipeline-manually)
4. [Restore from backup](#4-restore-from-backup)
5. [Data-quality triage](#5-data-quality-triage)
6. [Cert / TLS issues](#6-cert--tls-issues)
7. [Worker not running jobs](#7-worker-not-running-jobs)

---

## 1. Deploy a new version

Standard release. Assume `main` has been reviewed and merged.

```bash
# 1. Note the current SHA — you'll need it for rollback.
git -C /opt/sports rev-parse HEAD > /tmp/last_good_sha

# 2. Pull and rebuild only the images that changed.
cd /opt/sports
git fetch origin && git checkout main && git pull
docker compose build api web worker

# 3. Roll the services. Compose preserves volumes.
docker compose up -d api web worker

# 4. Smoke-check.
sleep 10
curl -fsS "https://${API_DOMAIN}/health"
curl -fsS "https://${API_DOMAIN}/ready" | jq .
docker compose ps
```

**Expected:** all services `healthy`, `/health` returns `{"status":"healthy"}`,
`/ready` reports green for odds + predictions freshness and disk free.

**If anything looks wrong:** go to [§2 Roll back](#2-roll-back-a-bad-deploy)
before debugging. Rollback first, root-cause second.

---

## 2. Roll back a bad deploy

Symptoms that warrant immediate rollback:
- `/health` 5xx for >2 minutes
- `/ready` reporting stale data or low disk
- web app login broken or 5xx on `/`
- worker container in a crash loop

```bash
cd /opt/sports
LAST=$(cat /tmp/last_good_sha)
git checkout "$LAST"
docker compose build api web worker
docker compose up -d api web worker
```

If the bad deploy involved a schema migration that worked but you don't trust
it, also restore the DB to the pre-deploy snapshot — see
[§4 Restore from backup](#4-restore-from-backup). The 18:00 UTC daily snapshot
is the reliable cutover point.

If a Caddy/proxy change broke routing:
```bash
docker compose restart proxy
docker compose logs --tail=200 proxy
```

If you cannot identify a known-good SHA, restore the latest backup and stand
the stack back up at the previous tag (`git tag -l 'v*'`).

---

## 3. Run the pipeline manually

The worker runs the pipeline on cron. To trigger off-schedule:

```bash
# Hourly fast path: ingest + predict, no retraining.
docker compose exec -u app worker /app/scripts/pipeline.sh --skip-training

# Full daily path: ingest + train + predict.
docker compose exec -u app worker /app/scripts/pipeline.sh

# Single league only (debug a flaky source):
docker compose exec -u app worker python -m src.data.ingest_manager --leagues NFL
docker compose exec -u app worker python -m src.predict.runner --leagues NFL --model-type ensemble

# Watch the latest log.
docker compose exec worker bash -lc 'ls -t /app/logs/pipeline_*.log | head -1 | xargs tail -f'
```

A clean run finishes with `pipeline complete` in `/app/logs/pipeline_<ts>.log`.
Any `WARN:` lines indicate non-fatal step failures — investigate but the
pipeline keeps going.

To see what cron has scheduled:
```bash
docker compose exec worker crontab -l -u app
docker compose exec worker tail -f /app/logs/cron.log
```

---

## 4. Restore from backup

See also [docs/BACKUP_RESTORE.md](BACKUP_RESTORE.md) for the full backup model.

**Latest snapshot, in place** (DESTRUCTIVE — overwrites current DB & models):

```bash
docker compose stop worker            # freeze writers
docker compose stop api               # freeze readers
docker compose run --rm worker /app/scripts/restore_from_s3.sh --force
docker compose up -d api worker
```

**Specific date:**
```bash
docker compose run --rm worker /app/scripts/restore_from_s3.sh \
  --date 2026-04-23 --force
```

**Dry run** (lists what would be restored, no writes):
```bash
docker compose run --rm worker /app/scripts/restore_from_s3.sh --dry-run
```

**Drill** (restore to a temp dir, sanity-check, throw away):
```bash
docker compose exec -u app worker /app/scripts/restore_drill.sh
```

The drill is the canonical answer to "are the backups good?". Run monthly
and after any change to the backup chain. Drill failure ⇒ backups don't
work, regardless of what the upload logs say.

---

## 5. Data-quality triage

The API exposes freshness gates at `/ready`. When monitoring fires:

```bash
curl -fsS "https://${API_DOMAIN}/ready" | jq .
```

Common failures and how to dig in:

### Stale odds (`odds_age_minutes` > `ODDS_FRESHNESS_MINUTES`)
```bash
# Most recent odds rows per league:
docker compose exec -u app worker python - <<'PY'
import sqlite3, os
con = sqlite3.connect(os.environ["SPORTS_DB_PATH"])
for row in con.execute("""
    SELECT league, MAX(fetched_at) AS latest, COUNT(*) AS n
    FROM odds_h2h GROUP BY league ORDER BY league;
"""):
    print(row)
PY

# Force a fetch:
docker compose exec -u app worker python -m src.data.ingest_odds \
  --league NFL --market h2h,totals --force-refresh
```

If `force-refresh` 401s, your Odds API key is exhausted — rotate to the next
key by editing `.odds_api_key_index` (the worker auto-rotates on 429s).

### Stale predictions (`predictions_age_minutes` > `PREDICTIONS_FRESHNESS_MINUTES`)
```bash
docker compose exec -u app worker python -m src.predict.runner \
  --leagues NFL --model-type ensemble --log-level DEBUG
```

If the runner errors out about missing models, you skipped training during
restore. Re-run training:
```bash
docker compose exec -u app worker python -m src.models.train \
  --league NFL --model-type ensemble
```

### Stale games / scores
```bash
# List games that should have a final score but don't.
docker compose exec -u app worker python scripts/list_stale_games.py

# Backfill from ESPN.
docker compose exec -u app worker python scripts/backfill_scores_espn.py
```

### Disk pressure
`/ready` reports `disk_free_mb`. When it's red:
```bash
docker compose exec worker df -h /app/data /app/backups /app/logs
docker compose exec worker du -sh /app/logs/* | sort -hr | head
```
Logs rotate by date — purge anything older than 30 days. Backups are pruned
automatically by the daily job, but a stuck job can leave them lingering.

---

## 6. Cert / TLS issues

Symptom: browsers show `NET::ERR_CERT_AUTHORITY_INVALID` or Caddy logs
`obtaining certificate: ... acme: error`.

```bash
docker compose logs --tail=200 proxy
```

Likely causes, in order:
1. **DNS hasn't propagated.** `dig +short ${APP_DOMAIN}` — must resolve to
   the host public IP. Wait, then `docker compose restart proxy`.
2. **Inbound 80/443 blocked.** ACME HTTP-01 needs port 80.
3. **Rate-limited.** LE staging is on; flip `acme_ca` in `Caddyfile` to
   staging while debugging. Production limit is 5 certs/week per FQDN.

The cert volume `caddy_data` survives container restarts. Don't `docker
volume rm` it during routine ops or you'll churn through the rate limit.

---

## 7. Worker not running jobs

```bash
docker compose ps worker            # status should be running + healthy
docker compose logs --tail=100 worker
docker compose exec worker crontab -l -u app
docker compose exec worker tail -f /app/logs/cron.log
```

Common issues:
- `permission denied` on `/app/data/betting.db`: volume was created by a
  different UID. Fix: `docker compose exec -u 0 worker chown -R app:app /app/data /app/models /app/logs /app/backups`.
- Cron starts but jobs never fire: check the WORKER_*_CRON values in `.env`
  — a bad expression silently skips that line.
- Pipeline jobs run but exit non-zero: open the per-run log under
  `/app/logs/<job>_<ts>.log` for the actual stack trace.
