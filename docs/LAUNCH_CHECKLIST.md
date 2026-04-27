# Launch Checklist

Use this list once, before flipping public DNS at the host. Every box must be
checked. Anything you can't tick is a launch blocker, not a wish-list item.

## 1. Host & network

- [ ] Production VM provisioned with Docker Engine 24+ and the Compose v2
      plugin installed.
- [ ] Inbound 80/443 reachable from the public internet.
- [ ] Outbound to `api.the-odds-api.com`, `api.clerk.com`, the S3 endpoint,
      and the model registry / package mirrors not blocked by a host firewall.
- [ ] DNS records:
  - [ ] `${APP_DOMAIN}` resolves to the host.
  - [ ] `${API_DOMAIN}` resolves to the host.
  - [ ] `dig +short ${APP_DOMAIN} ${API_DOMAIN}` returns the host IP.
- [ ] At least 50 GB free on the disk that backs `/var/lib/docker/volumes`.

## 2. Secrets & configuration

- [ ] `.env` exists at the repo root, copied from `.env.example`.
- [ ] All values marked **REQUIRED** in `.env.example` are filled in.
- [ ] `.env` permissions are `600` and owned by the deploy user
      (`stat -c '%a %U' .env`).
- [ ] `.env` is **not** tracked by git (`git status` shows it absent;
      `.gitignore` already excludes `.env*`).
- [ ] `ACME_EMAIL` is a real, monitored inbox.
- [ ] `API_CORS_ORIGINS` lists only `https://${APP_DOMAIN}`.
- [ ] `SPORTS_DB_PATH` and `MODELS_DIR` point inside `/app/...` (not
      host-relative paths).
- [ ] `WORKER_*_CRON` schedules are sane for the launch traffic profile.

## 3. Authentication

- [ ] Clerk **production** instance provisioned (not the dev instance).
- [ ] `CLERK_SECRET_KEY` is a `sk_live_...` key.
- [ ] `NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY` is a `pk_live_...` key.
- [ ] Clerk allowed origins include `https://${APP_DOMAIN}`.
- [ ] Sign-in flow works against the staging stack end-to-end before launch.

## 4. Data sources

- [ ] At least one `ODDS_API_KEY*` set; ideally 3+ for rotation.
- [ ] Manual ingest succeeds against a single league:
      ```
      docker compose run --rm worker python -m src.data.ingest_odds \
        --league NFL --market h2h,totals --force-refresh
      ```
- [ ] `RELEASE_LEAGUES` contains only leagues that are validated for public
      release. Anything else stays masked.

## 5. Backups

- [ ] S3 bucket exists; the IAM user has only the bucket scope.
- [ ] Lifecycle rule on the bucket: retain `${REMOTE_BACKUP_RETENTION_DAYS}`
      days, transition to IA after 30 days (cost optimization, optional).
- [ ] Manual upload smoke-test:
      ```
      docker compose exec -u app worker /app/scripts/backup_to_s3.sh
      ```
- [ ] Restore drill on a clean directory passes:
      ```
      docker compose exec -u app worker /app/scripts/restore_drill.sh
      ```
- [ ] Restore drill output records a row count for `games`, `odds`,
      `predictions`, and `models`.
- [ ] `BACKUP_RESTORE.md` reviewed by whoever holds the ops pager.

## 6. Application health

- [ ] `docker compose ps` shows all four services running and `healthy`.
- [ ] `curl -fsS https://${API_DOMAIN}/health` returns `200`.
- [ ] `curl -fsS https://${API_DOMAIN}/ready` returns `200` and reports
      fresh data (`odds_age_minutes < ODDS_FRESHNESS_MINUTES`,
      `predictions_age_minutes < PREDICTIONS_FRESHNESS_MINUTES`,
      `disk_free_mb > MIN_DISK_FREE_MB`).
- [ ] `curl -fsS https://${APP_DOMAIN}/` returns `200` and the HTML mentions
      the app shell (not the Next.js error page).
- [ ] Worker crontab installed:
      ```
      docker compose exec worker crontab -l -u app
      ```
      shows four lines (predict, train, backup_local, backup_remote).
- [ ] `docker compose logs --since=15m worker` contains no `ERROR` lines.

## 7. End-to-end validation

- [ ] Sign up a fresh user via the live web app and confirm sign-in works.
- [ ] Visit a release league page; predictions render with non-zero rows.
- [ ] Visit a non-release league page; predictions are masked as designed.
- [ ] Force a prediction refresh:
      ```
      docker compose exec -u app worker /app/scripts/pipeline.sh --skip-training
      ```
      and confirm the dashboard shows the new timestamp.

## 8. Observability & on-call

- [ ] External uptime monitor watching `https://${API_DOMAIN}/health` and
      `https://${APP_DOMAIN}/`. Alerts wired to a real human.
- [ ] Log retention policy on the host matches your incident-response needs
      (default: 30 days under `/app/logs`).
- [ ] On-call playbook bookmarks point at `docs/RUNBOOK.md`.
- [ ] Rollback rehearsal performed once: deploy a no-op commit, then roll
      back via the documented procedure.

## 9. Security pass

- [ ] HSTS header present on `${APP_DOMAIN}` (`curl -I` shows
      `Strict-Transport-Security`).
- [ ] No `Server:` header leaking the proxy version.
- [ ] No service exposes ports to the host other than `proxy:80` and
      `proxy:443`.
- [ ] `.env` does not contain any test/dev keys
      (`grep -E '_test_|_test\b|placeholder|replace_me' .env` is empty).
- [ ] Stripe keys, if used, are live keys; webhook signing secret matches
      the registered endpoint.

## 10. Communications

- [ ] Status page URL (or fallback channel) decided and shared.
- [ ] Internal #ops or equivalent channel notified of go-live time.
- [ ] First on-call rotation assigned.

---

Once every box is ticked, flip DNS (or remove the staging password) and
watch the worker complete one full predict cycle (~15 minutes after the
top of the hour) before declaring launch.
