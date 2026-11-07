# Deployment

This stack is designed for **one host**: a single VM or container host running
Docker Engine. Persistent state lives on disk-backed named volumes; the host
does not need a database. Two DNS records (`app.` and `api.`) plus an external
object-storage bucket complete the topology.

```
                ┌────────────────────────────────────────────────┐
                │                   Host (VM)                     │
                │                                                  │
   :443 ───────▶│  proxy (Caddy)  ──▶ web   (Next.js, :3000)     │
                │                  ╲                                │
                │                   ╰▶ api   (FastAPI, :8000) ◀── │
                │                          │                       │
                │                  ┌───────▼───────┐               │
                │   worker (cron)──▶│   /app/data   │              │
                │                  │   /app/models │               │
                │                  │   /app/logs   │               │
                │                  │   /app/backups│ ─────┐        │
                │                  └───────────────┘      │        │
                └─────────────────────────────────────────│────────┘
                                                          │
                                                          ▼
                                            S3-compatible object store
```

## Sizing

A reasonable starting size for a **single-region launch**:

| Resource     | Floor         | Comfortable | Why |
|--------------|---------------|-------------|-----|
| vCPU         | 2             | 4           | training spikes are CPU-bound |
| RAM          | 4 GB          | 8 GB        | LightGBM/XGBoost feature builds |
| Disk         | 50 GB SSD     | 100 GB SSD  | DB ≈ 3 GB now, parquet ≈ 5 GB, headroom for 7-day local backups |
| Bandwidth    | -             | -           | The Odds API calls are tiny |

The DB grows ~30 MB/day at the current ingest rate. Plan disk so you can run
12–18 months without resizing.

## Prerequisites

Per host:
- Docker Engine 24+ and the Compose v2 plugin
- DNS `A`/`AAAA` records:
  - `${APP_DOMAIN}` → host public IP
  - `${API_DOMAIN}` → host public IP
- Inbound TCP 80/443 open (Caddy needs both for HTTP-01 / HTTPS)
- An S3-compatible bucket with a dedicated access key (read+write)

Off-host, before you start:
- Clerk app provisioned (production instance)
- Stripe products + webhook signing secret (only if launching paid tiers)

## Bring-up — clean machine

```bash
git clone <repo-url> sports && cd sports
cp .env.example .env
$EDITOR .env                 # fill in REQUIRED values
docker compose pull caddy    # warm the proxy image
docker compose build
```

Restore the warehouse and model artifacts before starting the service stack.
The one-shot restore creates the named volumes and writes as the same `app`
user that runs cron jobs. [Backup and restore](BACKUP_RESTORE.md)

```bash
docker compose run --rm --no-deps --user app --entrypoint /app/scripts/restore_from_s3.sh worker --force
```

Scheduled `predict` and `train` jobs skip broad Odds API odds and scores by
default, even with provider credentials installed. Set
`WORKER_LIVE_ODDS_JOBS=train` in `.env` for one broad refresh per day at the
default cron. Set `WORKER_TRAIN_CRON` in UTC to match the intended collection
window, including Eastern daylight-saving changes. Adding `predict` also
enables the hourly refresh, for 25 broad passes per day at the default crons.
Budget for both markets per available league and scores. The paper reserve
and request caps cover supplemental collection only.
[Commercial cost assumptions](COMMERCIAL_VALIDATION.md)

```bash
docker compose up -d api worker
```

The default readiness window for odds is three hours. With one daily
refresh, start the dependent services while the required leagues have
recent priced receipts. If the warehouse needs a fresh receipt, wait for
an opted-in broad job or run one manual refresh in its place. The command
below fetches live odds and scores. Count it against the same daily budget
and run it only on the collection owner.

```bash
docker compose run --rm --no-deps --user app --entrypoint /app/scripts/pipeline.sh worker --skip-training
```

Once the API passes `/ready`, start the web app and proxy. The proxy binds
ports 80 and 443 and requests certificates once DNS resolves.

```bash
docker compose exec api curl -fsS http://localhost:8000/ready
docker compose up -d web proxy
curl -fsS https://${API_DOMAIN}/health
curl -fsS https://${APP_DOMAIN}/ | head
docker compose exec worker cat /etc/cron.d/sports
```

When moving the paper trial to this worker, pause the Windows prediction,
fixture discovery, early entry and closing jobs before taking the warehouse
snapshot. Restore it and the pinned model, opt in to one broad job with
`WORKER_LIVE_ODDS_JOBS=train`, and set `WORKER_PAPER_CLOSE_ENABLED`,
`WORKER_PAPER_ENTRY_ENABLED` and `WORKER_PAPER_EVENTS_ENABLED` to `1` in `.env`
on one worker. Closing checks start on each UTC quarter hour. Early entry and
fixture discovery checks start five and ten minutes later. Each job writes
its own log and health heartbeat. The independently enabled supplemental jobs
use the same trial cutoffs and caps on both platforms. Entry reserves 200
credits and can spend up to two credits per Eastern date. Closing reserves
200 and can spend up to three credits per UTC date. Discovery uses the
zero-credit events endpoint.

## Rebuild / redeploy

Code-only changes (no schema or volume changes):
```bash
git pull
docker compose build api web worker
docker compose up -d api web worker
```

Compose recreates containers but keeps volumes, so DB and models survive.

If the rebuild was bad:
```bash
git checkout <previous-good-sha>
docker compose build api web worker
docker compose up -d api web worker
```

See [docs/RUNBOOK.md](RUNBOOK.md) for the full rollback procedure.

## Configuration model

| Layer       | File                          | Audience                             |
|-------------|-------------------------------|--------------------------------------|
| Secrets     | `.env`                        | host operator, never committed       |
| Public web  | build-args in `docker-compose.yml` | bake into Next.js bundle at build |
| Proxy       | `Caddyfile` + `${APP,API}_DOMAIN`  | TLS + vhost split                |
| Worker      | `WORKER_*_CRON` in `.env`     | controls schedules without rebuilds  |

`NEXT_PUBLIC_*` variables ship in the JavaScript bundle. Changing them
requires `docker compose build web`. Server-only secrets (Clerk secret key,
Stripe secret key) are read at runtime from `.env`.

Set the same `STRIPE_ACCOUNT_ID`, `STRIPE_PRICE_ID`, and Stripe key mode
in the web and API environments. Checkout and entitlement reconciliation
verify the provider account against `STRIPE_ACCOUNT_ID`. The subscriber
gate checks that Clerk's verified billing record matches the account, key
mode, price, and canonical customer. A legacy premium flag requires provider
reconciliation before it grants access. [Stripe API keys](https://docs.stripe.com/keys)
· [Clerk metadata](https://clerk.com/docs/guides/users/extending)

Run paid test drills with a separate Clerk instance. Use the same production
Clerk instance for the no-charge pilot and paid site so a signed-in pilot
participant retains their account. Before changing a live Stripe account or
price, close enrollment and reconcile existing active subscriptions in the
old configuration.

`PAID_ENROLLMENT_ENABLED` defaults to `false`. After verifying the paid launch
checklist, set it to `true` in the API environment and recreate the API service.
The endpoint for starting new Checkout sessions still requires a passing publisher
run within 30 hours and at least one genuine pick issued by a currently passing
rule in the past seven days. A clean publisher run with zero qualifying bets can
still satisfy the first condition. Revoked releases, failed runs, and voided picks
cannot satisfy the relevant checks. Quote expiry still controls each pick shown
to subscribers. Set the flag back to `false` and recreate the API service to close
new enrollment.

Full-stack Compose mounts the private `sports_checkout` volume at
`/app/checkout-state`. Before the first paid enrollment, provision the SQLite
file once as the web service user. The web route requires the file to exist
and stops checkout if it is missing. An empty directory after a restart or
restore requires investigation before checkout can resume.

```bash
docker compose exec web node --input-type=module -e '
import { closeSync, openSync } from "node:fs";
import { DatabaseSync } from "node:sqlite";
const path = process.env.SPORTS_CHECKOUT_DB_PATH;
closeSync(openSync(path, "wx", 0o600));
const db = new DatabaseSync(path);
db.exec("PRAGMA user_version = 1");
db.close();'
```

Keep the volume across redeployments and include it in the private restore
drill. Use a consistent SQLite backup of the live database. Close enrollment
before any restore or rollback. Reconcile Stripe and Clerk activity after the
backup cutoff with the restored reservations before reopening. Disabling the
app route stops new sessions. Expire existing open Stripe sessions if payment
must stop immediately. Web processes on one host share this file. A second
host needs an equivalent shared transactional store before accepting checkout
traffic.

The store saves Stripe resource IDs, the Clerk account ID, and the exact
server-created requests. Stripe collects the billing email for new customers.
Checkout retries use the same saved idempotency key. Unresolved requests
that reach 23 hours require reconciliation in Stripe before an operator
can recover checkout. Provider-confirmed terminal sessions can be replaced.
Keep Stripe's single-subscription setting enabled as a separate control.
[Stripe idempotent requests](https://docs.stripe.com/api/idempotent_requests) · [Stripe Checkout settings](https://docs.stripe.com/payments/checkout/limit-subscriptions)

New Checkout sessions return to `/account` with the Stripe session ID. The
signed-in account can confirm a paid session and recover access while webhook
delivery is pending. The same reconciliation handles webhook updates, including
later cancellations. During the test-mode drill, complete Checkout with webhook
delivery paused, confirm access on the return page, then resume delivery and
verify a cancellation removes access. [Stripe Checkout fulfillment](https://docs.stripe.com/checkout/fulfillment?ui=full-page)

## Networking

- All services share the `sports` Compose network.
- Only `proxy` publishes ports to the host.
- `web` calls `api` over the internal network at `http://api:8000` for
  server-side fetches (set `NEXT_PUBLIC_API_URL` to the public domain so
  client-side fetches go through Caddy).
- The API's CORS allowlist is enforced by FastAPI (`API_CORS_ORIGINS`),
  not Caddy. Caddy is intentionally CORS-blind.

## Persistent storage

| Volume          | Mount path      | Owner mode | Read/write |
|-----------------|-----------------|------------|------------|
| `sports_data`   | `/app/data`     | app:app    | api: ro · worker: rw |
| `sports_models` | `/app/models`   | app:app    | api: ro · worker: rw |
| `sports_logs`   | `/app/logs`     | app:app    | rw          |
| `sports_backups`| `/app/backups`  | app:app    | worker only |
| `caddy_data`    | `/data` (Caddy) | caddy      | proxy only — holds LE certs |

Back up the host volumes with `docker volume inspect` to find the bind path
on disk and snapshot it, OR rely on the worker's `/app/scripts/backup_to_s3.sh`
which is the supported path. See [docs/BACKUP_RESTORE.md](BACKUP_RESTORE.md).

## Optional: standalone Next.js output

To shrink the web image (~70%) enable `output: "standalone"` in
`web-app/next.config.ts` and switch `web-app/Dockerfile`'s runner stage to
copy `.next/standalone` and run `node server.js`. The current Dockerfile is
deliberately conservative — it works regardless of that toggle.

## What's NOT covered here

- **Multi-host / HA**: the SQLite-on-disk choice rules this out for v1.
  See `docs/ARCHITECTURE.md` for the path forward (Postgres + WAL replicas).
- **Blue/green**: the redeploy is in-place. Rollback is by SHA, not by slot.
- **Observability**: the API exposes `/health`. Wire up uptime monitoring
  (Better Uptime, UptimeRobot) against `https://${API_DOMAIN}/health` —
  that's the launch-day minimum.
