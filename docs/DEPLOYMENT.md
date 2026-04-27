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
docker compose build         # ~10 min on a 4 vCPU host first time
docker compose up -d proxy api web
docker compose logs -f api   # watch /health flip to 200
```

The first `docker compose up -d` will:
- create the `sports_data`, `sports_models`, `sports_logs`, `sports_backups`
  volumes
- bind 80/443 on the host
- request Let's Encrypt certs once DNS resolves

Once the API is healthy:

```bash
# Restore DB + models from S3 BEFORE starting the worker so we don't
# overwrite a fresh empty DB.
docker compose run --rm worker /app/scripts/restore_from_s3.sh --force
docker compose up -d worker
```

Verify:
```bash
curl -fsS https://${API_DOMAIN}/health
curl -fsS https://${APP_DOMAIN}/ | head
docker compose exec worker crontab -l -u app
```

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
