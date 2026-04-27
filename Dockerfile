# syntax=docker/dockerfile:1.6
#
# Python image for the FastAPI service and the worker.
# Both services run from the same image; the compose file picks the entrypoint.
#
# Stage 1: build the poetry virtualenv against /opt/venv so we can copy it
# into the slim runtime stage without dragging poetry along.

ARG PYTHON_VERSION=3.11-slim-bookworm

FROM python:${PYTHON_VERSION} AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    POETRY_VERSION=1.8.3 \
    POETRY_HOME=/opt/poetry \
    POETRY_VIRTUALENVS_CREATE=true \
    POETRY_VIRTUALENVS_IN_PROJECT=false \
    POETRY_NO_INTERACTION=1 \
    VIRTUAL_ENV=/opt/venv

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        build-essential \
        curl \
        libgomp1 \
 && rm -rf /var/lib/apt/lists/*

RUN curl -sSL https://install.python-poetry.org | python3 - \
 && ln -s /opt/poetry/bin/poetry /usr/local/bin/poetry

WORKDIR /app

COPY pyproject.toml poetry.lock ./

RUN python -m venv "${VIRTUAL_ENV}" \
 && poetry config virtualenvs.path /opt \
 && VIRTUAL_ENV="${VIRTUAL_ENV}" poetry install --only main --no-root --no-ansi


FROM python:${PYTHON_VERSION} AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    VIRTUAL_ENV=/opt/venv \
    PATH="/opt/venv/bin:/usr/local/bin:/usr/bin:/bin"

# Runtime libs only — keep the image small.
# - libgomp1: lightgbm/xgboost
# - tini: PID 1 + signal forwarding
# - cron: worker scheduler
# - curl, ca-certificates: healthchecks + S3 backups
# - awscli (via pip, see below): external object-storage backups
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        cron \
        libgomp1 \
        tini \
        tzdata \
 && rm -rf /var/lib/apt/lists/*

# Dedicated non-root user for the API. The worker container overrides to root
# because cron requires it; the worker still drops to `app` when running jobs.
RUN groupadd --system --gid 1001 app \
 && useradd  --system --uid 1001 --gid app --home-dir /app --shell /usr/sbin/nologin app

COPY --from=builder /opt/venv /opt/venv

# awscli is intentionally a separate layer so the heavy poetry deps don't
# get rebuilt every time we bump the cli.
RUN pip install --no-cache-dir "awscli==1.34.30"

WORKDIR /app

# Copy only what the runtime needs. Tests, notebooks, etc. are excluded by
# .dockerignore.
COPY src ./src
COPY scripts ./scripts
COPY config ./config
COPY pyproject.toml poetry.lock README.md ./

# Ensure shell scripts are executable inside the image regardless of the host
# filesystem's exec bit (matters on Windows hosts).
RUN find ./scripts -type f -name "*.sh" -exec chmod +x {} \;

# Runtime data lives under /app/data, /app/models, /app/logs, /app/backups.
# Compose mounts persistent volumes here. Pre-create them so the image works
# even if a volume is missing (e.g. local one-shot runs).
RUN mkdir -p /app/data /app/models /app/logs /app/backups \
 && chown -R app:app /app

USER app

ENV SPORTS_DB_PATH=/app/data/betting.db \
    MODELS_DIR=/app/models

EXPOSE 8000

ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["uvicorn", "src.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
