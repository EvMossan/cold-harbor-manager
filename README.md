# Cold Harbour Web

High-frequency monitoring and dashboarding for each trading destination with a lightweight Flask UI, async Alpaca account manager, and secure access to Postgres/Timescale data via Cloudflare Access tunnels.

## Table of Contents

- [Architecture](#architecture)
- [Repository layout](#repository-layout)
- [Getting started](#getting-started)
- [Configuration](#configuration)
- [Running the application](#running-the-application)
- [Deployment](#deployment)
- [Monitoring and maintenance](#monitoring-and-maintenance)
- [Contributing](#contributing)

## Architecture

- **Web dashboard** – `cold_harbour.create_app()` wires the `accounts`
  blueprint (`src/cold_harbour/web/routes.py`) into Flask, serves
  `account_positions.html`, and streams updates over SSE.
- **Account manager** – `AccountManager` (async) keeps databases, Alpaca
  REST/ZMQ streams, and in-flight equity/time-series data in sync for
  trading logic.
- **Data Ingester** – A standalone service
  (`src/cold_harbour/services/activity_ingester`) that acts as a raw data
  lake. It ingests orders and activities via Stream and REST (with
  self-healing capabilities) into the `account_activities` schema.
- **Data layer** – PostgreSQL + Timescale hosts positions/equity tables.
  `infrastructure/db.py` wraps `asyncpg` pools, and helper modules
  (`core.account_utils`, `core.equity`, `core.market_schedule`) provide
  order/market logic.
- **Security tunnel** – `entrypoint.sh` launches `cloudflared access tcp`
  tunnels for Postgres/Timescale before `gunicorn`, so local dev and Cloud
  Run deployments reuse the same encrypted path controlled by
  `CF_ACCESS_*` secrets.

## Documentation

- [Web Layer Architecture](docs/web_architecture.md) – explains how the Flask
  blueprint per destination exposes SSE streams, cached APIs, and the `/stream/events`
  flow driven by PostgreSQL `LISTEN`/`NOTIFY`.
- [Core Analytics Reference](docs/core_analytics.md) – documents the lot/FIFO
  engines, orphan/chain logic, and equity Smart Sharpe/drawdown computations.
- [Infrastructure and Deployment](docs/infrastructure.md) – covers the Cloudflare
  tunnels, Cloud Build pipeline, and required environment secrets.

## Repository layout

- `src/cold_harbour/` – application code (web layer, services, core helpers, infrastructure glue).
- `docker-compose.yml` – quick dev stack with watch-and-reload manager/web services wired to the local `src/`.
- `Dockerfile`, `cloudbuild.yaml` – build artifacts that install dependencies from `requirements.txt` and run the app via `entrypoint.sh`.
- `entrypoint.sh` – Cloudflare tunnel bootstrapper + gunicorn runner; respects `WEB_RELOAD` and defaults to port `5000`.
- `.env` (template kept out of git via `.gcloudignore`) – sample environment variables for DB DSNs, Alpaca credentials, Cloudflare tokens, and runtime thresholds.
- `docs/` – placeholder for architecture diagrams, runbooks, or ancillary docs.
- `CONTRIBUTING.md` – contribution guidelines and workflow expectations.

## Getting started

### Prerequisites

- Docker & Docker Compose (for containerized dev).
- Python 3.11+ virtual environment when working directly with `requirements.txt`.
- Access to the same Postgres/Timescale databases tunneled via Cloudflare Access and Alpaca API credentials per destination.

### Environment

1. Copy `.env` (not checked in) and populate:
   - `POSTGRESQL_LIVE_CONN_STRING` / `TIMESCALE_LIVE_CONN_STRING` for Cloudflare tunnel endpoints (`127.0.0.1:15433/15434`).
   - Optional `_SQLALCHEMY` variants for SQLAlchemy-based access.
   - `CF_ACCESS_CLIENT_ID` / `CF_ACCESS_CLIENT_SECRET` to launch tunnels.
   - `ALPACA_*` pairs referenced by `core.destinations` entries.
   - Tweak `ACCOUNT_SCHEMA`, `HEARTBEAT_SEC`, `FLUSH_INTERVAL_S`, and other runtime knobs as needed.
2. Inspect `core/destinations.py` to understand destination-specific table/notify name generation and add `destinations_local.py` for local-only entries if required.

### Local dev with Docker Compose

```sh
docker compose up --build
```

- `manager` service runs `python -m cold_harbour.manager_run` with live reload.
- `ingester` service runs the raw data collection layer (`activity_ingester.ingester_run`).
- `web` service starts Flask via `entrypoint.sh` on port `5000`.

All services inherit `.env`, mount `src/`, and reuse the same tunnelling
strategy so you get parity with Cloud Run.

## Configuration

- **Database connections** – all Postgres/Timescale DSNs support either SQLAlchemy URLs (`postgresql+psycopg2://…`) or libpq-style DSNs (`host=… port=…`); helpers transparently normalize them to whichever client is starting.
- **Alpaca destinations** – `core.destinations.DESTINATIONS` defines `key_id`, `secret_key`, `base_url`, and optional `initial_deposit`. The slug derived from the destination name governs every table (`open_trades_<slug>`, `closed_trades_<slug>`, `equity_full_<slug>`) and NOTIFY channel (`pos_channel_<slug>`, `closed_channel_<slug>`).
- **Runtime knobs** – `AccountManager` reads `HEARTBEAT_SEC`, `SNAPSHOT_SEC`, `UI_SNAPSHOT_SEC`, `FLUSH_INTERVAL_S`, `CLOSED_SYNC_SEC`, etc., from the environment; default values mirror the previous implementation and can be overridden to tune responsiveness.
- **Cloudflare tunnels** – `CF_ACCESS_CLIENT_ID`/`_SECRET` are mandatory for production; without them, `entrypoint.sh` warns and skips tunnels, so ensure Cloud Run or local dev injects them.

## Running the application

- **Docker build/test image**:

  ```sh
  docker build -t cold-harbour/web .
  ```

  This installs dependencies from `requirements.txt`, copies the source tree, and relies on `entrypoint.sh` to boot `gunicorn` with gevent workers. `PORT` defaults to `5000`.

- **Standalone web server**:

  ```py
  python -m cold_harbour.app run_web
  ```

  This launches Flask in debug-off mode on `0.0.0.0:${PORT:-5000}`.

- **Async AccountManager (multi-account)**:

  ```sh
  python -m cold_harbour.manager_run
  ```

  or instantiate `AccountManager` with `cold_harbour.app.run_manager(cfg)` when embedding in other scripts. The manager opens ZMQ price streams, polls Alpaca orders, and streams position/equity updates via PostgreSQL `NOTIFY`.

- **Data Ingester Service**:

  ```sh
  python -m cold_harbour.services.activity_ingester.ingester_run
  ```

  Launches the background service that captures raw orders and activities
  into the `account_activities` schema for all configured destinations.

- **Web UI** – SSE endpoints are anchored to `accounts_bp` (`accounts`, `pos`, `closed` channels). Toggle `DISABLE_SSE=True` in the environment to fall back to polling for debugging.

## Deployment

- **Secrets** – `cloudbuild.yaml` references:

  - `POSTGRESQL_LIVE_CONN_STRING` / `_SQLALCHEMY`
  - `TIMESCALE_LIVE_CONN_STRING` / `_SQLALCHEMY`
  - `CF_ACCESS_CLIENT_ID` / `CF_ACCESS_CLIENT_SECRET`

  Store these in Secret Manager using the names above and grant the Cloud Build service account the Secret Accessor role. Cloud Run receives the same secrets via `--set-secrets` and respects the `PORT` it exposes.

- **Cloud Build command** – submit builds with:

  ```sh
  gcloud builds submit \
    --config cloudbuild.yaml \
    --substitutions=_SERVICE_NAME=cold-harbour-web,_REGION=europe-north1,_PROJECT_NUMBER=YOUR_REAL_PROJECT_NUMBER,COMMIT_SHA=$(git rev-parse HEAD)
  ```

  The pipeline builds the image, pushes to `gcr.io/$PROJECT_ID/<service>`, and deploys to Cloud Run. `_PROJECT_NUMBER` resolves the canonical secret resource names referenced in the file.

- **Cloud Run runtime** – the deployed container runs `entrypoint.sh`, which launches Cloudflare tunnels before `gunicorn`. Ensure Cloud Run has access to the same secrets and that the `PORT` environment variable (Cloud Run provides it automatically) is not overridden.

## Monitoring and maintenance

- Inspect Cloud Run revisions and logs via `gcloud run services describe <service> --region europe-north1` and `gcloud run services logs read <service> --region europe-north1`.
- The UI heartbeat interval defaults to `30s` but is configurable via `HEARTBEAT_SEC` or `EQUITY_UPDATE_INTERVAL_S`; `entrypoint.sh` logs a warning when Cloudflare tokens are absent.
- Rebuild equity series or refill historical snapshots with helpers in `core.equity` and `core.account_start`.
- Keep `requirements.txt` aligned with prod; pip freeze before releasing large changes.

## Contributing

- See [`CONTRIBUTING.md`](CONTRIBUTING.md) for branch/PR expectations.
- Use `docs/` for any architecture diagrams or runbooks that explain complex flows beyond the README.
