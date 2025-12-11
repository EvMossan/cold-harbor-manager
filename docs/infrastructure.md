# Infrastructure and Deployment

## Cloudflare tunnels

`entrypoint.sh` (used by Docker, Cloud Run, and local launches) starts
Cloudflare Access TCP tunnels before handing control to `gunicorn`. When
`CF_ACCESS_CLIENT_ID` and `CF_ACCESS_CLIENT_SECRET` are present, it
launches two background `cloudflared access tcp` commands that
terminate on `localhost:15433` and `localhost:15434`. The `--hostname`
flags default to `dbtunnel-pg.cold-harbor.org` (Postgres) and
`dbtunnel-ts.cold-harbor.org` (Timescale), but can be overridden. The
app then connects to the databases via `POSTGRESQL_LIVE_CONN_STRING` and
`TIMESCALE_LIVE_CONN_STRING`, which point at the local tunnel ports so the
databases stay hidden behind Cloudflare Access.

## CI/CD flow

`cloudbuild.yaml` defines a three-step pipeline:

1. `docker build` – builds `gcr.io/$PROJECT_ID/cold-harbour-web:$COMMIT_SHA`.
2. `docker push` – pushes the image to Artifact Registry so Cloud Run can
   pull it.
3. `gcloud run deploy` – deploys the image to Cloud Run with `--allow-unauthenticated`.

During the deploy step, Cloud Build injects secrets from Secret Manager
into the Cloud Run service using `--set-secrets`. The pipeline wires the
same secret names used in the codebase (`POSTGRESQL_LIVE_CONN_STRING`,
`TIMESCALE_LIVE_CONN_STRING`, `CF_ACCESS_CLIENT_ID`,
and `CF_ACCESS_CLIENT_SECRET`).

## Critical environment variables

| Variable | Purpose |
|----------|---------|
| `CF_ACCESS_CLIENT_ID` | Cloudflare Access service token ID used by `entrypoint.sh`. |
| `CF_ACCESS_CLIENT_SECRET` | Cloudflare Access service token secret. |
| `POSTGRESQL_LIVE_CONN_STRING` | DSN (SQLAlchemy URL or libpq) for the live Postgres tunnel on port 15433. |
| `TIMESCALE_LIVE_CONN_STRING` | DSN for Timescale (defaults to tunnel on port 15434). |
| `WEB_RELOAD` | When `1`, `entrypoint.sh` passes `--reload` to `gunicorn` for local tweaks. |
| `ALPACA_*` vars | Client/secret/base_url per destination managed through `core.destinations`. |
| `ACCOUNT_SCHEMA` | Optional schema prefix used by both the manager and web layer. |

Keeping these variables aligned in Secret Manager and `.env` ensures
the tunnels, database connections, and runtime knobs remain synchronized
between local Docker Compose and Cloud Run environments.

## Airflow & Risk Orchestration

The stack now includes Apache Airflow to handle periodic tasks (Risk
Manager):

| Service | Description |
|---------|-------------|
| `airflow-ch-mgr` | Runs the Airflow Scheduler, Webserver, and Triggerer. It mounts the `dags/` directory to execute risk logic. |
| `postgres-airflow-mgr` | Dedicated PostgreSQL instance (port 5435) for Airflow's internal metadata state. |

The DAGs in `dags/breakeven_multi_account.py` dynamically import account
configurations from `src/coldharbour_manager/core/destinations.py` to
spawn parallel risk-management tasks for every active destination.
