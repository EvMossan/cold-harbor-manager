# Infrastructure and Deployment

## Cloudflare tunnels

`entrypoint.sh` (used by Docker, Cloud Run, and local launches) brings up
Cloudflare Access TCP tunnels before handing control to `gunicorn`. It reads
`CF_ACCESS_CLIENT_ID`/`CF_ACCESS_CLIENT_SECRET`, spawns `cloudflared access
tcp` processes, and exposes the tunneled Postgres/Timescale ports locally.
See `entrypoint.sh` for the exact CLI arguments.

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

## Configuration

For a complete list of environment variables and their purposes, please refer to the [Operations Guide](operations_guide.md#environment-variables).

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

The Airflow log server hostname defaults to `localhost` so that the API
and scheduler can reach the local webserver. Override it in Docker or
Cloud Run by setting `AIRFLOW_LOG_SERVER_HOST` when a different host is
required.
