## Quick local checklist
1) Build the Docker image (`docker build .`) from the repo root so the application already packages its dependencies and `cloudflared`.
2) Run the container with your `.env` file so `CF_ACCESS_*` credentials and the 127.0.0.1 DB tunnel addresses are injected, for example `docker run -p 5001:5000 --env-file .env ...`.
3) Point a browser to `http://localhost:5001/` and confirm that the Flask UI loads data through the tunnels managed by the included `entrypoint.sh`.

## Google Cloud deployment
### Secret Manager setup
Store each sensitive value in Secret Manager so Cloud Build and Cloud Run can inject them securely (names match `cloudbuild.yaml` and the console screenshot):

- `POSTGRESQL_LIVE_CONN_STRING` -> Postgres connection string that uses `127.0.0.1:15433` (e.g., `postgresql://user:pass@127.0.0.1:15433/my-db?sslmode=disable`).
- `TIMESCALE_LIVE_CONN_STRING` -> Timescale string that uses `127.0.0.1:15434` with `sslmode=disable`.
- `CF_ACCESS_CLIENT_ID` and `CF_ACCESS_CLIENT_SECRET` -> Cloudflare service token credentials required by `cloudflared`.

Use automatic replication, and grant the Cloud Build service account the Secret Accessor role so the pipeline can read the latest versions.

Note that `gcloud run` requires the canonical resource name when referencing secrets, so the pipeline uses the numeric `_PROJECT_NUMBER` substitution (e.g., `471500379526`) when building `projects/.../secrets/...` strings; this avoids the “invalid secret name” failure that occurs when the hyphenated project ID is supplied. Cloud Run already injects a `PORT` env var, so we no longer try to override it from the pipeline.

### Cloud Build + Cloud Run pipeline
The included `cloudbuild.yaml` builds the Docker image, pushes it to `gcr.io/$PROJECT_ID/<service>` and deploys it to Cloud Run with the secrets injected as environment variables.

1. Enable the Cloud Run, Cloud Build, and Secret Manager APIs.
2. Submit the build with any desired overrides (service name, region, secret IDs):

   ```sh
   gcloud builds submit \
     --config=cloudbuild.yaml \
     --substitutions=_SERVICE_NAME=my-service,_REGION=us-central1,_PROJECT_NUMBER=471500379526
   ```

   The defaults match the Secret Manager names above (`POSTGRESQL_LIVE_CONN_STRING`, `TIMESCALE_LIVE_CONN_STRING`, `CF_ACCESS_CLIENT_ID`, `CF_ACCESS_CLIENT_SECRET`) and a numeric secret project identifier. Customize `_PROJECT_NUMBER` and the `_DB_*`/`_CF_*` substitutions only if your project or secret names differ.
3. The final step in `cloudbuild.yaml` calls `gcloud run deploy` and attaches the secrets via `--set-secrets` so Cloud Run has the same runtime expectations as local Docker.

### Runtime tips
Cloud Run forwards a `$PORT` env var (default `5000` in the Dockerfile/entrypoint) so the container remains compatible. The entrypoint also starts the two `cloudflared access tcp` tunnels before running `gunicorn`, so there is no need for extra sidecars. After deployment, verify the revision with `gcloud run services describe my-service --region us-central1` and read logs via `gcloud run services logs read my-service`.
