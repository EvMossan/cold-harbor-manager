## Quick checklist for the deployment
1) Build your own Dockerfile for the Flask app (copy code, install deps, expose `$PORT`, run gunicorn). Do **not** expose DB ports.
2) Add `cloudflared` to the image or run it before the app: it must open two tunnels to `dbtunnel-pg.cold-harbor.org` (→15433) and `dbtunnel-ts.cold-harbor.org` (→15434) using `CF_ACCESS_CLIENT_ID/SECRET`.
3) In `.env` set DB strings to `127.0.0.1:15433/15434` with `sslmode=disable`, plus the Cloudflare token envs. Keep real tokens out of git.
4) there is the launcher `entrypoint.sh` that starts both tunnels and then gunicorn; you can reuse or adapt it in your Dockerfile/entrypoint instead of writing the commands inline.
5) Local test: `docker build`, `docker run -p 5001:5000 --env-file .env ...`, open `http://localhost:5001/`, verify data loads.
6) Cloud Run: keep DB strings + CF token in Google Secret Manager and inject as env vars; deploy the image; ensure the entrypoint (or a sidecar) starts `cloudflared` before the app.
