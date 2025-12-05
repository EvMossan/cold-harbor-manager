#!/bin/sh
#!/bin/sh
# Wrapper entrypoint that starts Cloudflare Access TCP tunnels for Postgres/Timescale
# and then launches the web app. It expects Cloudflare service token credentials
# in CF_ACCESS_CLIENT_ID / CF_ACCESS_CLIENT_SECRET and optional hostnames in
# CF_PG_HOSTNAME / CF_TS_HOSTNAME. Used both locally and in Cloud Run to avoid
# exposing the databases.
set -e

# Defaults for tunnel hostnames (must be provided via env in prod)
CF_PG_HOSTNAME="${CF_PG_HOSTNAME:-postgres.example.com}"
CF_TS_HOSTNAME="${CF_TS_HOSTNAME:-timescale.example.com}"

# Start Cloudflare tunnels if credentials are provided
if [ -n "$CF_ACCESS_CLIENT_ID" ] && [ -n "$CF_ACCESS_CLIENT_SECRET" ]; then
  cloudflared access tcp \
    --hostname "$CF_PG_HOSTNAME" \
    --url tcp://0.0.0.0:15433 \
    --service-token-id "$CF_ACCESS_CLIENT_ID" \
    --service-token-secret "$CF_ACCESS_CLIENT_SECRET" &
  CF_PG_PID=$!

  cloudflared access tcp \
    --hostname "$CF_TS_HOSTNAME" \
    --url tcp://0.0.0.0:15434 \
    --service-token-id "$CF_ACCESS_CLIENT_ID" \
    --service-token-secret "$CF_ACCESS_CLIENT_SECRET" &
  CF_TS_PID=$!
else
  echo "Warning: CF_ACCESS_CLIENT_ID/SECRET not set; skipping cloudflared tunnels" >&2
fi

# Give tunnels a moment to start
sleep 2

# Run gunicorn (Cloud Run provides $PORT)
RELOAD_FLAG=""
if [ "${WEB_RELOAD:-0}" = "1" ]; then
  RELOAD_FLAG="--reload"
fi

exec gunicorn ${RELOAD_FLAG} -k gevent -w 1 -b "0.0.0.0:${PORT:-5000}" "cold_harbour:create_app()"
