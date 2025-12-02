# Operations Guide

This guide captures the knobs operators and traders adjust when onboarding
new destinations or managing the runtime infrastructure.

## Adding a new destination

1. Copy one of the dictionaries in `src/cold_harbour/core/destinations.py`
   and update fields:
   - `name`: the human-friendly identifier shown in UI tabs.
   - `base_url`, `key_id`, `secret_key`: point at the Alpaca endpoints for
     that destination. Use environment variables such as
     `ALPACA_BASE_URL_<NAME>` / `ALPACA_API_KEY_<NAME>` /
     `ALPACA_SECRET_KEY_<NAME>` so the credentials stay secret.
   - `risk_factor`, `orders_table`, and `trained_classifier_path`: tweak
     trading configuration hooks if the bot must use a different model or
     dataset (the latter is a path to your external ML artefact that the
     manager may consult before sending signals).
2. `slug_for` derives the per-destination slug by lower-casing `name`,
   replacing any character outside `[a-z0-9_]` with `_`, collapsing
   repeated underscores, stripping edges, and prefixing `d_` if the slug
   starts with a digit. This slug names the tables and notify channels
   (e.g., `closed_trades_<slug>`, `pos_channel_<slug>`).
3. After adding the dictionary, restart the manager/web services so the
   new blueprint can register and the tables `accounts.open_trades_<slug>`
   / `accounts.closed_trades_<slug>` / `account_activities.raw_*_<slug>`
   are created via the respective services.

## Environment variables

| Variable | Purpose |
|----------|---------|
| `ALPACA_BASE_URL_<NAME>` | Alpaca REST/WebSocket base URL for that destination (`live` or `paper`). |
| `ALPACA_API_KEY_<NAME>` | A key matching the destination name; used to populate `key_id`. |
| `ALPACA_SECRET_KEY_<NAME>` | Secret paired with the API key (`secret_key`). |
| `CF_ACCESS_CLIENT_ID`, `CF_ACCESS_CLIENT_SECRET` | Cloudflare Access tokens consumed by `entrypoint.sh` to spawn `cloudflared access tcp` tunnels (ports 15433/15434). |
| `POSTGRESQL_LIVE_CONN_STRING` / `TIMESCALE_LIVE_CONN_STRING` | DSNs pointing at the tunneled Postgres/Timescale endpoints; either libpq-style or SQLAlchemy URLs are accepted. |
| `WEB_RELOAD` | When `1`, the web service runs `gunicorn --reload` for template tuning. |
| `ACCOUNT_SCHEMA` | Optional schema prefix (default `accounts`) shared across manager/web/ingester migrations. |
| `TRAINED_CLASSIFIER_PATH` *(if used)* | Override or extend the `trained_classifier_path` defined per destination so the manager can import a new model artifact before trading. |

## Database management

`src/cold_harbour/infrastructure/db.py` contains the async helpers used by
magagers to fetch/exec statements. `AsyncAccountRepository.create` accepts
both SQLAlchemy URLs (e.g. `postgresql+psycopg2://...`) and libpq DSNs
(`host=… user=…`). Helpers `_url_from_sqlalchemy` and `_url_from_libpq`
strip the driver suffix or parse key/value pairs to produce an `asyncpg`
compatible URL. That means the same environment variable (`*_CONN_STRING`)
can be used by synchronous and asynchronous services without duplication.

When the tunnels from `entrypoint.sh` expose 15433/15434 locally, those
DSNs should point at `localhost:15433` (Postgres) and
`localhost:15434` (Timescale) so the services can connect securely even
inside Cloud Run or Docker containers.
