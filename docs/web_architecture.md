# Web Layer Architecture

The Flask UI lives under `src/cold_harbour/web/routes.py` and builds a
per-destination blueprint via `_make_blueprint_for_dest`. Each entry in
`core.destinations.DESTINATIONS` gets a slugified prefix (e.g.
`/demo/`) so the same `account_positions.html` template can be rendered for
any account. The blueprint attaches hooks to log timings, relax the CSP,
and expose both HTML views and JSON/SSE endpoints that feed the dashboard.

## Blueprint per destination

`_make_blueprint_for_dest` wires a `Blueprint` at `/<slug>` and reuses
`account_table_names` to resolve table names (open, closed, equity,
metrics, notify channels, etc.). Tables derived from `equity_full_*` also
create matching `equity_intraday_*` and `cash_flows_*` names so the same
logic can read intraday series without manual configuration.

## API reference

All API routes live inside the per-account blueprint (e.g.
`/demo/api/equity`). Key routes include:

- `/api/positions_cached` – returns open positions with an ETag built
  from `_open_version()` (max `updated_at` + row count) and `Cache-Control`
  set to `public, max-age=30`. If the client sends `If-None-Match`, the
  handler short-circuits with `304` to avoid hitting Postgres.
- `/api/equity` – reads `tables['equity']` and accepts a `window`
  parameter so the UI can request `all`, `first_trade` (default), or `Nd`
  (e.g. `365d`). The handler also keeps a TTL cache in `_cache` per window
  key so repeated opens within ~45 seconds return the cached payload.
- `/api/equity_intraday` – streams minute-resolution equity series. Returns
  a JSON object `{ "series": [...], "meta": {...} }`. The `meta` block
  contains critical session anchors (`baseline_deposit`, `session_flows`,
  `live_deposit`) which the frontend uses to calculate real-time gains
  relative to the session open.
- `/api/metrics` – returns the most recent JSON blob from `tables['metrics']`
  (defaulting to `account_metrics`) so the UI can show aggregated KPIs.

All JSON endpoints serialize with `pd.DataFrame.to_json(..., date_format='iso')`
so timestamps stay ISO-formatted for the browser.

## Streaming (SSE)

The `/stream/events`, `/stream/positions`, and `/stream/closed`
endpoints use raw `psycopg2` connections to PostgreSQL with
`LISTEN`/`NOTIFY`. The `/stream/events` endpoint multiplexes updates
from two PostgreSQL channels: `pos` (for open position upserts/deletes)
and `closed` (signals to refresh the closed trades table). Each event
stream sets `ISOLATION_LEVEL_AUTOCOMMIT`, registers the
destination-specific notify channels from
`core.destinations.notify_channels_for`, and enters a `select.select`
loop that polls every five seconds. When `psycopg2.notifies` arrives,
`stream_events` wraps the payload in `{"channel":...}` before emitting
`data: ...\n\n` so the browser receives unified updates without polling.

## Caching strategy

The blueprint keeps an in-memory `_cache` dict seeded with `metrics` and
`equity` values tied to `HEARTBEAT_SEC`. When `/api/equity` or
`/api/equity_intraday` is called, the handlers first check `_cache`
entries (based on TTL and cached timestamp) and, on a hit, return the
cached JSON immediately. `api_positions_cached` applies a different
technique: it recomputes `_open_version()` (max timestamp + count) to
build an ETag, compares it with `If-None-Match`, and issues `304` if
nothing changed. This protects the database from excessive live requests
and keeps the UI in sync with the push-based SSE updates.
