# Data Flow and Schema Layers

Cold Harbour maintains two complementary schema layers so the raw
feeding stream can stay immutable while the live trading state remains
responsive for operators.

## Ingester layer (`account_activities` schema)

`IngesterService` (`src/cold_harbour/services/activity_ingester/runtime.py`
and its workers in `workers.py`) owns the `account_activities` schema.
Each destination gets `raw_orders_<slug>` and `raw_activities_<slug>`
tables that are append-only. Stream events arrive via `trade_updates` and
REST polling, then `transform.py` normalizes the JSON into a consistent
shape, adds ingestion timestamps, and, for fills, generates the synthetic
`Timestamp::ExecutionID` IDs used to deduplicate Stream vs REST rows. The
Rental `storage.py` module ensures the tables exist and performs
idempotent UPSERTs so the layer can replay downed workers without losing
data.

## Manager layer (`accounts` schema)

`AccountManager` (`src/cold_harbour/services/account_manager/runtime.py`) and
its supporting modules (open/closed tables defined via
`core.destinations.account_table_names`) keep the live state inside the
`accounts` schema. Tables like `open_trades_<slug>` and
`closed_trades_<slug>` are kept up-to-date via the manager's REST, ZMQ,
and equity reconcilers. Those tables are what the Web UI reads through
`src/cold_harbour/web/routes.py` and what dashboard summaries, KPI
tables, and SSE feeds rely on.

## Flow diagram (text-based)

1. **Alpaca WebSocket (`trade_updates`)** feeds events to the Ingester
   Stream worker and also hits the Account Manager when live logic
   executes.
2. **Data Ingester (raw)** captures every order/activity into
   `account_activities.raw_*` tables with synthetic IDs for HEADERS and
   REST healing to ensure completeness.
3. **Parallel ingestion paths**: Healing/Backfill workers keep the raw
   tables rehydrated while the Account Manager replays the same raw data
   through consensus logic for trading.
4. **Account Manager → Live DB** writes to `accounts.open_trades_<slug>`
   and `accounts.closed_trades_<slug>`, enabling fast queries for the
   Web layer.
5. **Web UI / SSE** (`src/cold_harbour/web/routes.py`) reads from the live
   tables, serves `/api` endpoints, and listens to PostgreSQL `NOTIFY`
   channels so dashboards remain low-latency.
