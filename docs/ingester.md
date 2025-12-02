# Data Ingester Service

The **Data Ingester** is a standalone infrastructure service
responsible for the reliable capture and storage of raw trading data.
Unlike the Account Manager, which maintains a "live" state for trading
logic, the Ingester acts as an immutable **Data Lake**, capturing every
order lifecycle event and account activity (fills, dividends, journals)
into a dedicated schema.

## Overview

The service operates as a multi-account daemon that:
1.  **Raw Storage:** Persists data into the `account_activities`
schema with minimal transformation.
2.  **Redundancy:** Uses a "Healing" mechanism to fill gaps in
WebSocket streams via REST polling.
3.  **Idempotency:** Generates synthetic IDs for stream events to ensure
perfect deduplication against REST API data.
4.  **Independence:** Runs separately from the trading logic to ensure
data is captured even if the Account Manager restarts or crashes.

## Architecture

The system is orchestrated by the `IngesterService`
(`src/cold_harbour/services/activity_ingester/runtime.py`), which manages a
set of workers for each account defined in `DESTINATIONS`.

### Core Components

| Component | Source File | Description |
|-----------|-------------|-------------|
| **Runtime** | `runtime.py` | Orchestrator that connects to the DB and spawns workers for each account. |
| **Workers** | `workers.py` | Contains the logic for Stream, Backfill, and Healing tasks. |
| **Storage** | `storage.py` | Handles schema creation and idempotent UPSERT operations. |
| **Transform** | `transform.py` | Normalizes incoming data and generates synthetic IDs for stream events (Time::UUID). |
| **Config** | `config.py` | Static configuration (intervals, batch sizes, schema names). |

### Background Workers

For *each* configured account, the Ingester runs three concurrent tasks:

1.  **`stream_consumer`**: Connects to the Alpaca WebSocket
(`trade_updates`) to ingest events in real-time. It provides low-latency
visibility into order changes.
2.  **`backfill_task`**: Runs once at startup. It checks the latest
timestamp in the DB and pulls historical data from the API to fill gaps
caused by downtime.
3.  **`healing_worker`**: A periodic background task (default: every 5
mins) that polls the REST API for recent history. It ensures that any
events dropped by the WebSocket connection are eventually captured and
stored.

## Data Model

The service manages per-account tables within the `account_activities`
schema (e.g., `account_activities.raw_orders_live`).

### 1. Raw Orders (`raw_orders_<slug>`)
Stores the mutable state of orders. Since order statuses change (New ->
Filled), this table uses upserts to reflect the latest state.

| Column | Type | Description |
|--------|------|-------------|
| `id` | UUID (PK) | The Alpaca Order ID. |
| `client_order_id` | Text | Client-side ID. |
| `status` | Text | Current status (filled, canceled, etc.). |
| `raw_json` | JSONB | The full, untouched JSON payload from the API. |
| `updated_at` | Timestamptz | Last update timestamp from the broker. |

### 2. Raw Activities (`raw_activities_<slug>`)
Stores an append-only log of financial events (FILL, DIV, FEE, JNLC).

| Column | Type | Description |
|--------|------|-------------|
| `id` | Text (PK) | Unique ID. For fills, this is a synthetic `Timestamp::ExecutionID` to deduplicate Stream vs REST data. |
| `activity_type` | Text | Event type (FILL, DIV, etc.). |
| `net_amount` | Numeric | Cash impact of the event. |
| `execution_id` | UUID | Specific execution ID for fills. |
| `raw_json` | JSONB | Full event payload. |

## Runtime Behavior

### Idempotency Strategy
A key challenge is reconciling WebSocket "Trade Updates" (which don't
have standard Activity IDs) with REST "Account Activities".

The `transform.py` module solves this by generating a **Synthetic ID** for
stream events:
- Format: `YYYYMMDDHHMMSSmmm::execution_id`
- Timezone: America/New_York (matching Alpaca's internal ID
generation).

This allows the Ingester to insert data from the Stream immediately, and
later safely "overlap" it with data fetched by the Healing worker without
creating duplicates.

### Configuration

Configuration is handled via `IngesterConfig` in `config.py` and
environment variables.

| Constant | Default | Description |
|----------|---------|-------------|
| `DB_SCHEMA` | `account_activities` | Postgres schema for raw tables. |
| `HEALING_INTERVAL_SEC` | `300` | How often the healer polls REST API. |
| `HEALING_LOOKBACK_SEC` | `600` | Lookback window for healing (overlapping the interval). |

## Development

### Running the Ingester
The service is typically run via Docker Compose or the module entrypoint.

```bash
# Run standalone
python -m cold_harbour.services.activity_ingester.ingester_run
```
It reuses the same `POSTGRESQL_LIVE_CONN_STRING` and `DESTINATIONS`
configuration as the Account Manager.
