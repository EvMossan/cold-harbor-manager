# Account Manager Service

The **Account Manager** is a core service in Cold Harbour responsible for synchronizing trading account state between the Alpaca Brokerage API and the local PostgreSQL database in real-time. It acts as the "source of truth" for the application's understanding of current positions, equity, and order history.

## Overview

The service operates as an autonomous async agent that:
1.  **Maintains State:** Keeps an in-memory mirror of open positions and orders.
2.  **Syncs to DB:** Persists this state to PostgreSQL tables (`account_open_positions`, `account_closed`, etc.).
3.  **Real-time Updates:** Listens to Alpaca trade streams (WebSockets) for order fills and updates.
4.  **Price Feeds:** Consumes real-time market data (via ZMQ) to update mark-to-market values of open positions.
5.  **Push Notifications:** Uses PostgreSQL `NOTIFY` to push updates to the UI and other consumers immediately upon change.

## Architecture

The system is built around the `AccountManager` class (`src/cold_harbour/services/account_manager/runtime.py`), which orchestrates several specialized background workers.

### Core Components

| Component | Source File | Description |
|-----------|-------------|-------------|
| **Runtime** | `runtime.py` | The main entry point. Initializes resources, manages lifecycle, and spawns workers. |
| **Config** | `config.py` | Handles configuration validation and environment variable parsing. |
| **State** | `state.py` | Manages in-memory state mutations and database upserts/deletes. |
| **Trades** | `trades.py` | Processes trade updates from Alpaca (fills, cancellations) and manages order history. |
| **Snapshot** | `snapshot.py` | Reconciles state by pulling full position lists from Alpaca (used for initialization and drift correction). |
| **Workers** | `workers.py` | Contains the loops for background tasks (see below). |
| **DB** | `db.py` | Database abstraction layer for asyncpg/SQLAlchemy interactions. |

### Background Workers

The `AccountManager` runs several concurrent `asyncio` tasks:

1.  **`price_listener`**: Connects to a ZeroMQ (ZMQ) socket to receive real-time quote/trade data. It updates the current market price (`mkt_px`) of open positions and calculates P&L.
2.  **`db_worker`**: A throttling mechanism that flushes "dirty" state (changed prices) to the database at a fixed interval (e.g., every 60s) to avoid overwhelming the DB with tick-by-tick updates, while immediate structural changes (fills) are pushed instantly.
3.  **`snapshot_loop`**: Periodically performs a "full sync" with Alpaca to ensure local state hasn't drifted from the broker (e.g., due to missed WebSocket messages).
4.  **`closed_trades_worker`**: Periodically scans for newly closed trades and archives them to the closed trades history table.
5.  **`metrics_worker`**: Computes account KPIs. It now prioritizes the `cumulative_return` from the `account_equity_intraday` table (if available) to ensure the "Total Return %" on the dashboard updates in real-time during the session, falling back to daily equity only if necessary. Runs on a high-frequency 5-second interval to keep Total Return and Sharpe ratios live.
6.  **`cash_flows_worker`**: Bootstraps and ingests cash flow activities (dividends, journals, deposits) independently of equity updates so the ledger stays current even if equity rebuilding is paused.
7.  **`equity_intraday_worker`**: Rebuilds the intraday equity curve every minute using a "Mark-to-Market Delta" approach. It anchors calculations to the previous day's close and adds the P&L delta of current positions + intraday cash flows, ensuring the chart matches the broker's intraday volatility exactly.
8.  **`ui_heartbeat_worker`**: Emits a heartbeat signal over the PostgreSQL notification channel to let the frontend know the backend is alive.
9.  **`schedule_supervisor`**: A high-level supervisor that manages the lifecycle of the trading session. It polls `market_schedule` and automatically calls `_activate_session` (spawning workers) or `_deactivate_session` (canceling tasks) based on Pre-Market, Open, and Post-Market windows. Includes an automatic retention policy that prunes session rows older than 120 days to keep the table compact.

## Data Model

The service manages several key PostgreSQL tables. The schema and table names are configurable but default to:

### 1. Open Positions Table (`account_open_positions`)
Stores currently active positions.

| Column | Type | Description |
|--------|------|-------------|
| `parent_id` | Text (PK) | The ID of the parent order that opened the position. |
| `symbol` | Text | Ticker symbol (e.g., SPY). |
| `qty` | Integer | Current quantity held. |
| `avg_fill` | Real | Average entry price for this specific lot. |
| `avg_px_symbol` | Real | Broker's average entry price for the symbol (WAC). |
| `mkt_px` | Real | Current market price (updated via ZMQ). |
| `profit_loss` | Real | Unrealized P&L. |
| `sl_px` / `tp_px` | Real | Stop Loss and Take Profit prices (if bracket order). |
| `updated_at` | Timestamp | Last update time. |

### 2. Closed Trades Table (`account_closed`)
Stores historical performance of finished trades.

| Column | Type | Description |
|--------|------|-------------|
| `entry_lot_id` | Text | Reference to the opening order. |
| `exit_order_id` | Text | Reference to the closing order. |
| `pnl_cash` | Real | Realized Profit/Loss in cash. |
| `return_pct` | Real | Realized return percentage. |
| `duration_sec` | Real | How long the position was held. |

### 3. Equity & Cash Flow
-   `account_equity_full`: Stores historical equity curve.
-   `account_equity_intraday`: High-resolution intraday equity tracking.
-   `cash_flows`: Records deposits, withdrawals, and other cash movements.

## Configuration

Configuration is managed via `_Config` in `config.py`. Key environment variables include:

| Env Variable | Description | Default |
|--------------|-------------|---------|
| `API_KEY` | Alpaca API Key ID | (Required) |
| `SECRET_KEY` | Alpaca Secret Key | (Required) |
| `ALPACA_BASE_URL` | API Endpoint | `https://paper-api.alpaca.markets` |
| `CONN_STRING_POSTGRESQL` | DB Connection String | (Required) |
| `ZMQ_SIP_STREAM_ENDPOINT` | ZMQ Address for prices | `tcp://127.0.0.1:5558` |
| `POS_CHANNEL` | Postgres NOTIFY channel for positions | `pos_channel` |
| `ACCOUNT_LABEL` | Logical name for this account | Derived from config |
| `POSTGRESQL_LIVE_LOCAL_CONN_STRING` | Direct Postgres DSN used by `manager_run.py` when bypassing Cloudflare tunnels in local/network-adjacent runs | (Optional; overrides the tunneled value) |
| `UI_PUSH_PCT_THRESHOLD` | Minimum percentage swing before the UI push channel emits a refreshed row | `0.0` (push every update) |

## Runtime Behavior

### Initialization
When `run()` is called:
1.  Connects to PostgreSQL.
2.  Ensures all required tables exist (creating them if missing).
3.  Synchronizes the market schedule.
4.  Fetches an initial snapshot of all positions from Alpaca.
5.  Loads the latest `mkt_px` values from TimescaleDB (`fetch_latest_prices`
    in `db.py`) because the ZMQ price listener may not be operational yet.
6.  Starts the WebSocket trade stream and background workers.

### Trade Lifecycle
1.  **New Order:** `trades.py` detects a `new` order event.
2.  **Fill:** On `fill` or `partial_fill`, a row is upserted to `account_open_positions`.
3.  **Update:** ZMQ worker updates `mkt_px`. P&L is recalculated.
4.  **Close:** When `qty` reaches 0, the row is removed from `account_open_positions` and a record is written to `account_closed`.

#### Stream Robustness
The manager implements a Dual Client Fallback strategy for WebSockets.
It tries to connect with the modern `alpaca-py` `stream` client first, and if
the attempt fails or the package is unavailable, it falls back to the legacy
`alpaca_trade_api.stream` (SDK v2) to keep the stream running.

### Database Notifications
The service uses PostgreSQL `NOTIFY` to broadcast changes.
-   **Channel:** Configurable via `POS_CHANNEL`.
-   **Payload:** JSON object containing the updated row or deletion event.
    -   Upsert: `{"row": {...}}`
    -   Delete: `{"op": "delete", "parent_id": "..."}`
-   **Smart Throttling:** `pos_channel` notifies only when `mkt_px` shifts
    beyond `UI_PUSH_PCT_THRESHOLD` or when structural fields (`qty`, `status`,
    etc.) mutate. This keeps the UI responsive without overwhelming it on
    volatile ticks.

## Development

### Running the Manager
The manager is typically run via the `manager_run.py` script, which can handle multiple accounts defined in `cold_harbour.destinations`.

```bash
# Example run command
python -m cold_harbour.manager_run
```

### Extending
-   **Adding Columns:** Update `DB_COLS` in `config.py` and the `_ensure_tables` SQL in `db.py`.
-   **New Workers:** Define the worker in `workers.py` and add it to the `background` list in `runtime.py`.
