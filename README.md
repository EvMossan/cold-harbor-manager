# Cold Harbour: Real-Time Trading Analytics Dashboard

**Cold Harbour** is a specialized high-frequency monitoring system designed for algorithmic traders. It serves as a unified "Control Tower" for Alpaca brokerage accounts, providing real-time visibility into positions, order flow, and equity performance that standard broker interfaces often lack.

It bridges the gap between raw execution data and actionable trading insights, acting as the authoritative source of truth for your trading bot's state.

---

## Key Features

### 📊 Real-Time Account Analytics
- **Live P&L Tracking:** Streaming mark-to-market valuation of all open positions via ZeroMQ price feeds.
- **Smart Metrics:** Real-time calculation of Sharpe Ratio (10d/21d/63d...), Drawdown, Win Rate, and Risk/Reward ratios.
- **Equity Curve:** High-resolution (1-minute) intraday equity charting alongside historical daily performance.

### 🛡️ Order Management & Risk
- **Deep Chain Tracing:** Automatically resolves complex order chains (OTO/OCO/Brackets), tracking positions even through multiple order replacements/modifications.
- **Orphan Detection:** Identifies "orphaned" positions that lack Stop-Loss or Take-Profit protection.
- **Data Lake (Ingester):** A standalone service that immutably records every trade event, order state change, and cash flow (dividends, fees) for auditability.

### ⚡ Technical Architecture
- **Event-Driven UI:** A lightweight Flask dashboard powered by Server-Sent Events (SSE) for sub-second updates without page reloads.
- **Hybrid Data Storage:** Uses **PostgreSQL** for transactional state (orders/positions) and **TimescaleDB** for time-series data (intraday equity, price bars).
- **Secure Connectivity:** Built-in integration with **Cloudflare Access** tunnels to securely expose database connections from Cloud Run or local dev environments without public IPs.

- **Hybrid Pricing Model:** Simultaneously tracks "Strategy Price" (for technical execution) and "Broker Average Cost" (for financial reconciliation), preventing P&L drift while maintaining precise stop-loss logic.
- **Session-Aware Architecture:** An autonomous `Schedule Supervisor` manages the lifecycle of background workers, automatically spinning up resources for Pre-Market and shutting down after Post-Market close.

---

## System Components

1.  **Account Manager (The Brain):**
    An async Python daemon that maintains the "live" state. It reconciles REST API snapshots with WebSocket streams, calculates Greeks/metrics, and pushes updates to the UI via Postgres `NOTIFY`.

2.  **Data Ingester (The Memory):**
    A robust, self-healing service that creates a raw data lake. It uses **Synthetic IDs**
    (`Timestamp::ExecutionID`) to seamlessly deduplicate high-speed WebSocket events
    against REST API history, ensuring 100% data integrity even during connection
    drops.

3.  **Web Dashboard (The View):**
    A concise, single-page application (`account_positions.html`) rendering live tables and charts. It features visual P&L flashing, "Break-Even" status indicators, and multi-account switching.

## Repository Layout

- `src/cold_harbour/` – Application source code.
  - `services/account_manager/` – Core trading logic and state management.
  - `services/activity_ingester/` – Raw data capture service.
  - `web/` – Flask blueprint and Jinja2 templates.
  - `core/` – Shared financial math (Smart Sharpe, FIFO/LIFO matching).
- `docs/` – Detailed architectural documentation.
- `docker-compose.yml` – Full local development stack (Manager + Web + Ingester).

## Getting Started

### Prerequisites
- Docker & Docker Compose.
- Access to an Alpaca Trading Account (Live or Paper).
- A Postgres+TimescaleDB instance (local or remote).

### Quick Start (Local)

1.  **Configure Environment:**
    Copy the template `.env` (not included in git) and populate your credentials:
    ```bash
    ALPACA_API_KEY_LIVE=...
    ALPACA_SECRET_KEY_LIVE=...
    POSTGRESQL_LIVE_LOCAL_CONN_STRING=postgresql://user:pass@localhost:5433/db
    ```

2.  **Launch Stack:**
    ```bash
    docker compose up --build
    ```
    This spins up the *Manager* (syncing state), the *Ingester* (archiving history), and the *Web UI*.

3.  **Access Dashboard:**
    Open `http://localhost:5000` to view your account metrics.

## Documentation

For deep dives into specific subsystems, refer to the `docs/` directory:

- [**Core Analytics**](docs/core_analytics.md): How trades are matched (FIFO vs. Lot) and how "Deep Chain" tracing works.
- [**Account Manager**](docs/account_manager.md): Architecture of the state machine and background workers.
- [**Data Ingester**](docs/ingester.md): Explanation of the immutable data lake and healing strategies.
- [**Web Architecture**](docs/web_architecture.md): How the SSE streaming and caching layers function.

## Deployment

The system is designed for **Google Cloud Run**. The `cloudbuild.yaml` pipeline builds the container and deploys it with secrets injected from Secret Manager. `entrypoint.sh` automatically handles Cloudflare Tunnel initialization before starting the application.

## Contributing

Please refer to [`CONTRIBUTING.md`](CONTRIBUTING.md) for branch naming conventions and Pull Request workflows.
