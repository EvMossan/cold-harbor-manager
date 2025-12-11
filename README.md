# Cold Harbour: Real-Time Trading Analytics Dashboard

**Cold Harbour** is a specialized high-frequency monitoring system designed for algorithmic traders. It serves as a unified "Control Tower" for Alpaca brokerage accounts, providing real-time visibility into positions, order flow, and equity performance that standard broker interfaces often lack.

It bridges the gap between raw execution data and actionable trading insights, acting as the authoritative source of truth for your trading bot's state.

## Preview

![Platform preview](docs/img/preview.png)

## Key Features

### 🛡️ Active Risk Management (The "Guardian")
*Powered by Apache Airflow*
-   **Break-Even Engine:** Automatically trails Stop-Loss orders to the entry price once a position is "safe" (price > entry + trigger).
-   **Smart Triggers:** Uses pre-calculated volatility targets (e.g., 30-min breakout levels) to arm the break-even logic, ensuring stops aren't moved prematurely during noise.
-   **Session Orchestration:** An autonomous Supervisor manages the trading lifecycle, spinning up workers for Pre-Market (04:00 ET) and shutting down after Post-Market close to save resources.

### 📊 Real-Time Analytics (The "Control Tower")
-   **Hybrid Pricing Model:** Simultaneously tracks **Strategy Price** (technical execution basis) and **Broker WAC** (tax/accounting basis), preventing P&L drift in decision-making.
-   **Live Greeks & Metrics:** Streaming calculation of Sharpe Ratio (Smart/Rolling), Win Rate, and Drawdown updated every 5 seconds.
-   **Intraday Equity Curve:** High-resolution (1-minute) charting that reconciles mark-to-market position values with cash flows (dividends, fees) in real-time.

### 🔍 Data Integrity & Execution
-   **Deep Chain Tracing:** Resolves complex OTO/Bracket chains, tracking "Grandchild" orders through multiple replacements (e.g., user-modified stops) to maintain correct parentage.
-   **Orphan Detection:** Identifies "broken" positions where the broker reports exposure but the bot sees no active stop-loss protection.
-   **Immutable Data Lake:** A dedicated Ingester service captures every WebSocket event into a raw schema using synthetic IDs, ensuring zero data loss even during downtime.

### ⚡ Technical Architecture
-   **Event-Driven UI:** Server-Sent Events (SSE) push updates to the dashboard with sub-second latency.
-   **Secure Tunnels:** Integrated **Cloudflare Access** tunnels expose local PostgreSQL/TimescaleDB instances securely without public IPs.

---

## System Components

The following architecture diagram summarizes the key services and data
flows:

```mermaid
graph TD
    %% Global Styles
    classDef external fill:#f9f,stroke:#333,stroke-width:2px;
    classDef service fill:#e1f5fe,stroke:#0277bd,stroke-width:2px;
    classDef worker fill:#e8f5e9,stroke:#2e7d32,stroke-width:1px;
    classDef db fill:#eee,stroke:#333,stroke-width:2px,shape:cylinder;
    classDef bus fill:#fff9c4,stroke:#fbc02d,stroke-width:2px,shape:rect;

    %% ---------------------------------------------------------
    %% 1. EXTERNAL SOURCES (Top)
    %% ---------------------------------------------------------
    subgraph External [External Market Data]
        direction TB
        AlpacaWS(Alpaca WebSocket):::external
        ZMQ(ZMQ Price Stream):::external
        AlpacaREST(Alpaca REST API):::external
    end

    %% ---------------------------------------------------------
    %% 2. INGESTION SERVICE
    %% ---------------------------------------------------------
    subgraph Ingester_Layer [Data Ingester Service]
        direction TB
        IngesterRun(<b>ingester_run.py</b>):::service
        
        %% Stack workers vertically
        StreamWork(stream_consumer):::worker
        HealWork(healing_worker):::worker
        
        IngesterRun --> StreamWork
        StreamWork ~~~ HealWork
    end

    %% ---------------------------------------------------------
    %% 3. RAW STORAGE
    %% ---------------------------------------------------------
    subgraph Raw_DB [Raw Storage Schema]
        direction TB
        RawOrders[(raw_orders_live)]:::db
        RawActs[(raw_activities_live)]:::db
        RawOrders ~~~ RawActs
    end

    %% ---------------------------------------------------------
    %% 4. MANAGER SERVICE
    %% ---------------------------------------------------------
    subgraph Manager_Layer [Account Manager Service]
        direction TB
        MgrRun(<b>manager_run.py</b>):::service
        
        %% Stack workers vertically
        PriceList(price_listener):::worker
        SnapLoop(snapshot_loop):::worker
        MetricsWork(metrics_worker):::worker
        EquityWork(equity_intraday):::worker
        
        %% Invisible links to force column layout
        MgrRun --> PriceList
        PriceList ~~~ SnapLoop
        SnapLoop ~~~ MetricsWork
        MetricsWork ~~~ EquityWork
    end

    %% ---------------------------------------------------------
    %% 5. LIVE STORAGE
    %% ---------------------------------------------------------
    subgraph Live_DB [Live Accounts Schema]
        direction TB
        TblLive[(open_positions)]:::db
        TblClosed[(closed_trades)]:::db
        TblMetrics[(account_metrics)]:::db
        TblLive ~~~ TblClosed
        TblClosed ~~~ TblMetrics
    end

    %% ---------------------------------------------------------
    %% 6. CONSUMERS (Bottom)
    %% ---------------------------------------------------------
    subgraph Consumers [Clients & Orchestration]
        direction TB
        RiskDAG(<b>Airflow Risk DAG</b>):::service
        Flask(<b>Flask Dashboard</b>):::service
        SSE((SSE Stream)):::bus
        
        RiskDAG ~~~ Flask
    end

    %% =========================================================
    %% DATA FLOW CONNECTIONS
    %% =========================================================

    %% Inflow
    AlpacaWS --> StreamWork
    AlpacaREST --> HealWork
    ZMQ --> PriceList

    %% Ingestion Writes
    StreamWork --> RawOrders & RawActs
    HealWork --> RawOrders & RawActs

    %% Manager Reads/Writes
    RawOrders & RawActs --> SnapLoop
    PriceList -- Update Px --> TblLive
    SnapLoop -- Reconcile --> TblLive
    
    %% Internal Manager Flow
    TblLive --> MetricsWork & EquityWork
    MetricsWork --> TblMetrics
    EquityWork --> TblLive

    %% Risk Manager Flow
    TblLive --> RiskDAG
    RiskDAG -- Patch Order --> AlpacaREST

    %% Web Flow
    TblMetrics --> Flask
    TblLive -.->|PG NOTIFY| SSE
    SSE --> Flask

```

1.  **Account Manager (The Brain):**
    An async Python daemon that maintains the "live" state. It reconciles REST API snapshots with WebSocket streams, calculates Greeks/metrics, and pushes updates to the UI via Postgres `NOTIFY`.

2.  **Data Ingester (The Memory):**
    A robust, self-healing service that creates a raw data lake. It uses **Synthetic IDs**
    (`Timestamp::ExecutionID`) to seamlessly deduplicate high-speed WebSocket events
    against REST API history, ensuring 100% data integrity even during connection
    drops.

3.  **Web Dashboard (The View):**
    A concise, single-page application (`account_positions.html`) rendering live tables and charts. It features visual P&L flashing, "Break-Even" status indicators, and multi-account switching.

4.  **Risk Manager (The Guardian):**
    An automated break-even engine running on **Apache Airflow**. It scans active bracket orders every minute and mechanically moves stop-losses to the entry price once targets are hit, securing profits without manual intervention.

5.  **Airflow Orchestrator:**
    Manages the scheduling of the Risk Manager cycles and other periodic maintenance tasks (DAGs), ensuring they run only during valid market hours across multiple accounts.

## Repository Layout

- `src/coldharbour_manager/` – Application source code.
  - `services/account_manager/` – Core trading logic and state management.
  - `services/activity_ingester/` – Raw data capture service.
  - `web/` – Flask blueprint and Jinja2 templates.
  - `core/` – Shared financial math (Smart Sharpe, FIFO/LIFO matching).
  - `services/risks_manager/` – Break-even logic and order modification engine.
- `docs/` – Detailed architectural documentation.
- `docker-compose.yml` – Full local development stack (Manager + Web + Ingester).
- `dags/` – Airflow DAG definitions for multi-account risk management.

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

## Packaging for reuse

This repository already exposes its core modules as a Python package
named `coldharbor_manager`. Run `pip install .` (or `pip install -e .`
for editable installs) from the project root to make the package
available to other applications. The package simply extends the legacy
`coldharbour_manager` directory, so clients can import either
`coldharbor_manager.services.account_manager` or the legacy path
`coldharbour_manager.services.account_manager`, whichever suits their code
base.

## Documentation

For deep dives into specific subsystems, refer to the `docs/` directory:

### Core Logic & Trading
-   [**Core Analytics**](docs/core_analytics.md): Trade matching engines (FIFO vs. Lot), GTC expiration logic, and Chain Tracing.
-   [**Risk Manager**](docs/risk_manager.md): Break-even algorithms and Airflow DAG scheduling.
-   [**Account Manager**](docs/account_manager.md): The state machine, background workers, and session supervisor.

### Data & Infrastructure
-   [**Data Flow & Schema**](docs/data_flow_and_schema.md): How data moves from Alpaca WS → Ingester (Raw) → Manager (Live) → UI.
-   [**Data Ingester**](docs/ingester.md): The immutable data lake, healing strategies, and backfill logic.
-   [**Infrastructure**](docs/infrastructure.md): Cloudflare tunnels, Docker containers, and CI/CD pipelines.

### Operations & UI
-   [**Web Architecture**](docs/web_architecture.md): Flask blueprints, SSE streaming, and caching layers.
-   [**Frontend Logic**](docs/frontend_logic.md): JavaScript state management for the dashboard (`account_positions.html`).
-   [**Operations Guide**](docs/operations_guide.md): Adding new destinations, environment variables, and troubleshooting.
- [**Web Architecture**](docs/web_architecture.md): How the SSE streaming and caching layers function.
- [**Risk Manager**](docs/risk_manager.md): Logic for break-even stops and Airflow scheduling.

## Deployment

The system is designed for **Google Cloud Run**. The `cloudbuild.yaml` pipeline builds the container and deploys it with secrets injected from Secret Manager. `entrypoint.sh` automatically handles Cloudflare Tunnel initialization before starting the application.

## Contributing

Please refer to [`CONTRIBUTING.md`](CONTRIBUTING.md) for branch naming conventions and Pull Request workflows.
