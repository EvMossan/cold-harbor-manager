# Core Analytics Reference

The `src/coldharbour_manager/core` package holds the financial logic that powers
both the manager and the dashboard summaries. `account_analytics.py` reads
fills/orders, matches trades, and enriches every position, while
`equity.py` replays cash flows into a Sharpe/drawdown-aware curve.

## Trade matching

### `build_lot_portfolio`

Constructs a **lot-based portfolio** that mirrors the open-position table
while implementing a **Split Pricing Logic** to reconcile Strategy
targets with Broker P/L.

- **Buy Price (Strategy Execution):** Derived from the actual execution
  price stored in the DB. This is used to calculate technical metrics
  like `TP Reach %` and `Break-Even` status, ensuring the bot's logic
  remains consistent with its original entry intent.
- **Avg Entry (API):** Derived from the Broker's API (WAC). This is used
  strictly for the `Profit/Loss` column to ensure the dashboard values
  match the broker's official balance.

The function deduplicates orders by `updated_at` and matches active legs
(limit/stop) to these positions.

### `build_closed_trades_df_lot`

Implements a **Lot-Based** close engine. It pre-processes raw fills using
`_compress_fills` to calculate weighted average prices per order ID. It
then matches SELL orders against specific BUY lots (inventory) using a
priority queue.

- **Matching Priority:** Direct Parent Match -> FIFO Fallback for orphans.
- **Unit PnL Validation:** The function also calculates `fifo_unit_pnl`
  (Pure FIFO per share) internally to compute `diff_pnl`, highlighting
  discrepancies between the Lot-based strategy and the broker's FIFO
  accounting.

### Order Expiration (GTC)

Alpaca orders with `TimeInForce.GTC` (Good Till Canceled) implicitly expire
after 90 days, though the API does not always populate `expires_at`.

`build_lot_portfolio` implements a fallback logic:
1.  **Explicit:** Uses the `expires_at` timestamp from the order leg if
    present.
2.  **Inferred:** If missing, calculates `created_at + 90 days` (defined as
    `ALPACA_GTC_DAYS`).
3.  **Metric:** Calculates `Days_To_Expire` (integer) relative to the current
    timestamp and exposes it to the dashboard to warn about stale orders
    nearing cancellation.

## Advanced features

### Replacement Chain Resolution ("Latest Successor")

Instead of recursive tracing, `build_lot_portfolio` pre-processes the
entire order history to build a **Forward Replacement Map**.

1.  **Indexing:** It scans all orders to map `replaces` IDs to their new
    `id`.
2.  **Resolution:** A helper `get_latest_successor(id)` follows this chain
    to the bitter end to find the currently active order ID.
3.  **Fresh Data:** When processing a parent order's legs (TP/SL), the
    system resolves the leg ID to its latest successor. It then fetches
    the *live* status, limit price, and stop price from that successor
    order (preferring DB state over stale JSON snapshots).

This ensures that even if a user modifies a Stop-Loss order 5 times in
the Alpaca UI, the portfolio snapshot will use the price and status of
the 5th (final) replacement order.

## Equity & metrics

`src/coldharbour_manager/core/equity.py` rebuilds the raw equity curve by
aggregating realized/unrealized PnL, aligning cash flows, and computing
Sharpe/drawdown statistics.

- **Flow alignment** – `_align_flows_to_business_days` punches every cash
  flow into the next business day index (courtesy of either the
  schedule table or `pd.bdate_range`). Amounts beyond the indexed range
  are skipped until the index expands on the next rebuild.
- **Series rebuild** – `rebuild_equity_series` (called once per day) reads
  fills, positions, cash flows, and optionally live unrealized PnL,
  then builds `deposit`, `realised_pl`, `unrealised_pl`, and their
  cumulative returns. `drawdown` is stored as `deposit / peak - 1.0`, so
  each row records the persistent drawdown since the all-time-high.
- **Smart Sharpe** – Rolling Sharpe ratios are computed in `_rolling_sharpe`.
  The `smart` variant multiplies the standard deviation by
  `_autocorr_penalty` to inflate the denominator when autocorrelation is
  present. The table stores realized/unrealized/daily Sharpe numbers for
  every configured window (e.g. 10, 21, 63, 126, 252 trading days).
- **Intraday updates** – `update_today_row` keeps the latest equity row
  aligned with the trading schedule so intraday readers (like the web
  layer) can display up-to-date deposits without rerunning the full
  rebuild.

## Analytics vs. Reporting

Previously, reporting logic was split between the Manager and Web layers.
In the current architecture, **all heavy lifting is centralized in the Account
Manager**.

-   **`src/coldharbour_manager/services/account_manager/core_logic/account_analytics.py`**:
    This is the unified calculation engine. It is used by:
    1.  **Trading Logic:** To reconcile fills and build the live portfolio state.
    2.  **Metrics Worker:** To periodically compute the full KPI set (Win Rate, Sharpe, Totals) and serialize it to the `account_metrics` table.

-   **Web Layer Role:**
    The Flask app (`routes.py`) is purely a **viewer**. It does not perform trade matching or P&L calculation. Instead, it queries the `account_metrics` table for the latest JSON payload and serves it directly to the frontend. `src/coldharbour_manager/core/account_utils.py` remains as a legacy/offline analysis toolkit.
