# Core Analytics Reference

The `src/cold_harbour/core` package holds the financial logic that powers
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

## Advanced features

### Orphan matching

The lot portfolio includes a **Sibling Strategy** that handles orphaned
sells (limit + stop pairs) whose explicit parent linkage is missing.
It matches these orphaned orders with open lots by comparing quantities
and uses the sell orders’ price legs to populate the lot’s missing TP/SL
targets, effectively pairing the closest sibling legs when the parent
chain can no longer be followed. Lots that receive values this way record
`Source: Sibling Strategy`, signalling that the targets were derived
from nearby orders rather than from a direct parent-child chain.

### Parent-child chain tracing

`trace_active_leg` recursively walks parent/child order relationships
(using `orders_map` and `children_map`) and stops at active limit/stop
orders. When a replacement order exists (`status == 'replaced'`), it
follows the `replaced_by` chain to the latest active leg. This lets the
portfolio expose accurate targets even when a trader modifies an order
mid-flight.

## Equity & metrics

`src/cold_harbour/core/equity.py` rebuilds the raw equity curve by
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

## Analytics vs. Reporting Utils

`src/cold_harbour/core/account_analytics.py` (notably `build_lot_portfolio`)
is treated as part of the **Account Manager**’s control loop. The manager
calls it to reconcile fills/orders, maintain lot-level open positions,
trace parent/child chains, and keep the live state aligned with Alpaca’s
order book for decision-making. That logic is executed where the trading
logic runs, so the portfolio it builds lands in `accounts.open_trades_*`
and `closed_trades_*` tables.

In contrast, the Web layer uses helpers from
`src/cold_harbour/core/account_utils.py`, especially `calculate_trade_metrics`,
to summarize the already-settled trades exposed by the manager. Those
helpers resolve the schema returned by `account_table_names` into
flattened DataFrames, collapse slice-level exits, and calculate the final
KPI buckets that populate the dashboard (TP/SL splits, durations,
mean stop/take, etc.). This split keeps the heavy reconciliation inside
the manager (analytics) while the UI performs lightweight reporting over
the cached results (`accounts` schema tables).
