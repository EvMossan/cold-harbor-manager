# Core Analytics Reference

The `src/cold_harbour/core` package holds the financial logic that powers
both the manager and the dashboard summaries. `account_analytics.py` reads
fills/orders, matches trades, and enriches every position, while
`equity.py` replays cash flows into a Sharpe/drawdown-aware curve.

## Trade matching

### `build_lot_portfolio`

Creates a **lot-based portfolio** that mirrors the open-position table.
It sorts fills by execution time, builds `lots` keyed by `order_id`, and
maintains `fifo_queues` so partial closes can fall back to FIFO when a
parent/child link is missing. The function also consumes Alpaca's
current positions (via `api.list_positions()`) to append `cur_px` and
`avg_px` data for live quoting. Every lot tracks `Take_Profit_Price` and
`Stop_Loss_Price` by tracing active children through `trace_active_leg`.
Parent-child chains are resolved by inspecting `orders_df` metadata,
allowing the logic to follow replacements (`status == 'replaced'`) and
derive current limit/stop levels.

### `build_closed_trades_df_fifo`

Implements a **pure FIFO** close engine. It sorts fills chronologically,
builds per-symbol buy inventory, and matches sells against the oldest
buys, emitting one closed trade per matched tranche. Every record
contains entry/exit IDs, fulfillment timestamps, PnL cash/pct, and
`exit_type` sourced from `orders_df`. The FIFO engine is used as a
reference to compare against the lot-based view and to compute unit PnL
fallbacks when parent matches are unavailable.

## Advanced features

### Orphan matching

The lot portfolio includes a second pass that scans remaining open lots
with missing stop or take-profit levels. It filters `orders_df` for
active sell orders (`status` in `['new', 'held', ...]`) whose quantity
matches the exposed lot quantity, then assigns orphaned TP/SL prices
from that order. Any lot updated via this heuristic receives a `Source`
flag of `Orphan Match` to signal the compensation logic.

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
