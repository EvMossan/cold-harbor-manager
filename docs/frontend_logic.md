# Frontend Logic (`account_positions.html`)

This document explains the JavaScript powering
`src/cold_harbour/web/templates/account_positions.html`, which wraps a
dashboard in a single-page interaction surface.

## State management (pfHeader)

A global `pfHeader` object lifts key values across page reloads so the
daily and intraday views stay in sync. Lines ~553‑618 initialize
`pfHeader` on `window`, attach DOM handles (`pfHeader.valueEl` etc.), and
reserve storage for `baseDeposit`, `dailyDeposit`, `intradayDeposit`,
and session metadata. `syncPortfolioHeaderState` consults those fields to
render the right currency/percentage display: if the user is viewing
`intraday` data it favors `pfHeader.intradayDeposit`; otherwise it
defaults to the daily baseline. Updates to session flow, realised P/L,
and live deposits (see `renderPortfolioHeaderValue` and
`updateSessionMetadata`) mutate `pfHeader` so the UI reflects the most
recent streaming + REST data without recalculating everything.

## SSE batching and scheduling

SSE updates arrive through `/stream/events`, but the UI batches them via
`state.queue` (lines ~504‑620). `handleStreamEvent` parses each `data`
message, pushes position updates into `state.queue`, and lets
`fetchPositionsCached` refresh the `DataTable` when the server emits a
heartbeat. A periodic `setInterval` every 250 ms calls `flush()`, which
consumes the queue, de-duplicates deletions, and applies upserts before
redrawing the table. When the tab is hidden (`state.hidden`), `flush`
still runs but drains the queue immediately so background tabs do not
accumulate stale state.

`window.scheduleAligned` (lines ~460‑529) keeps periodic tasks
(such as `loadKpi`, `refreshDaily`, and `refreshIntra`) synchronized with
`HEARTBEAT_MS`. It aligns each callback to the next heartbeat boundary,
then reschedules itself, which keeps the dashboard from drifting even if
initial loads happen slightly off-cycle. This prevents overlapping
requests and keeps the UI responsive even under Hammer-style SSE bursts
because `flush` and the aligned scheduler pace updates instead of
re-rendering immediately on every notification.

## Charting and color logic

Custom Chart.js helpers build the equity/drawdown charts. `splitSeriesWithZero`
(lines ~1653‑1700) takes the equity cumulative-return series, splits it
into positive/negative datasets, and injects explicit zero-crossing
points so Chart.js can draw continuous GREEN/RED segments without gaps
when the curve crosses the zero line. `cfgDaily` and `cfgIntraday`
invoke this helper, then create lookahead gradients, zero-line overlays,
and per-axis formatting that match the CSS palette. The plugin
`crosshairPlugin` also massaged dataset styling (point radius, border
colors) so hovering highlights remain visible.

By combining `pfHeader`, the SSE queue, `scheduleAligned`, and
`splitSeriesWithZero`, the template keeps the dashboard state consistent
and performant even when Alpaca emits bursts of WebSocket updates or the
browser window is throttled.
