# Risk Manager Service

The **Risk Manager** is an autonomous service responsible for eliminating
tail-risk in active trades. It continuously monitors open bracket
positions and moves Stop-Loss orders to "Break-Even" (entry price) once
specific favorable conditions are met.

## Overview

Unlike the Account Manager (which maintains state), the Risk Manager is an
**active execution agent** that runs on a schedule.

- **Goal:** Protect capital by neutralizing risk on profitable trades
  ("free roll").
- **Mechanism:** Updates the `stop_price` of existing open Stop-Loss legs
  via the Alpaca API.
- **Orchestration:** Runs as a series of DAGs inside **Apache Airflow**,
  triggering every minute during market hours.

## Trading Logic

The logic is defined in `BreakevenOrderManager`.

1.  **Trigger Level:** A position is "armed" when the price hits a
    pre-calculated target (e.g., `takeprofit_2_1_price` from the
    breakouts table).
2.  **Move Condition:**
    * **Long:** `Max High >= Trigger` AND `Last Trade >= Entry`.
    * **Short:** `Min Low <= Trigger` AND `Last Trade <= Entry`.
3.  **Safety Rails:**
    * Stops are never lowered (for longs) or raised (for shorts).
    * If the price has already crossed back through the entry, the move
      is skipped to avoid immediate stop-outs.

## Infrastructure

The Risk Manager is deployed differently from the persistent services:

* **Execution:** Python Script wrapped in Airflow DAGs
  (`dags/breakeven_multi_account.py`).
* **Scheduling:** `*/1 9-16 * * 1-5` (Every minute during NYSE trading
  hours).
* **Multi-Account:** Dynamically generates a separate DAG for each
  account defined in `destinations.py`.

## Data Interaction

* **Input:** Reads open orders from the database/API and market data
  (1-min bars) to find high/lows since entry.
* **Output:** Sends `PATCH /v2/orders/{id}` requests to Alpaca to update
  stop prices.
* **Audit:** Logs all decisions and moves to the `log_breakeven_<slug>`
  table in PostgreSQL.
