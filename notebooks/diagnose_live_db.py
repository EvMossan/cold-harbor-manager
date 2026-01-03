import asyncio
import os
from typing import Iterable, Optional, Sequence

import asyncpg
import pandas as pd


CONN_STRING = os.getenv(
    "POSTGRESQL_LIVE_LOCAL_CONN_STRING",
    "postgresql://airflow:airflow@192.168.3.77:5433/airflow",
)

# Optional filters for deep-dive examples.
ORDER_ID = os.getenv("DIAG_ORDER_ID")
ENTRY_LOT_ID = os.getenv("DIAG_ENTRY_LOT_ID")


async def fetch_df(
    conn: asyncpg.Connection, sql: str, args: Sequence[object] = ()
) -> pd.DataFrame:
    """Run a read-only query and return a DataFrame."""
    rows = await conn.fetch(sql, *args)
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame([dict(r) for r in rows])


def _print_df(title: str, df: pd.DataFrame, limit: int = 20) -> None:
    """Pretty print a DataFrame with a title."""
    print(f"\n=== {title} ===")
    if df.empty:
        print("(no rows)")
        return
    with pd.option_context("display.max_rows", limit, "display.width", 120):
        print(df.head(limit))


async def main() -> None:
    """Run diagnostics for the Live account tables."""
    conn = await asyncpg.connect(CONN_STRING)
    try:
        summary_sql = """
            select
              count(*) as total_rows,
              count(distinct id) as distinct_ids,
              count(distinct order_id) as distinct_order_ids,
              min(transaction_time) as min_time,
              max(transaction_time) as max_time
            from account_activities.raw_activities_live;
        """
        summary_df = await fetch_df(conn, summary_sql)
        _print_df("raw_activities_live summary", summary_df)

        dup_key_sql = """
            select
              order_id,
              transaction_time,
              qty,
              price,
              side,
              count(*) as rows
            from account_activities.raw_activities_live
            where activity_type = 'FILL'
              and order_id is not null
            group by order_id, transaction_time, qty, price, side
            having count(*) > 1
            order by rows desc
            limit 20;
        """
        dup_key_df = await fetch_df(conn, dup_key_sql)
        _print_df("FILL duplicates by key", dup_key_df)

        multi_exit_sql = """
            select
              entry_lot_id,
              count(distinct exit_order_id) as distinct_exits,
              count(*) as rows
            from accounts.closed_trades_live
            where entry_lot_id is not null
            group by entry_lot_id
            having count(distinct exit_order_id) > 1
            order by distinct_exits desc
            limit 20;
        """
        multi_exit_df = await fetch_df(conn, multi_exit_sql)
        _print_df("entry_lot_id with multiple exit_order_id", multi_exit_df)

        if ORDER_ID:
            order_sql = """
                select
                  id,
                  activity_type,
                  order_id,
                  execution_id,
                  transaction_time,
                  qty,
                  price,
                  side,
                  net_amount
                from account_activities.raw_activities_live
                where order_id = $1
                order by transaction_time, id;
            """
            order_df = await fetch_df(conn, order_sql, (ORDER_ID,))
            _print_df(f"raw_activities for order_id={ORDER_ID}", order_df)

            order_group_sql = """
                select
                  order_id,
                  transaction_time,
                  qty,
                  price,
                  side,
                  count(*) as rows,
                  array_agg(id order by id) as ids
                from account_activities.raw_activities_live
                where activity_type = 'FILL'
                  and order_id = $1
                group by order_id, transaction_time, qty, price, side
                order by rows desc;
            """
            order_group_df = await fetch_df(
                conn, order_group_sql, (ORDER_ID,)
            )
            _print_df(
                f"FILL groups for order_id={ORDER_ID}", order_group_df
            )

        if ENTRY_LOT_ID:
            closed_sql = """
                select
                  entry_lot_id,
                  exit_order_id,
                  exit_time,
                  pnl_cash,
                  pnl_cash_fifo,
                  diff_pnl
                from accounts.closed_trades_live
                where entry_lot_id = $1
                order by exit_time, exit_order_id;
            """
            closed_df = await fetch_df(conn, closed_sql, (ENTRY_LOT_ID,))
            _print_df(
                f"closed_trades for entry_lot_id={ENTRY_LOT_ID}", closed_df
            )
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
