from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from coldharbour_manager.infrastructure.db import AsyncAccountRepository


async def _db_fetch(
    repo: Optional[AsyncAccountRepository], sql: str, *args: Any
) -> list[dict]:
    assert repo is not None
    return await repo.fetch(sql, *args)


async def fetch_latest_prices(
    repo: Optional[AsyncAccountRepository], symbols: list[str]
) -> dict[str, float]:
    """Return latest price per symbol from TimescaleDB."""
    if not repo or not symbols:
        return {}

    symbols_upper = [s.upper() for s in symbols]

    # Use DISTINCT ON to fetch the newest row per symbol.
    # Adjust the timestamp column name if the schema uses 'time'.
    sql = """
        SELECT DISTINCT ON (symbol) symbol, price
        FROM alpaca_trade_data
        WHERE symbol = ANY($1)
        ORDER BY symbol, timestamp DESC
    """

    try:
        rows = await repo.fetch(sql, symbols_upper)
        return {
            row["symbol"].upper(): float(row["price"])
            for row in rows
            if row.get("price") is not None
        }
    except Exception:
        return {}


async def _db_fetchrow(
    repo: Optional[AsyncAccountRepository], sql: str, *args: Any
) -> Optional[dict]:
    assert repo is not None
    return await repo.fetchrow(sql, *args)


async def _db_execute(
    repo: Optional[AsyncAccountRepository],
    logger: logging.Logger,
    sql: str,
    *args: Any,
) -> str:
    assert repo is not None
    try:
        return await repo.execute(sql, *args)
    except Exception:
        logger.exception("DB execute failed: %s", sql)
        raise


async def _ts_fetch(
    ts_repo: Optional[AsyncAccountRepository],
    repo: Optional[AsyncAccountRepository],
    sql: str,
    *args: Any,
) -> list[dict]:
    if ts_repo is not None:
        return await ts_repo.fetch(sql, *args)
    return await _db_fetch(repo, sql, *args)


async def _ts_fetchrow(
    ts_repo: Optional[AsyncAccountRepository],
    repo: Optional[AsyncAccountRepository],
    sql: str,
    *args: Any,
) -> Optional[dict]:
    if ts_repo is not None:
        return await ts_repo.fetchrow(sql, *args)
    return await _db_fetchrow(repo, sql, *args)


async def _db_executemany(
    repo: Optional[AsyncAccountRepository],
    sql: str,
    params: list[tuple[Any, ...]],
) -> None:
    assert repo is not None
    await repo.executemany(sql, params)


async def _type_exists(
    repo: Optional[AsyncAccountRepository], name: str
) -> bool:
    schema, typ = (None, name)
    if "." in name:
        schema, typ = name.split(".", 1)
    schema = schema or "public"
    row = await _db_fetchrow(
        repo,
        """
        SELECT 1
          FROM pg_catalog.pg_type t
          JOIN pg_catalog.pg_namespace n
            ON n.oid = t.typnamespace
         WHERE n.nspname = $1
           AND t.typname = $2
         LIMIT 1;
        """,
        schema,
        typ,
    )
    return bool(row)


async def _drop_type(
    repo: Optional[AsyncAccountRepository],
    name: str,
    logger: logging.Logger,
) -> None:
    schema, typ = (None, name)
    if "." in name:
        schema, typ = name.split(".", 1)
    schema = schema or "public"
    await _db_execute(
        repo,
        logger,
        f"DROP TYPE IF EXISTS {schema}.{typ};",
    )


async def _relation_exists(
    repo: Optional[AsyncAccountRepository], name: str
) -> bool:
    schema, table = (None, name)
    if "." in name:
        schema, table = name.split(".", 1)
    schema = schema or "public"
    row = await _db_fetchrow(
        repo,
        """
        SELECT 1
          FROM pg_catalog.pg_class c
          JOIN pg_catalog.pg_namespace n
            ON n.oid = c.relnamespace
         WHERE n.nspname = $1
           AND c.relname = $2
           AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
         LIMIT 1;
        """,
        schema,
        table,
    )
    return bool(row)


async def _ensure_intraday_table(
    repo: Optional[AsyncAccountRepository],
    tbl_equity_intraday: str,
    logger: logging.Logger,
) -> None:
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {tbl_equity_intraday} (
        ts                timestamptz PRIMARY KEY,
        deposit           real,
        cumulative_return real,
        drawdown          real,
        realised_return   real,
        updated_at        timestamptz DEFAULT now()
    );
    """
    await _db_execute(repo, logger, ddl)
    await _db_execute(
        repo,
        logger,
        f"""
        ALTER TABLE {tbl_equity_intraday}
        ADD COLUMN IF NOT EXISTS drawdown real
        """,
    )


async def _ensure_tables(
    repo: Optional[AsyncAccountRepository],
    tbl_live: str,
    tbl_closed: str,
    tbl_cash_flows: str,
    tbl_equity_intraday: str,
    tbl_market_schedule: str,
    tbl_metrics: str,
    lock: asyncio.Lock,
    logger: logging.Logger,
) -> None:
    def _schema_of(tbl: str) -> Optional[str]:
        return tbl.split(".", 1)[0] if "." in tbl else None

    def _idx_name(tbl: str, suffix: str) -> str:
        return f"idx_{tbl.replace('.', '_')}_{suffix}"

    schema = _schema_of(tbl_live) or _schema_of(tbl_closed)
    if schema:
        await _db_execute(
            repo,
            logger,
            f"CREATE SCHEMA IF NOT EXISTS {schema};",
        )

    async def _ensure_table(name: str, ddl: str) -> None:
        if await _relation_exists(repo, name):
            return
        if await _type_exists(repo, name):
            try:
                await _drop_type(repo, name, logger)
            except Exception as exc:
                logger.warning("drop type %s failed: %s", name, exc)
                raise
        await _db_execute(repo, logger, ddl)

    async with lock:
        await _ensure_table(
            tbl_live,
            f"""
            CREATE TABLE {tbl_live} (
                parent_id        text PRIMARY KEY,
                symbol           text,
                filled_at        timestamptz,
                qty              real,
                avg_fill         real,
                avg_px_symbol    real,
                sl_child         text,
                sl_px            real,
                tp_child         text,
                tp_px            real,
                mkt_px           real,
                moved_flag       text DEFAULT '—',
                buy_value        real,
                holding_days     integer,
                days_to_expire   integer,
                mkt_value        real,
                profit_loss      real,
                profit_loss_lot  real,
                tp_sl_reach_pct  real,
                updated_at       timestamptz DEFAULT now()
            );
            """,
        )
        await _db_execute(
            repo,
            logger,
            f"""
            ALTER TABLE {tbl_live}
            ADD COLUMN IF NOT EXISTS days_to_expire integer;
            """,
        )
        await _ensure_table(
            tbl_closed,
            f"""
            CREATE TABLE {tbl_closed} (
                entry_lot_id   text,
                exit_order_id  text,
                exit_parent_id text,
                symbol         text,
                side           text,
                qty            real,
                entry_time     timestamptz,
                entry_price    real,
                exit_time      timestamptz,
                exit_price     real,
                exit_type      text,
                pnl_cash       real,
                pnl_cash_fifo  real,
                diff_pnl       real,
                pnl_pct        real,
                return_pct     real,
                duration_sec   real,
                UNIQUE (entry_lot_id, exit_order_id)
            );
            """,
        )
        await _ensure_table(
            tbl_equity_intraday,
            f"""
            CREATE TABLE {tbl_equity_intraday} (
                ts                timestamptz PRIMARY KEY,
                deposit           real,
                cumulative_return real,
                drawdown          real,
                realised_return   real,
                updated_at        timestamptz DEFAULT now()
            );
            """,
        )
        await _db_execute(
            repo,
            logger,
            f"""
            ALTER TABLE {tbl_equity_intraday}
            ADD COLUMN IF NOT EXISTS drawdown real;
            """,
        )
        await _ensure_table(
            tbl_cash_flows,
            f"""
            CREATE TABLE {tbl_cash_flows} (
                id          text PRIMARY KEY,
                ts          timestamptz,
                amount      real,
                type        text,
                description text,
                updated_at  timestamptz DEFAULT now()
            );
            """,
        )
        await _ensure_table(
            tbl_metrics,
            f"""
            CREATE TABLE {tbl_metrics} (
                id              text PRIMARY KEY,
                updated_at      timestamptz DEFAULT now(),
                metrics_payload text
            );
            """,
        )
        await _ensure_table(
            tbl_market_schedule,
            f"""
            CREATE TABLE {tbl_market_schedule} (
                session_date    date PRIMARY KEY,
                pre_open_utc    timestamptz,
                open_utc        timestamptz,
                close_utc       timestamptz,
                post_close_utc  timestamptz,
                updated_at      timestamptz DEFAULT now()
            );
            """,
        )
        await _db_execute(
            repo,
            logger,
            f"""
            CREATE INDEX IF NOT EXISTS
                {_idx_name(tbl_market_schedule, 'open')}
            ON {tbl_market_schedule} (open_utc);
            """,
        )
        await _db_execute(
            repo,
            logger,
            f"""
            CREATE INDEX IF NOT EXISTS
                {_idx_name(tbl_live, 'symbol')}
            ON {tbl_live} (symbol);
            """,
        )
        await _db_execute(
            repo,
            logger,
            f"""
            CREATE INDEX IF NOT EXISTS
                {_idx_name(tbl_closed, 'symbol_time')}
            ON {tbl_closed} (symbol, entry_time);
            """,
        )
