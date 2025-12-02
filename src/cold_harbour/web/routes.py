#!/usr/bin/env python3
"""Blueprint-based web layer for the account dashboards.

This file mirrors the old ``account_web.py`` but exposes a blueprint factory
instead of a module-level Flask app. The actual Flask app will be created in
``cold_harbour.__init__.py`` via ``create_app``.
"""

from __future__ import annotations

import json
import logging
import math
import os
import select
import sys
import time  # used for SSE keep-alive comments
import uuid
from datetime import date, datetime, time as dtime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg2
from flask import (
    Blueprint,
    Response,
    current_app,
    g,
    jsonify,
    render_template,
    request,
)
from sqlalchemy import URL, create_engine, text

from cold_harbour.core.account_utils import (
    calculate_trade_metrics,
    coalesce_exit_slices,
    trading_session_date,
)
from cold_harbour.core.destinations import (
    DESTINATIONS,
    account_table_names,
    notify_channels_for,
    slug_for,
)

# ── config reused from AccountManager ────────────────────────────
PG_DSN = os.getenv(
    "POSTGRESQL_LIVE_CONN_STRING",
    "postgresql://user:pass@localhost:5432/live",
)


def _heartbeat_sec() -> int:
    """Return the shared heartbeat cadence in seconds."""
    for key in ("HEARTBEAT_SEC", "EQUITY_UPDATE_INTERVAL_S"):
        raw = os.getenv(key)
        if not raw:
            continue
        try:
            return max(1, int(raw))
        except ValueError:
            continue
    return 30


HEARTBEAT_SEC = _heartbeat_sec()


# ----- DSN / URL helper -----------------------------------------
def _make_sa_engine():
    """Return SQLAlchemy engine created from a URL or libpq-style DSN.

    The environment variable ``POSTGRESQL_LIVE_CONN_STRING`` can be either a
    SQLAlchemy URL (``postgresql+psycopg2://…``) or a space-separated libpq
    DSN (``host=… user=…``). We support both forms to match the rest of the
    codebase.
    """
    raw = os.getenv("POSTGRESQL_LIVE_CONN_STRING", "")
    if "://" in raw:
        return create_engine(
            raw,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20,
            pool_timeout=5,
            pool_recycle=1800,
        )

    parts = dict(p.split("=", 1) for p in raw.split())
    url = URL.create(
        drivername="postgresql+psycopg2",
        username=parts.get("user"),
        password=parts.get("password"),
        host=parts.get("host", "localhost"),
        port=parts.get("port"),
        database=parts.get("dbname"),
    )
    return create_engine(
        url,
        pool_pre_ping=True,
        pool_size=10,
        max_overflow=20,
        pool_timeout=5,
        pool_recycle=1800,
    )


engine = _make_sa_engine()

# Timescale connection (use SQLAlchemy engine to avoid pandas warnings)
TS_DSN = os.getenv("TIMESCALE_LIVE_CONN_STRING", "")


def _make_ts_engine():
    """Return SQLAlchemy engine for Timescale from URL or libpq DSN.

    Prefers ``TIMESCALE_LIVE_SQLALCHEMY`` if present; otherwise parses the
    libpq-style DSN in ``TIMESCALE_LIVE_CONN_STRING``. Falls back to the
    primary ``engine`` when nothing is configured.
    """
    raw = os.getenv("TIMESCALE_LIVE_SQLALCHEMY") or TS_DSN
    if not raw:
        return engine
    if "://" in raw:
        return create_engine(
            raw,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20,
            pool_timeout=5,
            pool_recycle=1800,
        )
    try:
        parts = dict(p.split("=", 1) for p in raw.split())
        url = URL.create(
            drivername="postgresql+psycopg2",
            username=parts.get("user"),
            password=parts.get("password"),
            host=parts.get("host", "localhost"),
            port=parts.get("port"),
            database=parts.get("dbname"),
        )
        return create_engine(
            url,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=20,
            pool_timeout=5,
            pool_recycle=1800,
        )
    except Exception:
        return engine


ts_engine = _make_ts_engine()

log = logging.getLogger("AccountWeb")
if not log.handlers:
    handler = logging.StreamHandler(stream=sys.stdout)
    fmt = "%(asctime)s %(levelname)s %(name)s: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    handler.setFormatter(logging.Formatter(fmt=fmt, datefmt=datefmt))
    lvl_raw = (
        os.getenv("ACCOUNT_WEB_LOG_LEVEL")
        or os.getenv("LOG_LEVEL")
        or "INFO"
    )
    try:
        lvl = getattr(logging, lvl_raw.strip().upper())
    except Exception:
        lvl = logging.INFO
    log.setLevel(lvl)
    handler.setLevel(logging.NOTSET)
    log.addHandler(handler)
    log.propagate = False

# Hardcoded bars table per requirement
BARS_TABLE = "public.alpaca_bars_1min"

# Toggle SSE streams; set to False to enable live updates.
DISABLE_SSE = False

accounts_bp = Blueprint("accounts", __name__)


def _now() -> float:
    return time.perf_counter()


def _req_id() -> str:
    try:
        return str(getattr(g, "req_id"))
    except Exception:
        return "-"


def _req_account() -> str:
    try:
        return str(getattr(g, "req_account"))
    except Exception:
        return "-"


def _log_timing(
    label: str,
    start_ts: float,
    rows: int | None = None,
    cache: str | None = None,
) -> None:
    try:
        dur_ms = (_now() - start_ts) * 1000.0
        log.debug(
            "timing req_id=%s account=%s op=%s duration_ms=%.1f%s%s",
            _req_id(),
            _req_account(),
            label,
            dur_ms,
            f" rows={rows}" if rows is not None else "",
            f" cache={cache}" if cache else "",
        )
    except Exception:
        pass


def _start_request_timer() -> None:
    try:
        g.req_start = _now()
        g.req_id = uuid.uuid4().hex[:8]
        bp = request.blueprint or ""
        g.req_account = bp.replace("acct_", "", 1) if bp else "-"
        log.debug(
            "http start req_id=%s account=%s path=%s",
            g.req_id,
            g.req_account,
            request.path,
        )
    except Exception:
        pass


def _end_request_timer(resp: Response) -> Response:
    try:
        start_ts = getattr(g, "req_start", None)
        if start_ts is not None:
            dur_ms = (_now() - start_ts) * 1000.0
            log.debug(
                "http done req_id=%s account=%s path=%s status=%s "
                "duration_ms=%.1f",
                _req_id(),
                _req_account(),
                request.path,
                resp.status_code,
                dur_ms,
            )
    except Exception:
        pass
    return resp


def relax_csp(resp: Response) -> Response:
    """Install a permissive CSP that works with CDN assets and inline JS."""
    resp.headers.pop("Content-Security-Policy", None)
    resp.headers["Content-Security-Policy"] = (
        "default-src 'self' https://cdn.jsdelivr.net https://cdnjs.cloudflare.com "
        "https://cdn.datatables.net https://code.jquery.com; "
        "script-src 'self' 'unsafe-inline' 'unsafe-eval' "
        "https://cdn.jsdelivr.net https://cdnjs.cloudflare.com "
        "https://cdn.datatables.net https://code.jquery.com; "
        "style-src 'self' 'unsafe-inline' https://cdn.datatables.net"
    )
    return resp


def register_request_hooks(app) -> None:
    """Attach common request/response hooks to the Flask app."""
    app.before_request(_start_request_timer)
    app.after_request(_end_request_timer)
    app.after_request(relax_csp)


def _make_blueprint_for_dest(dest: dict) -> Blueprint:
    """Create a per-destination blueprint mounted at ``/<slug>``.

    All routes use relative URLs inside the template so the same HTML
    works for every account without changes.
    """
    schema = os.getenv("ACCOUNT_SCHEMA", "accounts")
    tables = account_table_names(dest, schema=schema)
    if "schedule" in tables and "market_schedule" not in tables:
        tables["market_schedule"] = tables["schedule"]
    # Derive intraday equity and cash_flows table names from full equity table
    try:
        eq_full = tables.get("equity")
        if eq_full:
            if "." in eq_full:
                sch, base = eq_full.split(".", 1)
                if base.startswith("equity_full_"):
                    tables["equity_intraday"] = f"{sch}." + base.replace("equity_full_", "equity_intraday_", 1)
                    tables["cash_flows"] = f"{sch}." + base.replace("equity_full_", "cash_flows_", 1)
                else:
                    tables["equity_intraday"] = f"{eq_full}_intraday"
                    tables["cash_flows"] = f"{sch}.cash_flows_{base}"
            else:
                tables["equity_intraday"] = eq_full.replace("equity_full_", "equity_intraday_", 1)
                tables["cash_flows"] = eq_full.replace("equity_full_", "cash_flows_", 1)
    except Exception:
        pass
    chans = notify_channels_for(dest)
    slug = slug_for(dest)

    bp = Blueprint(f"acct_{slug}", __name__, url_prefix=f"/{slug}")

    # In-memory TTL caches per destination to avoid heavy recompute on tab open
    common_ttl = HEARTBEAT_SEC
    _cache = {
        "metrics": {"ts": None, "data": None, "ttl": common_ttl},
        "equity": {"ts": None, "data": None, "ttl": common_ttl},
    }

    def _fetch_open_df() -> pd.DataFrame:
        start_ts = _now()
        with engine.begin() as conn:
            sql = text(
                f"SELECT * FROM {tables['open']} ORDER BY symbol"
            )
            df = pd.read_sql(sql, conn)
        _log_timing("positions_sql", start_ts, len(df.index))
        return df

    def _open_version() -> str:
        start_ts = _now()
        with engine.begin() as conn:
            row = conn.execute(
                text(
                    f"SELECT COALESCE(MAX(updated_at), NOW()) AS max_upd, COUNT(*) AS cnt FROM {tables['open']}"
                )
            ).mappings().first()
        _log_timing("open_version", start_ts, int(row["cnt"] or 0))
        max_upd = row["max_upd"]
        cnt = int(row["cnt"] or 0)
        # ETag must be a string; include count to detect row adds/removes
        return f"{pd.to_datetime(max_upd, utc=True).isoformat()}-{cnt}"

    def _fetch_closed_df() -> pd.DataFrame:
        start_ts = _now()
        with engine.begin() as conn:
            sql = text(
                f"""
                SELECT symbol, side, qty,
                       entry_time AT TIME ZONE 'UTC'  AS entry_time,
                       exit_time  AT TIME ZONE 'UTC'  AS exit_time,
                       entry_price, exit_price,
                       exit_type, exit_order_id,
                       pnl_cash, return_pct
                  FROM {tables['closed']}
                 ORDER BY exit_time DESC
                """
            )
            df = pd.read_sql(sql, conn)
        _log_timing("closed_sql_full", start_ts, len(df.index))
        return df

    @bp.route("/")
    def home() -> str:
        """Render dashboard with a simple account tab bar.

        Pass the list of available destinations (slug + display name) and
        the current slug so the template can highlight the active tab.
        """
        tabs = [
            {"slug": slug_for(d), "name": d.get("name", "account")}
            for d in DESTINATIONS
        ]
        return render_template(
            "account_positions.html",
            tabs=tabs,
            current_slug=slug,
            heartbeat_ms=HEARTBEAT_SEC * 1000,
        )

    @bp.route("/api/positions")
    def api_positions() -> Response:  # type: ignore[override]
        start_ts = _now()
        df = _fetch_open_df()
        safe = json.loads(
            df.to_json(orient="records", date_format="iso")
        )
        _log_timing("positions", start_ts, len(df.index))
        return jsonify(safe)

    @bp.route("/api/closed")
    def api_closed() -> Response:  # type: ignore[override]
        start_ts = _now()
        # Return only the most recent 300 rows for UI speed.
        with engine.begin() as conn:
            sql = text(
                f"""
                SELECT symbol, side, qty,
                       entry_time AT TIME ZONE 'UTC'  AS entry_time,
                       exit_time  AT TIME ZONE 'UTC'  AS exit_time,
                       entry_price, exit_price,
                       exit_type, exit_order_id,
                       pnl_cash, return_pct
                  FROM {tables['closed']}
              ORDER BY exit_time DESC
                 LIMIT 300
                """
            )
            df = pd.read_sql(sql, conn)
        _log_timing("closed", start_ts, len(df.index))
        return jsonify(
            json.loads(df.to_json(orient="records", date_format="iso"))
        )

    @bp.route("/api/positions_cached")
    def api_positions_cached() -> Response:  # type: ignore[override]
        # Build ETag from DB version (max(updated_at) + count)
        etag = _open_version()
        inm = request.headers.get("If-None-Match")
        if inm == etag:
            _log_timing("positions_cached", _now(), cache="hit")
            return Response(status=304)
        hit_start = _now()
        df = _fetch_open_df()
        payload = json.loads(df.to_json(orient="records", date_format="iso"))
        resp = jsonify(payload)
        resp.headers["ETag"] = etag
        resp.headers["Cache-Control"] = "public, max-age=30, must-revalidate"
        _log_timing("positions_cached", hit_start, len(df.index), "miss")
        return resp

    @bp.route("/api/closed_cached")
    def api_closed_cached() -> Response:  # type: ignore[override]
        # Compute a lightweight ETag based on latest exit_time and limited count
        with engine.begin() as conn:
            ver_row = conn.execute(
                text(
                    f"SELECT COALESCE(MAX(exit_time), NOW()) AS max_exit FROM {tables['closed']}"
                )
            ).mappings().first()
        etag = pd.to_datetime(ver_row["max_exit"], utc=True).isoformat()
        inm = request.headers.get("If-None-Match")
        if inm == etag:
            _log_timing("closed_cached", _now(), cache="hit")
            return Response(status=304)
        hit_start = _now()
        with engine.begin() as conn:
            sql = text(
                f"""
                SELECT symbol, side, qty,
                       entry_time AT TIME ZONE 'UTC'  AS entry_time,
                       exit_time  AT TIME ZONE 'UTC'  AS exit_time,
                       entry_price, exit_price,
                       exit_type, exit_order_id,
                       pnl_cash, return_pct
                  FROM {tables['closed']}
              ORDER BY exit_time DESC
                 LIMIT 300
                """
            )
            df = pd.read_sql(sql, conn)
        payload = json.loads(df.to_json(orient="records", date_format="iso"))
        resp = jsonify(payload)
        resp.headers["ETag"] = etag
        resp.headers["Cache-Control"] = "public, max-age=30, must-revalidate"
        _log_timing("closed_cached", hit_start, len(df.index), "miss")
        return resp

    @bp.route("/stream/closed")
    def stream_closed() -> Response:  # type: ignore[override]
        if DISABLE_SSE:
            log.debug("SSE closed disabled; returning 204")
            return Response(status=204)

        def event_stream():
            conn = psycopg2.connect(PG_DSN)
            conn.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
            )
            cur = conn.cursor()
            cur.execute(f"LISTEN {chans['closed']};")
            try:
                yield ": connected\n\n"
                while True:
                    if select.select([conn], [], [], 5) == ([], [], []):
                        yield ": ping\n\n"
                        continue
                    conn.poll()
                    while conn.notifies:
                        conn.notifies.pop(0)
                        yield "data: refresh\n\n"
            except GeneratorExit:
                pass
            except Exception:
                pass
            finally:
                try:
                    cur.close()
                except Exception:
                    pass
                try:
                    conn.close()
                except Exception:
                    pass

        return Response(
            event_stream(),
            mimetype="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )

    @bp.route("/api/metrics")
    def api_metrics() -> Response:  # type: ignore[override]
        payload: dict[str, Any] | list[Any] = {}
        with engine.begin() as conn:
            row = conn.execute(
                text(
                    f"""
                    SELECT metrics_payload
                      FROM {tables.get('metrics', 'account_metrics')}
                  ORDER BY updated_at DESC
                     LIMIT 1
                    """
                )
            ).scalar_one_or_none()
        if row:
            try:
                payload = json.loads(row)
            except json.JSONDecodeError:
                payload = {}
        # Keep compatibility: frontend accepts either [{…}] or {…}
        if isinstance(payload, dict):
            return jsonify([payload])
        return jsonify(payload)

    @bp.route("/stream/positions")
    def stream_positions() -> Response:  # type: ignore[override]
        if DISABLE_SSE:
            log.debug("SSE positions disabled; returning 204")
            return Response(status=204)

        def event_stream():
            conn = psycopg2.connect(PG_DSN)
            conn.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
            )
            cur = conn.cursor()
            cur.execute(f"LISTEN {chans['pos']};")

            try:
                yield ": connected\n\n"
                while True:
                    # shorter timeout for quicker disconnect detection
                    if select.select([conn], [], [], 5) == ([], [], []):
                        yield ": ping\n\n"
                        continue
                    conn.poll()
                    while conn.notifies:
                        note = conn.notifies.pop(0)
                        yield f"data: {note.payload}\n\n"
            except GeneratorExit:
                pass
            except Exception:
                pass
            finally:
                try:
                    cur.close()
                except Exception:
                    pass
                try:
                    conn.close()
                except Exception:
                    pass

        return Response(
            event_stream(),
            mimetype="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )

    @bp.route("/stream/events")
    def stream_events() -> Response:  # type: ignore[override]
        if DISABLE_SSE:
            log.debug("SSE events disabled; returning 204")
            return Response(status=204)

        def event_stream():
            conn = psycopg2.connect(PG_DSN)
            conn.set_isolation_level(
                psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT
            )
            cur = conn.cursor()
            cur.execute(f"LISTEN {chans['pos']};")
            cur.execute(f"LISTEN {chans['closed']};")
            try:
                yield ": connected\n\n"
                while True:
                    if select.select([conn], [], [], 5) == ([], [], []):
                        yield ": ping\n\n"
                        continue
                    conn.poll()
                    while conn.notifies:
                        note = conn.notifies.pop(0)
                        channel = (
                            "pos"
                            if note.channel == chans["pos"]
                            else "closed"
                        )
                        try:
                            payload = json.dumps(
                                {"channel": channel, "payload": note.payload}
                            )
                            yield f"data: {payload}\n\n"
                        except Exception:
                            pass
            except GeneratorExit:
                pass
            except Exception:
                pass
            finally:
                try:
                    cur.close()
                except Exception:
                    pass
                try:
                    conn.close()
                except Exception:
                    pass

        return Response(
            event_stream(),
            mimetype="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )

    @bp.route("/api/equity")
    def api_equity() -> Response:  # type: ignore[override]
        start_ts = _now()
        # Optional window parameter.
        # Defaults to 'first_trade' to avoid long flat periods before trading
        # actually started. Other accepted values: 'all', '<Nd>' (e.g. 365d).
        window = (request.args.get("window") or "first_trade").strip().lower()

        # TTL cache per window mode
        cache_key = f"equity:{window}"
        try:
            now = pd.Timestamp.utcnow()
            ent = _cache.get(cache_key, {})
            ts = ent.get("ts")
            if ts is not None and (now - ts).total_seconds() < ent.get("ttl", 45):
                _log_timing("equity", start_ts, cache="hit")
                return jsonify(ent.get("data") or [])
        except Exception:
            pass

        # Build WHERE clause
        where = ""
        params: dict[str, Any] = {}
        if window == "all":
            where = ""
        elif window.endswith("d") and window[:-1].isdigit():
            days = int(window[:-1])
            where = f"WHERE date >= (CURRENT_DATE - INTERVAL '{days} days')"
        else:
            # 'first_trade' behaviour: start from earliest trade fill
            with engine.begin() as conn:
                try:
                    cut = conn.execute(
                        text(
                            f"""
                            SELECT MIN(ts::date)
                              FROM {tables['cash_flows']}
                             WHERE type ILIKE 'FILL%'
                            """
                        )
                    ).scalar()
                except Exception:
                    cut = None
            if cut is not None:
                where = "WHERE date >= :cut"
                params["cut"] = cut
            else:
                # fallback: last 365 days
                where = "WHERE date >= (CURRENT_DATE - INTERVAL '365 days')"

        with engine.begin() as conn:
            df = pd.read_sql(
                text(
                    f"""
                    SELECT date, deposit, cumulative_return, realised_return, drawdown
                      FROM {tables['equity']}
                      {where}
                     ORDER BY date
                    """
                ),
                conn,
                params=params or None,
            )
        payload = json.loads(df.to_json(orient="records", date_format="iso"))
        try:
            ttl = _cache.get("equity", {}).get("ttl", 45)
            _cache[cache_key] = {"ts": pd.Timestamp.utcnow(), "data": payload, "ttl": ttl}
        except Exception:
            pass
        _log_timing("equity", start_ts, len(df.index), "miss")
        return jsonify(payload)

    @bp.route("/api/equity_intraday")
    def api_equity_intraday() -> Response:  # type: ignore[override]
        """Return intraday equity as cash P/L relative to yesterday.

        Option A: read precomputed points from the manager's
        ``equity_intraday`` table and compute ``pl_cash = deposit -
        prev_deposit``. This eliminates duplicated math and keeps the UI
        aligned with the daily series. Falls back to a zero series when
        the table is empty.
        """
        start_ts = _now()
        # TTL cache ~30s
        def _to_float(value, default=None):
            try:
                if value is None:
                    return default
                f = float(value)
                return f if math.isfinite(f) else default
            except (TypeError, ValueError):
                return default

        def _to_iso(value):
            if not value:
                return None
            try:
                return pd.to_datetime(value, utc=True).isoformat()
            except Exception:
                try:
                    return pd.to_datetime(value).isoformat()
                except Exception:
                    return None

        ttl_seconds = min(HEARTBEAT_SEC, 10)
        try:
            now = pd.Timestamp.utcnow()
            ent = _cache.get("equity_intraday", {})
            ts = ent.get("ts")
            fresh = ts is not None and (
                (now - ts).total_seconds() < ent.get("ttl", ttl_seconds)
            )
            if fresh:
                _log_timing("equity_intraday", start_ts, cache="hit")
                return jsonify(ent.get("data") or {"series": [], "meta": {}})
        except Exception:
            pass

        start_param = (request.args.get("start") or "pre").strip().lower()

        def _start_seconds(value: str) -> int:
            if value in {"pre", "premarket", "pm"}:
                return 4 * 3600
            if value in {"rth", "regular"}:
                return 9 * 3600 + 30 * 60
            try:
                if ":" in value:
                    hh, mm = value.split(":", 1)
                    return int(hh) * 3600 + int(mm) * 60
                return int(value) * 3600
            except Exception:
                return 4 * 3600

        tz_name = "America/New_York"
        now_utc = pd.Timestamp.now(tz="UTC")
        schedule_tbl = tables.get("market_schedule") or tables.get("schedule")
        session_row = None
        if schedule_tbl:
            try:
                with engine.begin() as conn:
                    session_row = conn.execute(
                        text(
                            f"""
                            SELECT session_date, pre_open_utc, post_close_utc
                              FROM {schedule_tbl}
                             WHERE pre_open_utc <= :now_ts
                               AND post_close_utc > :now_ts
                          ORDER BY pre_open_utc DESC
                             LIMIT 1
                            """
                        ),
                        {"now_ts": now_utc.to_pydatetime()},
                    ).mappings().first()
                    if session_row is None:
                        session_row = conn.execute(
                            text(
                                f"""
                                SELECT session_date, pre_open_utc, post_close_utc
                                  FROM {schedule_tbl}
                                 WHERE post_close_utc <= :now_ts
                              ORDER BY post_close_utc DESC
                                 LIMIT 1
                                """
                            ),
                            {"now_ts": now_utc.to_pydatetime()},
                        ).mappings().first()
            except Exception as exc:
                session_row = None
                current_app.logger.debug(
                    "schedule lookup failed (%s); falling back to legacy window",
                    exc,
                )

        if session_row:
            sess_date = session_row["session_date"]
            pre_open_ts = pd.to_datetime(session_row["pre_open_utc"], utc=True)
            post_close_ts = pd.to_datetime(
                session_row["post_close_utc"], utc=True
            )
            start_offset = _start_seconds(start_param)
            start_local = datetime.combine(
                sess_date,
                dtime(0, 0, 0),
                tzinfo=ZoneInfo(tz_name),
            ) + timedelta(seconds=start_offset)
            start_utc = pd.to_datetime(start_local, utc=True)
            if start_utc < pre_open_ts:
                start_utc = pre_open_ts
            if start_utc > post_close_ts:
                start_utc = pre_open_ts
            last_complete = (
                (now_utc.floor("min") - pd.Timedelta(seconds=5)).floor("min")
            )
            if last_complete < start_utc:
                last_complete = start_utc
            end_utc = (
                post_close_ts
                if last_complete >= post_close_ts
                else last_complete
            )
        else:
            start_sec = _start_seconds(start_param)
            sess_date = trading_session_date(now_utc.to_pydatetime())
            start_local = datetime.combine(
                sess_date,
                dtime(0, 0, 0),
                tzinfo=ZoneInfo(tz_name),
            ) + timedelta(seconds=start_sec)
            start_utc = pd.to_datetime(start_local, utc=True)
            close_local = datetime.combine(
                sess_date,
                dtime(20, 0, 0),
                tzinfo=ZoneInfo(tz_name),
            )
            post_close_ts = pd.to_datetime(close_local, utc=True)
            last_complete = (
                (now_utc.floor("min") - pd.Timedelta(seconds=5)).floor("min")
            )
            if last_complete < start_utc:
                last_complete = start_utc
            end_utc = last_complete if last_complete <= post_close_ts else post_close_ts

        # Anchor deposit: strictly before the session date (base for this session)
        with engine.begin() as conn:
            prev_row = conn.execute(
                text(
                    f"""
                    SELECT deposit
                      FROM {tables['equity']}
                     WHERE date < :d
                  ORDER BY date DESC
                     LIMIT 1
                    """
                ),
                {"d": sess_date},
            ).mappings().first()
            prev_deposit = float(prev_row["deposit"]) if prev_row else 0.0

        # Use minute frequency (alias 'T' is deprecated)
        idx = pd.date_range(start_utc, end_utc, freq="min", tz="UTC")

        # Pull precomputed deposits from intraday table
        with engine.begin() as conn:
            df = pd.read_sql(
                text(
                    f"""
                    SELECT ts AT TIME ZONE 'UTC' AS ts, deposit
                      FROM {tables['equity_intraday']}
                     WHERE ts >= :start AND ts <= :end
                  ORDER BY ts
                    """
                ),
                conn,
                params={
                    "start": start_utc.to_pydatetime(),
                    "end": end_utc.to_pydatetime(),
                },
            )

        # Compose flows cumulative time series over idx
        with engine.begin() as conn:
            try:
                raw_types = os.getenv(
                    "CASH_FLOW_TYPES",
                    (
                        "CSD,CSW,JNLC,ACATC,ACATS,FEE,CFEE,DIV,DIVCGL,"
                        "DIVCGS,DIVNRA,DIVROC,DIVTXEX,DIVWH,INT,INTPNL"
                    ),
                )
                pats = [t.strip().upper() for t in raw_types.split(',') if t.strip()]
                where_types = " OR ".join([f"type ILIKE '{p}%'" for p in pats]) or "FALSE"
                fdf = pd.read_sql(
                    text(
                        f"""
                        SELECT ts AT TIME ZONE 'UTC' AS ts, amount
                          FROM {tables['cash_flows']}
                         WHERE ts >= :start AND ts <= :end
                           AND ({where_types})
                        ORDER BY ts
                        """
                    ),
                    conn,
                    params={
                        "start": start_utc.to_pydatetime(),
                        "end": end_utc.to_pydatetime(),
                    },
                )
            except Exception:
                fdf = pd.DataFrame(columns=["ts", "amount"])

        if df.empty:
            empty_series = pd.DataFrame({
                "ts": idx,
                "deposit": 0.0,
                "pl_cash": 0.0,
                "drawdown_cash": 0.0,
                "source": "bar",
            })
            series_payload = json.loads(
                empty_series.to_json(orient="records", date_format="iso")
            )
            meta = {
                "baseline_deposit": _to_float(prev_deposit, None),
                "baseline_ts": _to_iso(start_utc),
                "prev_deposit": _to_float(prev_deposit, None),
                "session_flows": 0.0,
                "session_realised": 0.0,
                "live_deposit": None,
                "live_ts": None,
                "session_start": _to_iso(start_utc),
                "session_date": str(sess_date),
            }
            payload = {"series": series_payload, "meta": meta}
            try:
                _cache["equity_intraday"] = {
                    "ts": pd.Timestamp.utcnow(),
                    "data": payload,
                    "ttl": ttl_seconds,
                }
            except Exception:
                pass
            return jsonify(payload)

        series = (
            df.assign(ts=pd.to_datetime(df["ts"], utc=True))
            .drop_duplicates("ts")
            .set_index("ts")["deposit"]
            .reindex(idx)
            .ffill()
        )
        # Drawdown in dollars (dep - rolling peak)
        dd_series = series - series.cummax()

        # Flows cumulative series on the same index
        if not fdf.empty:
            fmin = pd.to_datetime(fdf["ts"], utc=True).dt.floor("min")
            fser = (
                fdf.assign(_m=fmin)
                .groupby("_m")["amount"].sum()
                .reindex(idx, fill_value=0.0)
                .cumsum()
            )
        else:
            fser = pd.Series(0.0, index=idx)

        pl = series - float(prev_deposit) - fser
        out = pd.DataFrame(
            {
                "ts": idx,
                "deposit": series.astype(float).to_numpy(),
                "pl_cash": pl.astype(float).to_numpy(),
                "drawdown_cash": dd_series.astype(float).to_numpy(),
                "source": "bar",
            }
        )

        out = out.sort_values("ts").reset_index(drop=True)
        series_payload = json.loads(
            out.to_json(orient="records", date_format="iso")
        )
        latest_dep = _to_float(
            out.iloc[-1]["deposit"] if not out.empty else None,
            None,
        )
        latest_ts = _to_iso(
            out.iloc[-1]["ts"] if not out.empty else None
        )
        baseline_dep = _to_float(prev_deposit, None)
        baseline_ts = _to_iso(start_utc)
        session_flows = _to_float(
            fser.iloc[-1] if not fser.empty else 0.0, 0.0
        )
        session_realised = None
        live_dep = latest_dep
        live_ts = latest_ts
        meta = {
            "baseline_deposit": baseline_dep,
            "baseline_ts": baseline_ts,
            "prev_deposit": _to_float(prev_deposit, None),
            "session_flows": session_flows,
            "session_realised": session_realised,
            "live_deposit": live_dep,
            "live_ts": live_ts,
            "latest_deposit": latest_dep,
            "latest_ts": latest_ts,
            "session_start": _to_iso(start_utc),
            "session_date": str(sess_date),
        }
        payload = {"series": series_payload, "meta": meta}
        try:
            _cache["equity_intraday"] = {
                "ts": pd.Timestamp.utcnow(),
                "data": payload,
                "ttl": ttl_seconds,
            }
        except Exception:
            pass
        _log_timing(
            "equity_intraday",
            start_ts,
            len(series.get("ts", [])),
            "miss",
        )
        return jsonify(payload)

    return bp


@accounts_bp.route("/")
def index() -> str:
    """Render a simple index linking all account dashboards.

    Kept intentionally minimal to avoid adding templates/routes beyond the
    scope. Users can bookmark their preferred ``/<slug>/`` address.
    """
    links = [
        (slug_for(d), d.get("name", "account")) for d in DESTINATIONS
    ]
    items = "".join(
        f'<li><a href="/{s}/">{n} ({s})</a></li>' for s, n in links
    )
    return (
        "<h1>Accounts</h1>"
        "<ul>" + items + "</ul>"
    )


# Pre-build the per-destination blueprints so create_app can register them.
ACCOUNT_BLUEPRINTS = [_make_blueprint_for_dest(d) for d in DESTINATIONS]
