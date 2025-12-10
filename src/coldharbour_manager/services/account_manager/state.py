"""State helpers for the AccountManager runtime."""

from __future__ import annotations

import json
from typing import Any, Dict, Optional, TYPE_CHECKING

from coldharbour_manager.services.account_manager import snapshot
from coldharbour_manager.services.account_manager.config import DB_COLS
from coldharbour_manager.services.account_manager.utils import (
    _json_safe,
    _utcnow,
)

if TYPE_CHECKING:  # pragma: no cover - typing only
    from coldharbour_manager.services.account_manager.runtime import (
        AccountManager,
    )


async def upsert_live_row(
    mgr: "AccountManager",
    row: Dict[str, Any],
    *,
    force_push: bool = False,
) -> None:
    """Upsert a live parent row and push NOTIFY if needed."""

    cols = DB_COLS
    vals = [row.get(c) for c in cols]
    sets = ",".join(
        f"{c}=EXCLUDED.{c}" for c in cols if c != "parent_id"
    )

    parent_id = row["parent_id"]
    new_px = row.get("mkt_px")
    prev_state = mgr.state.get(parent_id)
    last_px = (prev_state or {}).get("_last_pushed_px")

    push_needed = force_push or (
        last_px is None
        or (
            new_px
            and last_px
            and abs(new_px - last_px) / last_px * 100
            >= mgr.UI_PUSH_PCT_THRESHOLD
        )
    )

    if not push_needed and prev_state:
        for k in (
            "qty",
            "avg_fill",
            "avg_px_symbol",
            "sl_child",
            "sl_px",
            "tp_child",
            "tp_px",
            "moved_flag",
        ):
            if row.get(k) != prev_state.get(k):
                push_needed = True
                break

    placeholders = ", ".join(f"${i}" for i in range(1, len(cols) + 1))
    await mgr._db_execute(
        f"""
            INSERT INTO {mgr.tbl_live} ({','.join(cols)})
            VALUES ({placeholders})
            ON CONFLICT (parent_id) DO UPDATE SET {sets};
            """,
        *vals,
    )

    if push_needed:
        payload = {c: _json_safe(row.get(c)) for c in cols}
        await mgr._db_execute(
            "SELECT pg_notify($1, $2);",
            mgr.pos_channel,
            json.dumps({"row": payload}),
        )
        row["_last_pushed_px"] = new_px

    mgr.state[parent_id] = row


async def delete_parent_row(
    mgr: "AccountManager",
    parent_id: str,
    *,
    skip_closed: bool = False,
    exit_px: Optional[float] = None,
    exit_type: str = "MANUAL",
) -> None:
    """Delete a parent row, write closed row, and notify clients."""

    row = mgr.state.get(parent_id)

    if row and not skip_closed:
        cp = row.copy()
        cp["qty"] = 0.0
        cp["mkt_px"] = exit_px or cp.get("mkt_px") or cp["avg_fill"]
        snapshot.refresh_row_metrics(cp, _utcnow())
        await mgr._record_closed_trade_now(cp, exit_type=exit_type)

    await mgr._db_execute(
        f"DELETE FROM {mgr.tbl_live} WHERE parent_id = $1;", parent_id
    )
    await mgr._db_execute(
        "SELECT pg_notify($1, $2);",
        mgr.pos_channel,
        json.dumps({"op": "delete", "parent_id": parent_id}),
    )

    mgr.state.pop(parent_id, None)
    sym = row.get("symbol") if row else None
    if sym:
        s = mgr.sym2pid.get(sym)
        if s:
            s.discard(parent_id)
            if not s:
                mgr.sym2pid.pop(sym, None)
