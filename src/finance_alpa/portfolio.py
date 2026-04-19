"""Portfolio persistence layer.

The DB `portfolio` table is the source of truth for positions. Users can seed
it from `config.yaml` or import a broker CSV via the UI. `cost_basis` is stored
as **per-share**, matching the `config.yaml` convention.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from .config import Position, load_app_config
from .db import connect

logger = logging.getLogger(__name__)

ImportMode = Literal["upsert", "replace"]


@dataclass(frozen=True)
class DbPosition:
    symbol: str
    shares: float
    cost_basis: float | None
    account: str | None


def get_positions() -> list[DbPosition]:
    with connect(read_only=True) as con:
        rows = con.execute(
            "SELECT symbol, shares, cost_basis, account FROM portfolio ORDER BY symbol"
        ).fetchall()
    return [DbPosition(*r) for r in rows]


def clear_positions() -> int:
    with connect() as con:
        before = con.execute("SELECT COUNT(*) FROM portfolio").fetchone()[0]
        con.execute("DELETE FROM portfolio")
    logger.info("Cleared %d portfolio rows", before)
    return before


def upsert_positions(rows: list[Position], mode: ImportMode = "upsert") -> int:
    """Insert or update positions. `rows[i].cost_basis` is per-share."""
    if not rows:
        return 0
    now = datetime.now()
    with connect() as con:
        if mode == "replace":
            con.execute("DELETE FROM portfolio")
        for p in rows:
            con.execute(
                """
                INSERT INTO portfolio (symbol, shares, cost_basis, account, updated_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT (symbol) DO UPDATE SET
                    shares = EXCLUDED.shares,
                    cost_basis = EXCLUDED.cost_basis,
                    account = EXCLUDED.account,
                    updated_at = EXCLUDED.updated_at
                """,
                [p.symbol, float(p.shares), p.cost_basis, p.account, now],
            )
    logger.info("Upserted %d positions (mode=%s)", len(rows), mode)
    return len(rows)


def sync_from_config() -> int:
    """Replace the DB portfolio with positions listed in config.yaml."""
    cfg = load_app_config()
    return upsert_positions(list(cfg.portfolio), mode="replace")


def tracked_symbols() -> list[str]:
    """Symbols to ingest against: watchlist (config) ∪ portfolio (DB).

    Falls back to config.yaml portfolio when the DB portfolio is empty, so
    first-time users don't have to seed the DB before ingest makes sense.
    """
    cfg = load_app_config()
    seen: dict[str, None] = {}
    for s in cfg.universe.watchlist:
        seen.setdefault(s, None)
    db_positions = get_positions()
    if db_positions:
        for p in db_positions:
            seen.setdefault(p.symbol, None)
    else:
        for p in cfg.portfolio:
            seen.setdefault(p.symbol, None)
    return list(seen.keys())
