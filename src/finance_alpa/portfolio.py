"""Portfolio persistence layer (multi-portfolio).

Tables: `portfolios` (id, name) and `positions` (portfolio_id, symbol, ...).
`cost_basis` is stored as **per-share**, matching the config.yaml convention.
Users can maintain multiple named portfolios via the UI.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from .config import DEFAULT_PORTFOLIO_NAME, Position, load_app_config
from .db import connect

logger = logging.getLogger(__name__)

ImportMode = Literal["upsert", "replace"]


@dataclass(frozen=True)
class DbPortfolio:
    id: int
    name: str


@dataclass(frozen=True)
class DbPosition:
    portfolio_id: int
    symbol: str
    shares: float
    cost_basis: float | None
    account: str | None


# --- Portfolios ---------------------------------------------------------

def list_portfolios() -> list[DbPortfolio]:
    with connect() as con:
        rows = con.execute(
            "SELECT id, name FROM portfolios ORDER BY created_at, id"
        ).fetchall()
    return [DbPortfolio(*r) for r in rows]


def get_portfolio_by_name(name: str) -> DbPortfolio | None:
    with connect() as con:
        row = con.execute(
            "SELECT id, name FROM portfolios WHERE name = ?", [name]
        ).fetchone()
    return DbPortfolio(*row) if row else None


def create_portfolio(name: str) -> DbPortfolio:
    name = name.strip()
    if not name:
        raise ValueError("Portfolio name cannot be empty")
    with connect() as con:
        con.execute("INSERT INTO portfolios (name) VALUES (?)", [name])
        row = con.execute(
            "SELECT id, name FROM portfolios WHERE name = ?", [name]
        ).fetchone()
    logger.info("Created portfolio %r", name)
    return DbPortfolio(*row)


def rename_portfolio(portfolio_id: int, new_name: str) -> None:
    new_name = new_name.strip()
    if not new_name:
        raise ValueError("Portfolio name cannot be empty")
    with connect() as con:
        con.execute(
            "UPDATE portfolios SET name = ? WHERE id = ?", [new_name, portfolio_id]
        )
    logger.info("Renamed portfolio %d → %r", portfolio_id, new_name)


def delete_portfolio(portfolio_id: int) -> None:
    with connect() as con:
        con.execute("DELETE FROM positions WHERE portfolio_id = ?", [portfolio_id])
        con.execute("DELETE FROM portfolios WHERE id = ?", [portfolio_id])
    logger.info("Deleted portfolio %d (and its positions)", portfolio_id)


def _get_or_create(con, name: str) -> int:
    con.execute(
        "INSERT INTO portfolios (name) VALUES (?) ON CONFLICT (name) DO NOTHING",
        [name],
    )
    return con.execute(
        "SELECT id FROM portfolios WHERE name = ?", [name]
    ).fetchone()[0]


# --- Positions ----------------------------------------------------------

def get_positions(portfolio_id: int) -> list[DbPosition]:
    with connect() as con:
        rows = con.execute(
            """
            SELECT portfolio_id, symbol, shares, cost_basis, account
            FROM positions WHERE portfolio_id = ? ORDER BY symbol
            """,
            [portfolio_id],
        ).fetchall()
    return [DbPosition(*r) for r in rows]


def get_all_positions() -> list[DbPosition]:
    with connect() as con:
        rows = con.execute(
            """
            SELECT portfolio_id, symbol, shares, cost_basis, account
            FROM positions ORDER BY portfolio_id, symbol
            """
        ).fetchall()
    return [DbPosition(*r) for r in rows]


def clear_positions(portfolio_id: int) -> int:
    with connect() as con:
        before = con.execute(
            "SELECT COUNT(*) FROM positions WHERE portfolio_id = ?", [portfolio_id]
        ).fetchone()[0]
        con.execute("DELETE FROM positions WHERE portfolio_id = ?", [portfolio_id])
    logger.info("Cleared %d positions from portfolio %d", before, portfolio_id)
    return before


def upsert_positions(
    portfolio_id: int, rows: list[Position], mode: ImportMode = "upsert"
) -> int:
    if not rows:
        if mode == "replace":
            clear_positions(portfolio_id)
        return 0
    now = datetime.now()
    with connect() as con:
        if mode == "replace":
            con.execute("DELETE FROM positions WHERE portfolio_id = ?", [portfolio_id])
        for p in rows:
            con.execute(
                """
                INSERT INTO positions (portfolio_id, symbol, shares, cost_basis, account, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (portfolio_id, symbol) DO UPDATE SET
                    shares = EXCLUDED.shares,
                    cost_basis = EXCLUDED.cost_basis,
                    account = EXCLUDED.account,
                    updated_at = EXCLUDED.updated_at
                """,
                [portfolio_id, p.symbol, float(p.shares), p.cost_basis, p.account, now],
            )
    logger.info(
        "Upserted %d positions into portfolio %d (mode=%s)", len(rows), portfolio_id, mode
    )
    return len(rows)


# --- Config sync --------------------------------------------------------

def sync_from_config() -> int:
    """Replace every portfolio in the DB with those listed in config.yaml."""
    cfg = load_app_config()
    total = 0
    with connect() as con:
        # Wipe everything, then recreate each named portfolio from config.
        con.execute("DELETE FROM positions")
        con.execute("DELETE FROM portfolios")
        for pf in cfg.portfolios:
            pf_id = _get_or_create(con, pf.name)
            for p in pf.positions:
                con.execute(
                    """
                    INSERT INTO positions (portfolio_id, symbol, shares, cost_basis, account, updated_at)
                    VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """,
                    [pf_id, p.symbol, float(p.shares), p.cost_basis, p.account],
                )
                total += 1
    logger.info("Seeded %d positions across %d portfolios from config.yaml",
                total, len(cfg.portfolios))
    return total


def seed_from_config_if_empty() -> int:
    """First-run convenience: populate DB portfolios from config.yaml when the
    DB has no portfolios at all. Never clobbers user edits on subsequent runs.
    """
    cfg = load_app_config()
    if not cfg.portfolios:
        return 0
    with connect() as con:
        count = con.execute("SELECT COUNT(*) FROM portfolios").fetchone()[0]
    if count:
        return 0
    return sync_from_config()


# --- Symbols tracked for ingest ----------------------------------------

def tracked_symbols() -> list[str]:
    """Symbols to ingest: watchlist ∪ union of positions across all portfolios.

    Falls back to config.yaml positions when the DB has no positions yet, so
    first-time users don't need to seed before ingest makes sense.
    """
    cfg = load_app_config()
    seen: dict[str, None] = {}
    for s in cfg.universe.watchlist:
        seen.setdefault(s, None)
    db_positions = get_all_positions()
    if db_positions:
        for p in db_positions:
            seen.setdefault(p.symbol, None)
    else:
        for p in cfg.all_positions:
            seen.setdefault(p.symbol, None)
    return list(seen.keys())
