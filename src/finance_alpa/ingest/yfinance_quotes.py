"""Ingest daily OHLCV quotes from Yahoo Finance for all configured symbols."""
from __future__ import annotations

import logging

import pandas as pd
import yfinance as yf

from ..db import connect
from ..portfolio import tracked_symbols

logger = logging.getLogger(__name__)


def _fnum(v) -> float | None:
    if v is None or pd.isna(v):
        return None
    return float(v)


def _inum(v) -> int | None:
    if v is None or pd.isna(v):
        return None
    return int(v)


def fetch_symbol(symbol: str, period: str = "3mo") -> list[tuple]:
    """Fetch daily OHLCV for one symbol. Returns rows ready for upsert."""
    df = yf.Ticker(symbol).history(period=period, interval="1d", auto_adjust=False)
    if df.empty:
        return []
    df = df.reset_index()
    rows: list[tuple] = []
    for _, r in df.iterrows():
        d = r["Date"]
        if hasattr(d, "date"):
            d = d.date()
        adj = r["Adj Close"] if "Adj Close" in r else r.get("Close")
        rows.append(
            (
                symbol,
                d,
                _fnum(r.get("Open")),
                _fnum(r.get("High")),
                _fnum(r.get("Low")),
                _fnum(r.get("Close")),
                _fnum(adj),
                _inum(r.get("Volume")),
            )
        )
    return rows


UPSERT_SQL = """
INSERT INTO quotes_daily (symbol, date, open, high, low, close, adj_close, volume)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT (symbol, date) DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    adj_close = EXCLUDED.adj_close,
    volume = EXCLUDED.volume
"""


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    symbols = tracked_symbols()
    if not symbols:
        logger.warning("No symbols to fetch. Add a watchlist in config.yaml or positions via the Portfolio page.")
        return

    total = 0
    with connect() as con:
        for sym in symbols:
            try:
                rows = fetch_symbol(sym)
            except Exception:
                logger.exception("Failed to fetch %s", sym)
                continue
            if rows:
                con.executemany(UPSERT_SQL, rows)
            total += len(rows)
            logger.info("%s: %d rows", sym, len(rows))
    logger.info(
        "Upserted %d quote rows across %d symbols", total, len(symbols)
    )


if __name__ == "__main__":
    main()
