"""Composite ranking over stored ratings, momentum, and recommendations.

Signals are z-scored across the universe, then combined by user-supplied weights.
Direction:
  - fmp_score    : higher is better
  - momentum_3m  : higher is better
  - reco_mean    : lower is better (1=Strong Buy, 5=Strong Sell) → inverted
"""
from __future__ import annotations

from collections.abc import Mapping

import pandas as pd

from ..db import connect
from ..portfolio import tracked_symbols

SIGNAL_SQL = """
WITH universe AS (
    SELECT UNNEST(?::VARCHAR[]) AS symbol
),
latest_fmp AS (
    SELECT symbol, score AS fmp_score, rating AS fmp_rating, as_of AS fmp_date
    FROM ratings
    WHERE source = 'fmp_rating'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY as_of DESC) = 1
),
latest_reco AS (
    SELECT symbol, score AS reco_mean, rating AS reco_label, as_of AS reco_date
    FROM ratings
    WHERE source = 'finnhub_reco'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY as_of DESC) = 1
),
latest_price AS (
    SELECT symbol, close AS last_price, date AS last_date
    FROM quotes_daily
    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) = 1
),
price_ref AS (
    SELECT symbol, close AS ref_price
    FROM quotes_daily
    WHERE date <= CURRENT_DATE - INTERVAL 63 DAY
    QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) = 1
),
next_earnings AS (
    SELECT symbol, MIN(report_date) AS next_earnings_date
    FROM earnings_calendar
    WHERE report_date >= CURRENT_DATE
    GROUP BY symbol
)
SELECT u.symbol,
       lp.last_price,
       lp.last_date,
       (lp.last_price - pr.ref_price) / NULLIF(pr.ref_price, 0) * 100 AS momentum_3m,
       f.fmp_score,
       f.fmp_rating,
       f.fmp_date,
       r.reco_mean,
       r.reco_label,
       r.reco_date,
       ne.next_earnings_date
FROM universe u
LEFT JOIN latest_fmp f   ON f.symbol = u.symbol
LEFT JOIN latest_reco r  ON r.symbol = u.symbol
LEFT JOIN latest_price lp ON lp.symbol = u.symbol
LEFT JOIN price_ref pr    ON pr.symbol = u.symbol
LEFT JOIN next_earnings ne ON ne.symbol = u.symbol
"""


def _zscore(s: pd.Series) -> pd.Series:
    s = pd.to_numeric(s, errors="coerce")
    std = s.std(ddof=0)
    if std is None or pd.isna(std) or std == 0:
        return pd.Series([0.0] * len(s), index=s.index)
    return (s - s.mean()) / std


def load_signals(symbols: list[str] | None = None) -> pd.DataFrame:
    if symbols is None:
        symbols = tracked_symbols()
    if not symbols:
        return pd.DataFrame()
    with connect() as con:
        return con.execute(SIGNAL_SQL, [symbols]).fetchdf()


def rank(weights: Mapping[str, float], symbols: list[str] | None = None) -> pd.DataFrame:
    """Rank tickers by a weighted composite of z-scored signals.

    Keys in ``weights``: "fmp", "momentum", "reco". Missing keys default to 0.
    """
    df = load_signals(symbols)
    if df.empty:
        return df

    df["z_fmp"] = _zscore(df["fmp_score"])
    df["z_mom"] = _zscore(df["momentum_3m"])
    # Lower reco_mean is better → invert.
    df["z_reco"] = -_zscore(df["reco_mean"])

    w_fmp = float(weights.get("fmp", 0) or 0)
    w_mom = float(weights.get("momentum", 0) or 0)
    w_reco = float(weights.get("reco", 0) or 0)
    total_w = w_fmp + w_mom + w_reco
    if total_w == 0:
        df["composite"] = 0.0
    else:
        df["composite"] = (
            w_fmp * df["z_fmp"].fillna(0)
            + w_mom * df["z_mom"].fillna(0)
            + w_reco * df["z_reco"].fillna(0)
        ) / total_w

    return df.sort_values("composite", ascending=False, kind="mergesort").reset_index(drop=True)
