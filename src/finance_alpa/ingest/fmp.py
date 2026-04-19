"""Ingest from Financial Modeling Prep — ratings, estimates, earnings, fundamentals.

Free-tier budget: 250 requests/day. This module keeps ingest to the tracked
universe and runs one endpoint per symbol per call.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import date, timedelta
from typing import Any

import httpx

from ..config import get_settings
from ..db import connect
from ..portfolio import tracked_symbols

logger = logging.getLogger(__name__)

BASE_URL = "https://financialmodelingprep.com/api/v3"
REQUEST_TIMEOUT = 20.0
RATE_LIMIT_SLEEP = 0.25  # light pause between calls to stay well under limits


def _client(api_key: str) -> httpx.Client:
    return httpx.Client(
        base_url=BASE_URL,
        params={"apikey": api_key},
        timeout=REQUEST_TIMEOUT,
    )


def _get_json(client: httpx.Client, path: str, **params: Any) -> list | dict | None:
    try:
        r = client.get(path, params=params)
    except httpx.RequestError:
        logger.exception("FMP request failed for %s", path)
        return None
    if r.status_code == 429:
        logger.warning("FMP rate-limited at %s — stopping this run", path)
        return None
    if r.status_code >= 400:
        logger.warning("FMP %s returned %d: %s", path, r.status_code, r.text[:200])
        return None
    try:
        return r.json()
    except Exception:
        logger.exception("Failed to parse FMP JSON from %s", path)
        return None


def ingest_rating(client: httpx.Client, con, symbol: str) -> int:
    data = _get_json(client, f"/rating/{symbol}")
    if not data:
        return 0
    row = data[0] if isinstance(data, list) else data
    as_of = row.get("date") or date.today().isoformat()
    con.execute(
        """
        INSERT INTO ratings (symbol, source, as_of, rating, score, payload)
        VALUES (?, 'fmp_rating', ?, ?, ?, ?)
        ON CONFLICT (symbol, source, as_of) DO UPDATE SET
            rating = EXCLUDED.rating,
            score = EXCLUDED.score,
            payload = EXCLUDED.payload
        """,
        [
            symbol,
            as_of,
            row.get("rating"),
            float(row.get("ratingScore") or 0) or None,
            json.dumps(row),
        ],
    )
    return 1


def ingest_estimates(client: httpx.Client, con, symbol: str) -> int:
    data = _get_json(client, f"/analyst-estimates/{symbol}")
    if not isinstance(data, list) or not data:
        return 0
    rows = 0
    for row in data:
        period = row.get("date") or ""
        if not period:
            continue
        con.execute(
            """
            INSERT INTO estimates (symbol, period, as_of, eps_mean, revenue_mean, payload)
            VALUES (?, ?, CURRENT_DATE, ?, ?, ?)
            ON CONFLICT (symbol, period, as_of) DO UPDATE SET
                eps_mean = EXCLUDED.eps_mean,
                revenue_mean = EXCLUDED.revenue_mean,
                payload = EXCLUDED.payload
            """,
            [
                symbol,
                period,
                _to_float(row.get("estimatedEpsAvg")),
                _to_float(row.get("estimatedRevenueAvg")),
                json.dumps(row),
            ],
        )
        rows += 1
    return rows


def ingest_fundamentals(client: httpx.Client, con, symbol: str) -> int:
    data = _get_json(client, f"/key-metrics-ttm/{symbol}")
    if not isinstance(data, list) or not data:
        return 0
    row = data[0]
    con.execute(
        """
        INSERT INTO fundamentals (symbol, as_of, metrics)
        VALUES (?, CURRENT_DATE, ?)
        ON CONFLICT (symbol, as_of) DO UPDATE SET metrics = EXCLUDED.metrics
        """,
        [symbol, json.dumps(row)],
    )
    return 1


def ingest_earnings_calendar(client: httpx.Client, con, days_ahead: int = 30) -> int:
    frm = date.today().isoformat()
    to = (date.today() + timedelta(days=days_ahead)).isoformat()
    data = _get_json(client, "/earning_calendar", **{"from": frm, "to": to})
    if not isinstance(data, list) or not data:
        return 0
    rows = 0
    for row in data:
        sym = row.get("symbol")
        report_date = row.get("date")
        if not sym or not report_date:
            continue
        con.execute(
            """
            INSERT INTO earnings_calendar (symbol, report_date, eps_est, revenue_est, confirmed)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (symbol, report_date) DO UPDATE SET
                eps_est = EXCLUDED.eps_est,
                revenue_est = EXCLUDED.revenue_est,
                confirmed = EXCLUDED.confirmed
            """,
            [
                sym,
                report_date,
                _to_float(row.get("epsEstimated")),
                _to_float(row.get("revenueEstimated")),
                bool(row.get("updatedFromDate")),
            ],
        )
        rows += 1
    return rows


def _to_float(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    settings = get_settings()
    if not settings.fmp_api_key:
        logger.warning(
            "FMP_API_KEY not set in .env — skipping FMP ingest. "
            "Sign up at https://site.financialmodelingprep.com/developer/docs"
        )
        return

    symbols = tracked_symbols()
    totals = {"rating": 0, "estimates": 0, "fundamentals": 0, "earnings": 0}

    with _client(settings.fmp_api_key) as client, connect() as con:
        for sym in symbols:
            for fn, key in (
                (ingest_rating, "rating"),
                (ingest_estimates, "estimates"),
                (ingest_fundamentals, "fundamentals"),
            ):
                try:
                    totals[key] += fn(client, con, sym)
                except Exception:
                    logger.exception("FMP %s failed for %s", key, sym)
                time.sleep(RATE_LIMIT_SLEEP)
            logger.info("FMP %s done", sym)

        try:
            totals["earnings"] = ingest_earnings_calendar(client, con)
        except Exception:
            logger.exception("FMP earnings calendar failed")

    logger.info("FMP ingest totals: %s", totals)


if __name__ == "__main__":
    main()
