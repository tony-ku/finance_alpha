"""Ingest news and analyst recommendation trends from Finnhub.

Free tier: 60 requests/minute. Per-symbol calls are modest; this module paces
requests with a small sleep to stay comfortably under the limit.
"""
from __future__ import annotations

import json
import logging
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

import httpx

from ..config import get_settings
from ..db import connect
from ..portfolio import tracked_symbols

logger = logging.getLogger(__name__)

BASE_URL = "https://finnhub.io/api/v1"
REQUEST_TIMEOUT = 20.0
RATE_LIMIT_SLEEP = 1.1  # 60/min → 1/sec, leave headroom


def _client(api_key: str) -> httpx.Client:
    return httpx.Client(
        base_url=BASE_URL,
        params={"token": api_key},
        timeout=REQUEST_TIMEOUT,
    )


def _get_json(client: httpx.Client, path: str, **params: Any) -> list | dict | None:
    try:
        r = client.get(path, params=params)
    except httpx.RequestError:
        logger.exception("Finnhub request failed for %s", path)
        return None
    if r.status_code == 429:
        logger.warning("Finnhub rate-limited at %s — stopping run", path)
        return None
    if r.status_code >= 400:
        logger.warning("Finnhub %s returned %d", path, r.status_code)
        return None
    try:
        return r.json()
    except Exception:
        logger.exception("Failed to parse Finnhub JSON from %s", path)
        return None


NEWS_UPSERT = """
INSERT INTO news (source, url, title, published_at, summary, tickers)
VALUES ('finnhub', ?, ?, ?, ?, ?)
ON CONFLICT (source, url) DO UPDATE SET
    title = EXCLUDED.title,
    published_at = COALESCE(news.published_at, EXCLUDED.published_at),
    summary = EXCLUDED.summary,
    tickers = EXCLUDED.tickers
"""


def ingest_company_news(
    client: httpx.Client, con, symbol: str, lookback_days: int = 14
) -> int:
    today = date.today()
    frm = (today - timedelta(days=lookback_days)).isoformat()
    items = _get_json(client, "/company-news", symbol=symbol, **{"from": frm, "to": today.isoformat()})
    if not isinstance(items, list) or not items:
        return 0
    rows = 0
    for item in items:
        url = item.get("url")
        if not url:
            continue
        ts = item.get("datetime")
        published = (
            datetime.fromtimestamp(ts, tz=timezone.utc).replace(tzinfo=None)
            if isinstance(ts, (int, float)) and ts
            else None
        )
        con.execute(
            NEWS_UPSERT,
            [
                url,
                item.get("headline") or "",
                published,
                item.get("summary") or "",
                [symbol],
            ],
        )
        rows += 1
    return rows


def ingest_recommendation(client: httpx.Client, con, symbol: str) -> int:
    items = _get_json(client, "/stock/recommendation", symbol=symbol)
    if not isinstance(items, list) or not items:
        return 0
    # Each item is a monthly snapshot: {period, strongBuy, buy, hold, sell, strongSell}
    # Compute a rec_mean in 1..5 where 1=Strong Buy (lower is better).
    rows = 0
    for item in items:
        period = item.get("period")
        if not period:
            continue
        sb = int(item.get("strongBuy") or 0)
        b = int(item.get("buy") or 0)
        h = int(item.get("hold") or 0)
        s = int(item.get("sell") or 0)
        ss = int(item.get("strongSell") or 0)
        total = sb + b + h + s + ss
        if total == 0:
            continue
        mean = (1 * sb + 2 * b + 3 * h + 4 * s + 5 * ss) / total
        con.execute(
            """
            INSERT INTO ratings (symbol, source, as_of, rating, score, payload)
            VALUES (?, 'finnhub_reco', ?, ?, ?, ?)
            ON CONFLICT (symbol, source, as_of) DO UPDATE SET
                rating = EXCLUDED.rating,
                score = EXCLUDED.score,
                payload = EXCLUDED.payload
            """,
            [
                symbol,
                period,
                _mean_to_label(mean),
                round(mean, 3),
                json.dumps(item),
            ],
        )
        rows += 1
    return rows


def _mean_to_label(mean: float) -> str:
    if mean < 1.5:
        return "Strong Buy"
    if mean < 2.5:
        return "Buy"
    if mean < 3.5:
        return "Hold"
    if mean < 4.5:
        return "Sell"
    return "Strong Sell"


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    settings = get_settings()
    if not settings.finnhub_api_key:
        logger.warning(
            "FINNHUB_API_KEY not set in .env — skipping Finnhub ingest. "
            "Sign up at https://finnhub.io/register"
        )
        return

    symbols = tracked_symbols()
    totals = {"news": 0, "reco": 0}

    with _client(settings.finnhub_api_key) as client, connect() as con:
        for sym in symbols:
            try:
                totals["news"] += ingest_company_news(client, con, sym)
            except Exception:
                logger.exception("Finnhub news failed for %s", sym)
            time.sleep(RATE_LIMIT_SLEEP)
            try:
                totals["reco"] += ingest_recommendation(client, con, sym)
            except Exception:
                logger.exception("Finnhub reco failed for %s", sym)
            time.sleep(RATE_LIMIT_SLEEP)
            logger.info("Finnhub %s done", sym)

    logger.info("Finnhub ingest totals: %s", totals)


if __name__ == "__main__":
    main()
