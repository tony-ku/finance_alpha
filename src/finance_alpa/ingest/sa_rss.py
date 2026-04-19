"""Ingest Seeking Alpha public RSS feeds into the articles table."""
from __future__ import annotations

import logging
from datetime import datetime

import feedparser

from ..config import load_app_config
from ..db import connect

logger = logging.getLogger(__name__)


def _parse_time(entry) -> datetime | None:
    """Return a naive UTC datetime from the entry's published/updated fields."""
    for attr in ("published_parsed", "updated_parsed"):
        t = getattr(entry, attr, None)
        if t:
            return datetime(*t[:6])
    return None


def _tickers_from_entry(entry) -> list[str]:
    """Best-effort ticker extraction from <category> tags on SA RSS entries."""
    tags = entry.get("tags") or []
    out: list[str] = []
    for t in tags:
        term = getattr(t, "term", None)
        if term is None and isinstance(t, dict):
            term = t.get("term")
        if not term:
            continue
        term = term.strip()
        if term.isupper() and term.isalpha() and 1 <= len(term) <= 6:
            out.append(term)
    return out


def parse_feed(url: str) -> list[dict]:
    parsed = feedparser.parse(url)
    items: list[dict] = []
    for e in parsed.entries:
        link = e.get("link")
        if not link:
            continue
        author = e.get("author")
        if not author and e.get("authors"):
            first = e.authors[0]
            author = first.get("name") if isinstance(first, dict) else None
        items.append(
            {
                "url": link,
                "title": e.get("title") or "",
                "author": author,
                "published_at": _parse_time(e),
                "summary": e.get("summary") or "",
                "tickers": _tickers_from_entry(e),
            }
        )
    return items


UPSERT_SQL = """
INSERT INTO articles
    (source, url, title, author, published_at, summary, tickers)
VALUES ('sa_rss', ?, ?, ?, ?, ?, ?)
ON CONFLICT (source, url) DO UPDATE SET
    title = EXCLUDED.title,
    author = EXCLUDED.author,
    published_at = COALESCE(articles.published_at, EXCLUDED.published_at),
    summary = EXCLUDED.summary,
    tickers = EXCLUDED.tickers
"""


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    cfg = load_app_config()
    if not cfg.sa_rss_feeds:
        logger.warning("No SA RSS feeds configured. Edit config.yaml.")
        return

    total = 0
    with connect() as con:
        for feed in cfg.sa_rss_feeds:
            try:
                items = parse_feed(feed.url)
            except Exception:
                logger.exception("Failed to parse %s", feed.url)
                continue
            for item in items:
                con.execute(
                    UPSERT_SQL,
                    [
                        item["url"],
                        item["title"],
                        item["author"],
                        item["published_at"],
                        item["summary"],
                        item["tickers"],
                    ],
                )
            logger.info("Feed '%s': %d items", feed.name, len(items))
            total += len(items)
    logger.info(
        "Upserted %d RSS items across %d feeds", total, len(cfg.sa_rss_feeds)
    )


if __name__ == "__main__":
    main()
