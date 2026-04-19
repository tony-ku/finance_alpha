"""DuckDB connection and schema."""
from __future__ import annotations

import contextlib
from collections.abc import Iterator

import duckdb

from .config import DATA_DIR, DB_PATH, RAW_EMAILS_DIR

SCHEMA = """
CREATE SEQUENCE IF NOT EXISTS alerts_seq;

CREATE TABLE IF NOT EXISTS tickers (
    symbol VARCHAR PRIMARY KEY,
    name VARCHAR,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS portfolio (
    symbol VARCHAR PRIMARY KEY,
    shares DOUBLE NOT NULL,
    cost_basis DOUBLE,
    account VARCHAR,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS watchlist (
    symbol VARCHAR PRIMARY KEY,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS quotes_daily (
    symbol VARCHAR NOT NULL,
    date DATE NOT NULL,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    adj_close DOUBLE,
    volume BIGINT,
    PRIMARY KEY (symbol, date)
);

CREATE TABLE IF NOT EXISTS fundamentals (
    symbol VARCHAR NOT NULL,
    as_of DATE NOT NULL,
    metrics JSON,
    PRIMARY KEY (symbol, as_of)
);

CREATE TABLE IF NOT EXISTS ratings (
    symbol VARCHAR NOT NULL,
    source VARCHAR NOT NULL,
    as_of DATE NOT NULL,
    rating VARCHAR,
    score DOUBLE,
    payload JSON,
    PRIMARY KEY (symbol, source, as_of)
);

CREATE TABLE IF NOT EXISTS sa_emails (
    message_id VARCHAR PRIMARY KEY,
    subject VARCHAR,
    sent_at TIMESTAMP,
    kind VARCHAR,
    html_path VARCHAR,
    parsed_json JSON
);

CREATE TABLE IF NOT EXISTS articles (
    source VARCHAR NOT NULL,
    url VARCHAR NOT NULL,
    title VARCHAR,
    author VARCHAR,
    published_at TIMESTAMP,
    summary TEXT,
    tickers VARCHAR[],
    read BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (source, url)
);

CREATE TABLE IF NOT EXISTS news (
    source VARCHAR NOT NULL,
    url VARCHAR NOT NULL,
    title VARCHAR,
    published_at TIMESTAMP,
    summary TEXT,
    tickers VARCHAR[],
    read BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (source, url)
);

CREATE TABLE IF NOT EXISTS estimates (
    symbol VARCHAR NOT NULL,
    period VARCHAR NOT NULL,
    as_of DATE NOT NULL,
    eps_mean DOUBLE,
    revenue_mean DOUBLE,
    payload JSON,
    PRIMARY KEY (symbol, period, as_of)
);

CREATE TABLE IF NOT EXISTS earnings_calendar (
    symbol VARCHAR NOT NULL,
    report_date DATE NOT NULL,
    eps_est DOUBLE,
    revenue_est DOUBLE,
    confirmed BOOLEAN,
    PRIMARY KEY (symbol, report_date)
);

CREATE TABLE IF NOT EXISTS alerts_log (
    id BIGINT DEFAULT nextval('alerts_seq') PRIMARY KEY,
    rule_name VARCHAR,
    symbol VARCHAR,
    fired_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    payload JSON
);
"""


def _ensure_dirs() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    RAW_EMAILS_DIR.mkdir(parents=True, exist_ok=True)


def init_db() -> None:
    """Create the DB file + schema if missing. Safe to call repeatedly."""
    _ensure_dirs()
    con = duckdb.connect(str(DB_PATH))
    try:
        con.execute(SCHEMA)
    finally:
        con.close()


@contextlib.contextmanager
def connect(read_only: bool = False) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open a DuckDB connection. Writers also run schema migrations."""
    _ensure_dirs()
    if read_only and not DB_PATH.exists():
        init_db()
    con = duckdb.connect(str(DB_PATH), read_only=read_only)
    try:
        if not read_only:
            con.execute(SCHEMA)
        yield con
    finally:
        con.close()
