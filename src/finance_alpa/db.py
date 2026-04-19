"""DuckDB connection and schema."""
from __future__ import annotations

import contextlib
from collections.abc import Iterator

import duckdb

from .config import DATA_DIR, DB_PATH, RAW_EMAILS_DIR

SCHEMA = """
CREATE SEQUENCE IF NOT EXISTS alerts_seq;
CREATE SEQUENCE IF NOT EXISTS portfolios_seq;

CREATE TABLE IF NOT EXISTS tickers (
    symbol VARCHAR PRIMARY KEY,
    name VARCHAR,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS portfolios (
    id BIGINT DEFAULT nextval('portfolios_seq') PRIMARY KEY,
    name VARCHAR UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS positions (
    portfolio_id BIGINT NOT NULL,
    symbol VARCHAR NOT NULL,
    shares DOUBLE NOT NULL,
    cost_basis DOUBLE,
    account VARCHAR,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (portfolio_id, symbol)
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


LEGACY_PORTFOLIO_TABLE = "portfolio"
DEFAULT_PORTFOLIO_NAME = "Sample Portfolio"


def _migrate_legacy_portfolio(con: duckdb.DuckDBPyConnection) -> None:
    """One-shot migration: copy rows from the old flat `portfolio` table into
    `positions` under a default-named portfolio, then drop the legacy table.

    Idempotent: no-op once the legacy table is gone.
    """
    row = con.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
        [LEGACY_PORTFOLIO_TABLE],
    ).fetchone()
    if not row:
        return
    con.execute(
        "INSERT INTO portfolios (name) VALUES (?) ON CONFLICT (name) DO NOTHING",
        [DEFAULT_PORTFOLIO_NAME],
    )
    pf_id = con.execute(
        "SELECT id FROM portfolios WHERE name = ?", [DEFAULT_PORTFOLIO_NAME]
    ).fetchone()[0]
    con.execute(
        """
        INSERT INTO positions (portfolio_id, symbol, shares, cost_basis, account, updated_at)
        SELECT ?, symbol, shares, cost_basis, account, updated_at FROM portfolio
        ON CONFLICT (portfolio_id, symbol) DO NOTHING
        """,
        [pf_id],
    )
    con.execute(f"DROP TABLE {LEGACY_PORTFOLIO_TABLE}")


def init_db() -> None:
    """Create the DB file + schema if missing. Safe to call repeatedly."""
    _ensure_dirs()
    con = duckdb.connect(str(DB_PATH))
    try:
        con.execute(SCHEMA)
        _migrate_legacy_portfolio(con)
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
            _migrate_legacy_portfolio(con)
        yield con
    finally:
        con.close()
