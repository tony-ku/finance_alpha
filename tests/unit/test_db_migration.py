"""Legacy `portfolio` → `positions` migration is idempotent and lossless."""
from __future__ import annotations

import duckdb

from finance_alpa.config import DEFAULT_PORTFOLIO_NAME
from finance_alpa.db import connect


def _seed_legacy(db_path) -> None:
    """Write a legacy-only schema (old `portfolio` table, no `portfolios`/
    `positions`) and insert two rows."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(db_path))
    try:
        con.execute(
            """
            CREATE TABLE portfolio (
                symbol VARCHAR PRIMARY KEY,
                shares DOUBLE NOT NULL,
                cost_basis DOUBLE,
                account VARCHAR,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            INSERT INTO portfolio (symbol, shares, cost_basis, account) VALUES
              ('AAPL', 10, 150.0, 'Taxable'),
              ('MSFT', 5, 320.0, 'IRA');
            """
        )
    finally:
        con.close()


def test_migration_moves_rows_and_drops_legacy_table(tmp_db):
    _seed_legacy(tmp_db)

    with connect() as con:
        # Migration runs as part of connect(). Legacy table is gone.
        legacy = con.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = 'portfolio'"
        ).fetchone()
        assert legacy is None

        # A Sample Portfolio was created to hold the migrated rows.
        names = [r[0] for r in con.execute("SELECT name FROM portfolios").fetchall()]
        assert names == [DEFAULT_PORTFOLIO_NAME]

        # All rows moved with their columns intact.
        rows = con.execute(
            """
            SELECT p.name, x.symbol, x.shares, x.cost_basis, x.account
            FROM positions x JOIN portfolios p ON p.id = x.portfolio_id
            ORDER BY x.symbol
            """
        ).fetchall()
        assert rows == [
            (DEFAULT_PORTFOLIO_NAME, "AAPL", 10.0, 150.0, "Taxable"),
            (DEFAULT_PORTFOLIO_NAME, "MSFT", 5.0, 320.0, "IRA"),
        ]


def test_migration_is_idempotent(tmp_db):
    _seed_legacy(tmp_db)
    # First open migrates.
    with connect() as con:
        before = con.execute("SELECT COUNT(*) FROM positions").fetchone()[0]
    # Second open should be a no-op — no duplicates, no errors.
    with connect() as con:
        after = con.execute("SELECT COUNT(*) FROM positions").fetchone()[0]
        legacy = con.execute(
            "SELECT 1 FROM information_schema.tables WHERE table_name = 'portfolio'"
        ).fetchone()
    assert before == after == 2
    assert legacy is None


def test_fresh_db_no_legacy_table_still_initializes(tmp_db):
    with connect() as con:
        tables = {
            r[0]
            for r in con.execute(
                "SELECT table_name FROM information_schema.tables"
            ).fetchall()
        }
    assert {"portfolios", "positions"}.issubset(tables)
    assert "portfolio" not in tables
