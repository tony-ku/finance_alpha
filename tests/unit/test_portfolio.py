"""Multi-portfolio persistence — CRUD + position scoping."""
from __future__ import annotations

import pytest

from finance_alpa import portfolio as P
from finance_alpa.config import AppConfig, Position


def _pos(symbol: str, shares: float, cost: float | None = None) -> Position:
    return Position(symbol=symbol, shares=shares, cost_basis=cost)


def test_create_and_list(tmp_db):
    a = P.create_portfolio("Taxable")
    b = P.create_portfolio("IRA")
    names = [p.name for p in P.list_portfolios()]
    assert names == ["Taxable", "IRA"]
    assert a.id != b.id


def test_create_empty_name_rejected(tmp_db):
    with pytest.raises(ValueError):
        P.create_portfolio("   ")


def test_duplicate_name_raises(tmp_db):
    P.create_portfolio("Taxable")
    with pytest.raises(Exception):  # DuckDB UNIQUE constraint
        P.create_portfolio("Taxable")


def test_rename(tmp_db):
    p = P.create_portfolio("Old")
    P.rename_portfolio(p.id, "New")
    assert P.get_portfolio_by_name("Old") is None
    assert P.get_portfolio_by_name("New").id == p.id


def test_delete_cascades_positions(tmp_db):
    a = P.create_portfolio("A")
    b = P.create_portfolio("B")
    P.upsert_positions(a.id, [_pos("AAPL", 10)])
    P.upsert_positions(b.id, [_pos("MSFT", 5)])
    P.delete_portfolio(a.id)
    assert P.get_portfolio_by_name("A") is None
    assert P.get_positions(a.id) == []
    # B is untouched.
    assert [p.symbol for p in P.get_positions(b.id)] == ["MSFT"]


def test_upsert_scoped_to_portfolio(tmp_db):
    a = P.create_portfolio("A")
    b = P.create_portfolio("B")
    P.upsert_positions(a.id, [_pos("AAPL", 10, 150.0)])
    P.upsert_positions(b.id, [_pos("AAPL", 20, 200.0)])
    # Same symbol in two portfolios — both rows exist, independently.
    rows_a = P.get_positions(a.id)
    rows_b = P.get_positions(b.id)
    assert rows_a[0].shares == 10 and rows_a[0].cost_basis == 150.0
    assert rows_b[0].shares == 20 and rows_b[0].cost_basis == 200.0


def test_upsert_updates_existing(tmp_db):
    a = P.create_portfolio("A")
    P.upsert_positions(a.id, [_pos("AAPL", 10, 150.0)])
    P.upsert_positions(a.id, [_pos("AAPL", 15, 160.0)])
    rows = P.get_positions(a.id)
    assert len(rows) == 1
    assert rows[0].shares == 15
    assert rows[0].cost_basis == 160.0


def test_replace_mode_wipes_only_target(tmp_db):
    a = P.create_portfolio("A")
    b = P.create_portfolio("B")
    P.upsert_positions(a.id, [_pos("AAPL", 10), _pos("MSFT", 5)])
    P.upsert_positions(b.id, [_pos("NVDA", 3)])
    P.upsert_positions(a.id, [_pos("GOOGL", 2)], mode="replace")
    assert [p.symbol for p in P.get_positions(a.id)] == ["GOOGL"]
    # B untouched.
    assert [p.symbol for p in P.get_positions(b.id)] == ["NVDA"]


def test_clear_positions_scoped(tmp_db):
    a = P.create_portfolio("A")
    b = P.create_portfolio("B")
    P.upsert_positions(a.id, [_pos("AAPL", 10)])
    P.upsert_positions(b.id, [_pos("MSFT", 5)])
    assert P.clear_positions(a.id) == 1
    assert P.get_positions(a.id) == []
    assert [p.symbol for p in P.get_positions(b.id)] == ["MSFT"]


def test_tracked_symbols_unions_across_portfolios(tmp_db, monkeypatch):
    fake_cfg = AppConfig.model_validate(
        {"universe": {"watchlist": ["SPY"]}, "portfolios": []}
    )
    monkeypatch.setattr(P, "load_app_config", lambda: fake_cfg)
    a = P.create_portfolio("A")
    b = P.create_portfolio("B")
    P.upsert_positions(a.id, [_pos("AAPL", 10)])
    P.upsert_positions(b.id, [_pos("MSFT", 5), _pos("AAPL", 1)])
    # Watchlist first, then DB-held symbols deduped.
    syms = P.tracked_symbols()
    assert syms[0] == "SPY"
    assert set(syms) == {"SPY", "AAPL", "MSFT"}


def test_tracked_symbols_falls_back_to_config_when_db_empty(tmp_db, monkeypatch):
    fake_cfg = AppConfig.model_validate(
        {
            "universe": {"watchlist": ["SPY"]},
            "portfolios": [
                {"name": "Sample", "positions": [{"symbol": "AAPL", "shares": 1}]}
            ],
        }
    )
    monkeypatch.setattr(P, "load_app_config", lambda: fake_cfg)
    # No DB portfolios created → fall back to config positions.
    assert set(P.tracked_symbols()) == {"SPY", "AAPL"}


def test_seed_from_config_if_empty(tmp_db, monkeypatch):
    fake_cfg = AppConfig.model_validate(
        {
            "portfolios": [
                {
                    "name": "Sample",
                    "positions": [
                        {"symbol": "AAPL", "shares": 10},
                        {"symbol": "MSFT", "shares": 5},
                    ],
                }
            ]
        }
    )
    monkeypatch.setattr(P, "load_app_config", lambda: fake_cfg)
    assert P.seed_from_config_if_empty() == 2
    assert [pf.name for pf in P.list_portfolios()] == ["Sample"]
    # Second call is a no-op because DB is non-empty.
    assert P.seed_from_config_if_empty() == 0


def test_seed_from_config_if_empty_skipped_when_user_has_portfolios(
    tmp_db, monkeypatch
):
    fake_cfg = AppConfig.model_validate(
        {
            "portfolios": [
                {"name": "Sample", "positions": [{"symbol": "AAPL", "shares": 10}]}
            ]
        }
    )
    monkeypatch.setattr(P, "load_app_config", lambda: fake_cfg)
    P.create_portfolio("UserOwn")
    assert P.seed_from_config_if_empty() == 0
    assert [pf.name for pf in P.list_portfolios()] == ["UserOwn"]
