"""AppConfig validation — new `portfolios:` shape + legacy `portfolio:` form."""
from __future__ import annotations

from finance_alpa.config import DEFAULT_PORTFOLIO_NAME, AppConfig


def test_empty_config_is_valid():
    cfg = AppConfig.model_validate({})
    assert cfg.portfolios == []
    assert cfg.all_positions == []
    assert cfg.all_symbols() == []


def test_new_portfolios_shape():
    cfg = AppConfig.model_validate(
        {
            "universe": {"watchlist": ["SPY"]},
            "portfolios": [
                {
                    "name": "Taxable",
                    "positions": [{"symbol": "AAPL", "shares": 10}],
                },
                {
                    "name": "IRA",
                    "positions": [{"symbol": "MSFT", "shares": 5}],
                },
            ],
        }
    )
    assert [pf.name for pf in cfg.portfolios] == ["Taxable", "IRA"]
    assert len(cfg.all_positions) == 2
    # Union preserves watchlist order first, then positions across portfolios.
    assert cfg.all_symbols() == ["SPY", "AAPL", "MSFT"]


def test_legacy_flat_portfolio_wraps_as_default():
    cfg = AppConfig.model_validate(
        {
            "portfolio": [
                {"symbol": "AAPL", "shares": 10},
                {"symbol": "MSFT", "shares": 5},
            ]
        }
    )
    assert len(cfg.portfolios) == 1
    assert cfg.portfolios[0].name == DEFAULT_PORTFOLIO_NAME
    assert {p.symbol for p in cfg.portfolios[0].positions} == {"AAPL", "MSFT"}


def test_new_and_legacy_produce_equivalent_all_positions():
    legacy = AppConfig.model_validate(
        {"portfolio": [{"symbol": "AAPL", "shares": 10}]}
    )
    new = AppConfig.model_validate(
        {
            "portfolios": [
                {
                    "name": DEFAULT_PORTFOLIO_NAME,
                    "positions": [{"symbol": "AAPL", "shares": 10}],
                }
            ]
        }
    )
    assert len(legacy.all_positions) == len(new.all_positions) == 1
    assert legacy.all_positions[0].symbol == new.all_positions[0].symbol == "AAPL"


def test_explicit_portfolios_wins_when_both_keys_present():
    # The legacy key is folded in only when `portfolios` is absent.
    cfg = AppConfig.model_validate(
        {
            "portfolio": [{"symbol": "LEGACY", "shares": 1}],
            "portfolios": [
                {"name": "New", "positions": [{"symbol": "NEW", "shares": 2}]}
            ],
        }
    )
    assert [pf.name for pf in cfg.portfolios] == ["New"]
    assert [p.symbol for p in cfg.all_positions] == ["NEW"]


def test_symbol_normalization():
    cfg = AppConfig.model_validate(
        {
            "universe": {"watchlist": [" spy ", "qqq"]},
            "portfolios": [
                {"name": "P", "positions": [{"symbol": "aapl ", "shares": 1}]}
            ],
        }
    )
    assert cfg.universe.watchlist == ["SPY", "QQQ"]
    assert cfg.portfolios[0].positions[0].symbol == "AAPL"
