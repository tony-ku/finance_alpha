"""Broker CSV auto-detect + parse_positions — mixed column names, cost bases."""
from __future__ import annotations

from finance_alpa.ingest.broker_csv import (
    ColumnMapping,
    detect_columns,
    parse_positions,
    read_csv,
)


def test_read_csv_strips_preamble_and_bom():
    raw = (
        b"\xef\xbb\xbf"  # UTF-8 BOM
        b"Account Statement,,,\n"
        b"As of 2026-04-18,,,\n"
        b"Symbol,Quantity,Average Cost,Account\n"
        b"AAPL,10,150.00,Taxable\n"
        b"MSFT,5,320.00,IRA\n"
    )
    df = read_csv(raw)
    assert list(df.columns) == ["Symbol", "Quantity", "Average Cost", "Account"]
    assert len(df) == 2


def test_detect_columns_fidelity_shape():
    import pandas as pd

    df = pd.DataFrame(
        columns=["Symbol", "Quantity", "Average Cost Basis", "Account Name"]
    )
    m = detect_columns(df)
    assert m.symbol == "Symbol"
    assert m.shares == "Quantity"
    assert m.cost_basis == "Average Cost Basis"
    assert m.cost_basis_total is None
    assert m.account == "Account Name"


def test_detect_columns_total_only_flagged():
    import pandas as pd

    df = pd.DataFrame(columns=["Ticker", "Shares", "Cost Basis Total"])
    m = detect_columns(df)
    assert m.symbol == "Ticker"
    assert m.shares == "Shares"
    assert m.cost_basis is None
    assert m.cost_basis_total == "Cost Basis Total"
    assert any("total cost column" in n.lower() for n in m.notes)


def test_parse_positions_per_share_cost_basis():
    import pandas as pd

    df = pd.DataFrame(
        {
            "Symbol": ["AAPL", "MSFT"],
            "Shares": [10, 5],
            "Avg Cost": ["$150.00", "320"],
        }
    )
    mapping = ColumnMapping(symbol="Symbol", shares="Shares", cost_basis="Avg Cost")
    out, warns = parse_positions(df, mapping, cost_is_total=False)
    assert warns == []
    assert len(out) == 2
    assert out[0].cost_basis == 150.0
    assert out[1].cost_basis == 320.0


def test_parse_positions_total_cost_normalized_to_per_share():
    import pandas as pd

    df = pd.DataFrame(
        {
            "Symbol": ["AAPL"],
            "Shares": [10],
            "Cost Basis Total": ["1,500.00"],
        }
    )
    mapping = ColumnMapping(
        symbol="Symbol", shares="Shares", cost_basis_total="Cost Basis Total"
    )
    out, _ = parse_positions(df, mapping, cost_is_total=True)
    assert len(out) == 1
    assert out[0].cost_basis == 150.0  # 1500 / 10


def test_parse_positions_skips_non_symbols_and_zero_shares():
    import pandas as pd

    df = pd.DataFrame(
        {
            # "Cash & Equivalents" fails the ticker regex (has spaces/&);
            # lowercase "aapl_lower" fails (starts with non-uppercase after strip/upper? actually it's upper()'d — use a chars-disallowed case).
            "Symbol": ["AAPL", "Cash & Equivalents", "MSFT", "123ABC"],
            "Shares": [10, 100, 0, 15],
        }
    )
    mapping = ColumnMapping(symbol="Symbol", shares="Shares")
    out, warns = parse_positions(df, mapping)
    # AAPL kept; Cash & Equivalents + 123ABC fail the ticker regex; MSFT has zero shares.
    assert [p.symbol for p in out] == ["AAPL"]
    assert any("zero shares" in w for w in warns)
    assert any("non-symbol" in w for w in warns)


def test_parse_positions_requires_mappings():
    import pandas as pd

    df = pd.DataFrame({"Ticker": ["AAPL"], "Qty": [10]})
    out, warns = parse_positions(df, ColumnMapping())
    assert out == []
    assert warns and "required" in warns[0]
