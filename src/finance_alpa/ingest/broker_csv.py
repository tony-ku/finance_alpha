"""Parse brokerage CSV exports into Position rows.

Auto-detects column names from common US broker formats (Schwab, Fidelity,
Vanguard, IBKR Flex, Robinhood). The caller is expected to let the user
override the mapping via UI before committing.

All cost_basis values are normalized to **per-share** in the output.
"""
from __future__ import annotations

import io
import logging
import re
from dataclasses import dataclass, field

import pandas as pd

from ..config import Position

logger = logging.getLogger(__name__)


@dataclass
class ColumnMapping:
    symbol: str | None = None
    shares: str | None = None
    cost_basis: str | None = None
    cost_basis_total: str | None = None  # if only a total column is present
    account: str | None = None
    notes: list[str] = field(default_factory=list)


SYMBOL_PATTERNS = [
    r"^symbol$",
    r"^ticker$",
    r"^security\s*symbol$",
]
SHARES_PATTERNS = [
    r"^quantity$",
    r"^shares$",
    r"^qty$",
    r"^units$",
    r"^position$",
]
COST_PER_SHARE_PATTERNS = [
    r"^avg\s*cost$",
    r"^average\s*cost$",
    r"^average\s*cost\s*basis$",
    r"^average\s*cost\s*per\s*share$",
    r"^cost\s*basis\s*per\s*share$",
    r"^costbasisprice$",
    r"^cost\s*basis\s*price$",
    r"^average\s*price$",
]
COST_TOTAL_PATTERNS = [
    r"^cost\s*basis\s*total$",
    r"^total\s*cost$",
    r"^costbasismoney$",
    r"^cost\s*basis$",  # intentionally last — ambiguous
]
ACCOUNT_PATTERNS = [
    r"^account$",
    r"^account\s*name$",
    r"^account\s*number$",
    r"^accountid$",
]


def _match(col: str, patterns: list[str]) -> bool:
    key = col.strip().lower()
    return any(re.match(p, key) for p in patterns)


def detect_columns(df: pd.DataFrame) -> ColumnMapping:
    cols = list(df.columns)
    m = ColumnMapping()

    for c in cols:
        if m.symbol is None and _match(c, SYMBOL_PATTERNS):
            m.symbol = c
        if m.shares is None and _match(c, SHARES_PATTERNS):
            m.shares = c
        if m.cost_basis is None and _match(c, COST_PER_SHARE_PATTERNS):
            m.cost_basis = c
        if m.cost_basis_total is None and _match(c, COST_TOTAL_PATTERNS):
            m.cost_basis_total = c
        if m.account is None and _match(c, ACCOUNT_PATTERNS):
            m.account = c

    # Prefer per-share when both are found.
    if m.cost_basis and m.cost_basis_total:
        m.notes.append(
            f"Both per-share ({m.cost_basis}) and total ({m.cost_basis_total}) "
            "cost columns detected — using per-share."
        )
        m.cost_basis_total = None
    elif m.cost_basis_total and not m.cost_basis:
        m.notes.append(
            f"Only a total cost column ({m.cost_basis_total}) was detected — "
            "will divide by shares to get per-share."
        )

    if m.symbol is None:
        m.notes.append("No symbol column detected — pick one manually.")
    if m.shares is None:
        m.notes.append("No shares column detected — pick one manually.")
    return m


def read_csv(raw: bytes) -> pd.DataFrame:
    """Read a CSV from raw bytes, tolerant of encoding quirks + preamble rows."""
    for encoding in ("utf-8-sig", "utf-8", "utf-16", "cp1252", "latin-1"):
        try:
            text = raw.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ValueError("Could not decode CSV with common encodings")

    # Some brokers (Fidelity, Schwab) prefix metadata lines before the header.
    # Skip until the first line that looks like a CSV header (contains >=2 commas
    # and at least one likely header keyword).
    lines = text.splitlines()
    header_idx = 0
    for i, line in enumerate(lines):
        lower = line.lower()
        if line.count(",") >= 2 and any(
            k in lower for k in ("symbol", "ticker", "quantity", "shares")
        ):
            header_idx = i
            break
    buf = io.StringIO("\n".join(lines[header_idx:]))
    df = pd.read_csv(buf)
    df.columns = [str(c).strip() for c in df.columns]
    return df


def _to_float(v) -> float | None:
    if pd.isna(v):
        return None
    s = str(v).strip().replace(",", "").replace("$", "")
    if s in ("", "--", "N/A", "n/a", "None"):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def parse_positions(
    df: pd.DataFrame,
    mapping: ColumnMapping,
    cost_is_total: bool = False,
) -> tuple[list[Position], list[str]]:
    """Transform a broker CSV into Position rows.

    Returns (positions, warnings). Rows missing a symbol or shares are skipped
    with a warning.
    """
    warnings: list[str] = []
    out: list[Position] = []

    if mapping.symbol is None or mapping.shares is None:
        warnings.append("Both 'symbol' and 'shares' column mappings are required.")
        return out, warnings

    cost_col = mapping.cost_basis or mapping.cost_basis_total
    derive_total_from_col = bool(
        cost_col and (cost_is_total or cost_col == mapping.cost_basis_total)
    )

    for idx, r in df.iterrows():
        sym = str(r[mapping.symbol]).strip().upper()
        if not sym or sym.lower() in ("nan", "none"):
            continue
        # Strip out obviously-non-symbol values (cash positions, totals rows)
        if not re.match(r"^[A-Z][A-Z0-9.\-]{0,9}$", sym):
            warnings.append(f"Row {idx}: skipping non-symbol value '{sym}'")
            continue

        shares = _to_float(r[mapping.shares])
        if shares is None or shares == 0:
            warnings.append(f"Row {idx} ({sym}): missing or zero shares — skipped")
            continue

        cb: float | None = None
        if cost_col is not None:
            raw_cost = _to_float(r[cost_col])
            if raw_cost is not None:
                cb = (raw_cost / shares) if derive_total_from_col else raw_cost

        account = None
        if mapping.account:
            val = r[mapping.account]
            if pd.notna(val):
                account = str(val).strip() or None

        out.append(
            Position(
                symbol=sym,
                shares=shares,
                cost_basis=cb,
                account=account,
            )
        )
    return out, warnings
