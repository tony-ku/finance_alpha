"""Portfolio page — positions (DB-backed), latest prices, P/L, CSV import."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from finance_alpa.db import connect
from finance_alpa.ingest.broker_csv import (
    ColumnMapping,
    detect_columns,
    parse_positions,
    read_csv,
)
from finance_alpa.portfolio import (
    clear_positions,
    get_positions,
    sync_from_config,
    upsert_positions,
)
from finance_alpa.ui._theme import bootstrap

bootstrap()
st.title("Portfolio")


# ---------- Import expander --------------------------------------------

with st.expander("Add / edit positions", expanded=False):
    tab_csv, tab_seed = st.tabs(["Import broker CSV", "Seed from config.yaml"])

    with tab_csv:
        st.caption(
            "Upload a broker-exported CSV. Auto-detect works for Schwab, "
            "Fidelity, Vanguard, IBKR Flex, and Robinhood formats. "
            "Review the column mapping and preview before committing."
        )
        uploaded = st.file_uploader(
            "CSV file", type=["csv"], accept_multiple_files=False, key="pf_csv"
        )
        if uploaded is not None:
            try:
                df_raw = read_csv(uploaded.getvalue())
            except Exception as e:
                st.error(f"Could not parse CSV: {e}")
                df_raw = None

            if df_raw is not None and not df_raw.empty:
                st.markdown(f"**Detected {len(df_raw)} rows · {len(df_raw.columns)} columns**")
                st.dataframe(df_raw.head(10), use_container_width=True, hide_index=True)

                detected = detect_columns(df_raw)
                cols = [None, *df_raw.columns]

                st.markdown("**Column mapping** (adjust if auto-detect is wrong)")
                m_c1, m_c2, m_c3, m_c4 = st.columns(4)
                sel_symbol = m_c1.selectbox(
                    "Symbol", cols, index=cols.index(detected.symbol) if detected.symbol in cols else 0
                )
                sel_shares = m_c2.selectbox(
                    "Shares", cols, index=cols.index(detected.shares) if detected.shares in cols else 0
                )
                cost_choice = detected.cost_basis or detected.cost_basis_total
                sel_cost = m_c3.selectbox(
                    "Cost basis (optional)",
                    cols,
                    index=cols.index(cost_choice) if cost_choice in cols else 0,
                )
                sel_account = m_c4.selectbox(
                    "Account (optional)",
                    cols,
                    index=cols.index(detected.account) if detected.account in cols else 0,
                )
                cost_is_total = st.checkbox(
                    "Cost column is a TOTAL (not per-share) — divide by shares on import",
                    value=bool(detected.cost_basis_total and not detected.cost_basis),
                )
                mode = st.radio(
                    "Import mode",
                    ["upsert", "replace"],
                    format_func=lambda v: {
                        "upsert": "Upsert — update existing symbols, keep others",
                        "replace": "Replace — wipe existing positions first",
                    }[v],
                    horizontal=False,
                    key="pf_mode",
                )

                if detected.notes:
                    for n in detected.notes:
                        st.caption(f"• {n}")

                mapping = ColumnMapping(
                    symbol=sel_symbol,
                    shares=sel_shares,
                    cost_basis=None if cost_is_total else sel_cost,
                    cost_basis_total=sel_cost if cost_is_total else None,
                    account=sel_account,
                )

                if st.button("Preview parsed rows", key="pf_preview"):
                    positions, warns = parse_positions(df_raw, mapping, cost_is_total)
                    st.session_state["pf_preview_rows"] = positions
                    st.session_state["pf_preview_warns"] = warns
                    st.session_state["pf_preview_mode"] = mode

                if st.session_state.get("pf_preview_rows") is not None:
                    positions = st.session_state["pf_preview_rows"]
                    warns = st.session_state["pf_preview_warns"]
                    preview_df = pd.DataFrame([p.model_dump() for p in positions])
                    st.markdown(f"**{len(positions)} positions ready to import.**")
                    if not preview_df.empty:
                        st.dataframe(
                            preview_df, use_container_width=True, hide_index=True
                        )
                    for w in warns:
                        st.caption(f"⚠ {w}")
                    if positions and st.button(
                        f"Commit {len(positions)} positions", type="primary", key="pf_commit"
                    ):
                        n = upsert_positions(positions, mode=st.session_state["pf_preview_mode"])
                        st.success(
                            f"Imported {n} positions (mode="
                            f"{st.session_state['pf_preview_mode']})."
                        )
                        for key in ("pf_preview_rows", "pf_preview_warns", "pf_preview_mode"):
                            st.session_state.pop(key, None)
                        st.rerun()

    with tab_seed:
        st.caption(
            "Copies the `portfolio:` block from `config.yaml` into the DB, "
            "replacing any existing positions."
        )
        if st.button("Seed from config.yaml"):
            n = sync_from_config()
            st.success(f"Seeded {n} positions from config.yaml.")
            st.rerun()

    st.divider()
    if st.button("Clear all positions", type="secondary"):
        n = clear_positions()
        st.success(f"Cleared {n} positions.")
        st.rerun()


# ---------- Main portfolio view ----------------------------------------

positions_db = get_positions()
if not positions_db:
    st.info(
        "No positions in the DB yet. Use the expander above to import a broker "
        "CSV or seed from `config.yaml`."
    )
    st.stop()

positions = pd.DataFrame([p.__dict__ for p in positions_db])

with connect() as con:
    latest = con.execute(
        """
        SELECT symbol, close AS last_price, date AS as_of
        FROM quotes_daily
        QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) = 1
        """
    ).fetchdf()

df = positions.merge(latest, on="symbol", how="left")
cost_basis = df["cost_basis"].fillna(0)
df["market_value"] = df["shares"] * df["last_price"]
df["cost"] = df["shares"] * cost_basis
df["pl_abs"] = df["market_value"] - df["cost"]
df["pl_pct"] = (df["pl_abs"] / df["cost"].replace(0, pd.NA)) * 100

total_mv = float(df["market_value"].sum(skipna=True))
total_cost = float(df["cost"].sum())
total_pl = float(df["pl_abs"].sum(skipna=True))
total_pl_pct = (total_pl / total_cost * 100) if total_cost else None

h1, h2, h3, h4 = st.columns(4)
h1.metric("Positions", len(df))
h2.metric("Market value", f"${total_mv:,.2f}")
h3.metric("Cost basis", f"${total_cost:,.2f}")
h4.metric(
    "Total P/L",
    f"${total_pl:+,.2f}",
    f"{total_pl_pct:+.2f}%" if total_pl_pct is not None else None,
)

totals_row = {
    "symbol": "TOTAL",
    "shares": None,
    "cost_basis": None,
    "account": None,
    "last_price": None,
    "as_of": None,
    "market_value": total_mv,
    "cost": total_cost,
    "pl_abs": total_pl,
    "pl_pct": total_pl_pct,
}
display = pd.concat([df, pd.DataFrame([totals_row])], ignore_index=True)

st.dataframe(
    display,
    use_container_width=True,
    hide_index=True,
    column_config={
        "last_price": st.column_config.NumberColumn(format="$%.2f"),
        "cost_basis": st.column_config.NumberColumn(format="$%.2f"),
        "market_value": st.column_config.NumberColumn(format="$%.2f"),
        "cost": st.column_config.NumberColumn(format="$%.2f"),
        "pl_abs": st.column_config.NumberColumn(format="$%.2f"),
        "pl_pct": st.column_config.NumberColumn(format="%.2f%%"),
    },
)

missing = df[df["last_price"].isna()]["symbol"].tolist()
if missing:
    st.warning(
        f"No price data for: {', '.join(missing)}. "
        "Run the yfinance ingest (scheduler does this automatically) to populate."
    )
