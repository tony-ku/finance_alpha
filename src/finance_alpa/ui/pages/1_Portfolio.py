"""Portfolio page — multi-portfolio positions, prices, P/L, CSV import."""
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
from finance_alpa.ingest.yfinance_quotes import UPSERT_SQL as QUOTES_UPSERT_SQL
from finance_alpa.ingest.yfinance_quotes import fetch_symbol as fetch_quotes_for_symbol
from finance_alpa.portfolio import (
    clear_positions,
    create_portfolio,
    delete_portfolio,
    get_portfolio_by_name,
    get_positions,
    list_portfolios,
    rename_portfolio,
    seed_from_config_if_empty,
    sync_from_config,
    upsert_positions,
)
from finance_alpa.ui._theme import bootstrap

bootstrap("Portfolio · finance_alpa")
st.title("Portfolio")


# ---------- Portfolio selector ----------------------------------------

# Ensure at least one portfolio exists so the page has something to show.
seed_from_config_if_empty()
portfolios = list_portfolios()
if not portfolios:
    # No config.yaml portfolios either — bootstrap an empty default.
    create_portfolio("My Portfolio")
    portfolios = list_portfolios()

names = [p.name for p in portfolios]
selected_name = st.session_state.get("pf_selected_name")
if selected_name not in names:
    selected_name = names[0]

sel_col, new_col, rn_col, del_col = st.columns([3, 1, 1, 1])
with sel_col:
    selected_name = st.selectbox(
        "Portfolio", names, index=names.index(selected_name), key="pf_selector"
    )
    st.session_state["pf_selected_name"] = selected_name
selected = next(p for p in portfolios if p.name == selected_name)

with new_col:
    st.write("")  # align with selectbox
    if st.button("New", use_container_width=True):
        st.session_state["pf_show_new"] = True
with rn_col:
    st.write("")
    if st.button("Rename", use_container_width=True):
        st.session_state["pf_show_rename"] = True
with del_col:
    st.write("")
    if st.button("Delete", use_container_width=True, type="secondary"):
        st.session_state["pf_show_delete"] = True

if st.session_state.get("pf_show_new"):
    with st.form("pf_new_form", clear_on_submit=True):
        new_name = st.text_input("Portfolio name")
        c1, c2 = st.columns(2)
        if c1.form_submit_button("Create", type="primary"):
            if not new_name.strip():
                st.error("Name cannot be empty.")
            elif get_portfolio_by_name(new_name.strip()):
                st.error(f"A portfolio named {new_name!r} already exists.")
            else:
                p = create_portfolio(new_name)
                st.session_state["pf_selected_name"] = p.name
                st.session_state.pop("pf_show_new", None)
                st.rerun()
        if c2.form_submit_button("Cancel"):
            st.session_state.pop("pf_show_new", None)
            st.rerun()

if st.session_state.get("pf_show_rename"):
    with st.form("pf_rename_form", clear_on_submit=True):
        new_name = st.text_input("New name", value=selected.name)
        c1, c2 = st.columns(2)
        if c1.form_submit_button("Save", type="primary"):
            stripped = new_name.strip()
            if not stripped:
                st.error("Name cannot be empty.")
            elif stripped != selected.name and get_portfolio_by_name(stripped):
                st.error(f"A portfolio named {stripped!r} already exists.")
            else:
                rename_portfolio(selected.id, stripped)
                st.session_state["pf_selected_name"] = stripped
                st.session_state.pop("pf_show_rename", None)
                st.rerun()
        if c2.form_submit_button("Cancel"):
            st.session_state.pop("pf_show_rename", None)
            st.rerun()

if st.session_state.get("pf_show_delete"):
    st.warning(
        f"Delete portfolio {selected.name!r} and all its positions? "
        "This cannot be undone."
    )
    c1, c2 = st.columns(2)
    if c1.button("Confirm delete", type="primary", key="pf_del_confirm"):
        delete_portfolio(selected.id)
        st.session_state.pop("pf_selected_name", None)
        st.session_state.pop("pf_show_delete", None)
        st.rerun()
    if c2.button("Cancel", key="pf_del_cancel"):
        st.session_state.pop("pf_show_delete", None)
        st.rerun()


# ---------- Import expander --------------------------------------------

with st.expander(f"Add / edit positions in {selected.name!r}", expanded=False):
    tab_csv, tab_seed = st.tabs(["Import broker CSV", "Seed from config.yaml"])

    with tab_csv:
        st.caption(
            "Upload a broker-exported CSV. Auto-detect works for Schwab, "
            "Fidelity, Vanguard, IBKR Flex, and Robinhood formats. "
            "Positions will be imported into the selected portfolio."
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
                        "replace": "Replace — wipe existing positions in this portfolio first",
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
                    positions_parsed, warns = parse_positions(df_raw, mapping, cost_is_total)
                    st.session_state["pf_preview_rows"] = positions_parsed
                    st.session_state["pf_preview_warns"] = warns
                    st.session_state["pf_preview_mode"] = mode

                if st.session_state.get("pf_preview_rows") is not None:
                    positions_parsed = st.session_state["pf_preview_rows"]
                    warns = st.session_state["pf_preview_warns"]
                    preview_df = pd.DataFrame([p.model_dump() for p in positions_parsed])
                    st.markdown(f"**{len(positions_parsed)} positions ready to import into {selected.name!r}.**")
                    if not preview_df.empty:
                        st.dataframe(
                            preview_df, use_container_width=True, hide_index=True
                        )
                    for w in warns:
                        st.caption(f"⚠ {w}")
                    if positions_parsed and st.button(
                        f"Commit {len(positions_parsed)} positions", type="primary", key="pf_commit"
                    ):
                        n = upsert_positions(
                            selected.id,
                            positions_parsed,
                            mode=st.session_state["pf_preview_mode"],
                        )
                        st.success(
                            f"Imported {n} positions into {selected.name!r} "
                            f"(mode={st.session_state['pf_preview_mode']})."
                        )
                        for key in ("pf_preview_rows", "pf_preview_warns", "pf_preview_mode"):
                            st.session_state.pop(key, None)
                        st.rerun()

    with tab_seed:
        st.caption(
            "Replace every portfolio in the DB with those listed in `config.yaml`. "
            "Use this to reset back to the sample data."
        )
        if st.button("Seed all portfolios from config.yaml"):
            n = sync_from_config()
            st.success(f"Seeded {n} positions from config.yaml.")
            st.session_state.pop("pf_selected_name", None)
            st.rerun()

    st.divider()
    if st.button(f"Clear positions in {selected.name!r}", type="secondary"):
        n = clear_positions(selected.id)
        st.success(f"Cleared {n} positions from {selected.name!r}.")
        st.rerun()


# ---------- Main portfolio view ----------------------------------------

positions_db = get_positions(selected.id)
if not positions_db:
    st.info(
        f"No positions in {selected.name!r} yet. Use the expander above to "
        "import a broker CSV."
    )
    st.stop()

positions = pd.DataFrame([p.__dict__ for p in positions_db])


def _load_latest_prices() -> pd.DataFrame:
    with connect() as con:
        return con.execute(
            """
            SELECT symbol, close AS last_price, date AS as_of
            FROM quotes_daily
            QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) = 1
            """
        ).fetchdf()


latest = _load_latest_prices()
uncovered = sorted(set(positions["symbol"]) - set(latest["symbol"]))
# On first load the scheduler's yfinance pass may not have populated quotes yet.
# Fetch synchronously for held symbols so the page never renders empty prices.
# Gate with session_state so a persistently-failing symbol doesn't re-fetch on every rerender.
_FETCH_FLAG = "pf_autofetch_attempted"
if uncovered and not st.session_state.get(_FETCH_FLAG):
    st.session_state[_FETCH_FLAG] = True
    with st.spinner(f"Loading prices for {len(uncovered)} symbol(s)…"):
        with connect() as con:
            for sym in uncovered:
                try:
                    rows = fetch_quotes_for_symbol(sym)
                except Exception:
                    continue
                if rows:
                    con.executemany(QUOTES_UPSERT_SQL, rows)
    latest = _load_latest_prices()

df = positions.merge(latest, on="symbol", how="left")
# Format as_of as string before the totals row (None) is concatenated — otherwise
# the column becomes object dtype and Streamlit renders epoch nanoseconds.
df["as_of"] = df["as_of"].apply(
    lambda d: d.strftime("%Y-%m-%d") if pd.notna(d) else None
)
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

display_df = df.drop(columns=["portfolio_id"], errors="ignore")
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
display = pd.concat([display_df, pd.DataFrame([totals_row])], ignore_index=True)

st.dataframe(
    display,
    use_container_width=True,
    hide_index=True,
    column_config={
        "symbol": st.column_config.TextColumn("Symbol"),
        "shares": st.column_config.NumberColumn("Shares"),
        "cost_basis": st.column_config.NumberColumn("Cost Basis", format="$%.2f"),
        "account": st.column_config.TextColumn("Account"),
        "last_price": st.column_config.NumberColumn("Last Price", format="$%.2f"),
        "as_of": st.column_config.TextColumn("As Of"),
        "market_value": st.column_config.NumberColumn("Market Value", format="$%.2f"),
        "cost": st.column_config.NumberColumn("Total Cost", format="$%.2f"),
        "pl_abs": st.column_config.NumberColumn("P/L ($)", format="$%.2f"),
        "pl_pct": st.column_config.NumberColumn("P/L (%)", format="%.2f%%"),
    },
)

missing = df[df["last_price"].isna()]["symbol"].tolist()
if missing:
    st.warning(
        f"No price data for: {', '.join(missing)}. "
        "Run the yfinance ingest (scheduler does this automatically) to populate."
    )
