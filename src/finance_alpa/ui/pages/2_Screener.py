"""Screener page — composite ranking of tracked universe."""
from __future__ import annotations

from datetime import datetime

import pandas as pd
import streamlit as st

from finance_alpa.screeners.rank import rank
from finance_alpa.ui._theme import bootstrap

bootstrap()
st.title("Screener")

with st.sidebar:
    st.header("Weights")
    w_fmp = st.slider("FMP rating", 0.0, 1.0, 0.4, 0.05)
    w_mom = st.slider("Momentum (3m)", 0.0, 1.0, 0.3, 0.05)
    w_reco = st.slider("Analyst reco (Finnhub)", 0.0, 1.0, 0.3, 0.05)
    st.caption(f"Total weight: {w_fmp + w_mom + w_reco:.2f} (auto-normalized)")

df = rank({"fmp": w_fmp, "momentum": w_mom, "reco": w_reco})

if df.empty:
    st.info(
        "No symbols to rank. Add tickers under `universe.watchlist` in "
        "`config.yaml`, then run the ingest scripts:\n\n"
        "```bash\n"
        "python -m finance_alpa.ingest.yfinance_quotes\n"
        "python -m finance_alpa.ingest.fmp\n"
        "python -m finance_alpa.ingest.finnhub_news\n"
        "```"
    )
    st.stop()

# Coverage metrics — how many symbols have each signal populated.
cov = {
    "FMP rating": df["fmp_score"].notna().sum(),
    "Momentum": df["momentum_3m"].notna().sum(),
    "Finnhub reco": df["reco_mean"].notna().sum(),
    "Total": len(df),
}
cov_cols = st.columns(4)
for (label, val), col in zip(cov.items(), cov_cols):
    col.metric(label, f"{val}")

st.caption(
    "Data coverage. Run the FMP and Finnhub ingests to populate ratings and "
    "recommendations."
)

display = df[
    [
        "symbol",
        "last_price",
        "momentum_3m",
        "fmp_rating",
        "fmp_score",
        "reco_label",
        "reco_mean",
        "next_earnings_date",
        "composite",
    ]
].copy()
display.insert(0, "rank", display.index + 1)

st.dataframe(
    display,
    use_container_width=True,
    hide_index=True,
    column_config={
        "rank": st.column_config.NumberColumn("#", width="small"),
        "symbol": "Symbol",
        "last_price": st.column_config.NumberColumn("Last", format="$%.2f"),
        "momentum_3m": st.column_config.NumberColumn("Mom 3m", format="%.2f%%"),
        "fmp_rating": "FMP",
        "fmp_score": st.column_config.NumberColumn("FMP Score", format="%.2f"),
        "reco_label": "Reco",
        "reco_mean": st.column_config.NumberColumn("Reco μ", format="%.2f"),
        "next_earnings_date": st.column_config.DateColumn("Next ER"),
        "composite": st.column_config.NumberColumn("Composite", format="%.3f"),
    },
)

st.download_button(
    "Export CSV",
    data=display.to_csv(index=False).encode("utf-8"),
    file_name=f"screener_{datetime.now():%Y%m%d_%H%M}.csv",
    mime="text/csv",
)

with st.expander("How the composite score is computed"):
    st.markdown(
        """
Each signal is **z-scored** across the current universe (mean 0, std 1), then
combined by your weights:

- **FMP rating score** — higher is better.
- **Momentum (3m)** — 63-trading-day price change, higher is better.
- **Finnhub recommendation mean** — 1 = Strong Buy … 5 = Strong Sell.
  Inverted so a lower mean contributes positively.

Weights are auto-normalized to sum to 1. Missing signals are treated as 0
(average) rather than excluded so the ranking stays stable as coverage grows.
"""
    )
