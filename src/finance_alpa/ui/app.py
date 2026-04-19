"""finance_alpa — Streamlit entrypoint (home page)."""
from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import yfinance as yf

from finance_alpa.config import load_app_config
from finance_alpa.db import connect, init_db
from finance_alpa.ui._theme import bootstrap, safe_link_url

st.set_page_config(page_title="finance_alpa", layout="wide")
init_db()
bootstrap()

INDICES: list[tuple[str, str]] = [
    ("^GSPC", "S&P 500"),
    ("^DJI", "Dow 30"),
    ("^IXIC", "Nasdaq"),
    ("^RUT", "Russell 2000"),
    ("^VIX", "VIX"),
]

RANGE_OPTIONS = ["1D", "5D", "1M", "3M", "6M", "1Y", "3Y", "5Y", "10Y", "MAX"]
# Each range maps to yfinance history() kwargs. `years` means start = today - N years.
RANGE_CONFIG: dict[str, dict] = {
    "1D":  {"period": "1d",  "interval": "5m"},
    "5D":  {"period": "5d",  "interval": "30m"},
    "1M":  {"period": "1mo", "interval": "1d"},
    "3M":  {"period": "3mo", "interval": "1d"},
    "6M":  {"period": "6mo", "interval": "1d"},
    "1Y":  {"period": "1y",  "interval": "1d"},
    "3Y":  {"years": 3,      "interval": "1wk"},
    "5Y":  {"period": "5y",  "interval": "1wk"},
    "10Y": {"period": "10y", "interval": "1wk"},
    "MAX": {"period": "max", "interval": "1mo"},
}
DEFAULT_RANGE = "1Y"


def _last_prev_close(symbol: str) -> tuple[float, float | None] | None:
    try:
        hist = yf.Ticker(symbol).history(
            period="5d", interval="1d", auto_adjust=False
        )
    except Exception:
        return None
    closes = hist["Close"].dropna() if not hist.empty else hist
    if closes is None or len(closes) == 0:
        return None
    last = float(closes.iloc[-1])
    prev = float(closes.iloc[-2]) if len(closes) > 1 else None
    return last, prev


@st.cache_data(ttl=60, show_spinner=False)
def fetch_index_snapshot(symbol: str) -> dict | None:
    r = _last_prev_close(symbol)
    if r is None:
        return None
    last, prev = r
    return {
        "symbol": symbol,
        "last": last,
        "prev_close": prev,
        "change": (last - prev) if prev is not None else None,
        "change_pct": ((last - prev) / prev * 100) if prev else None,
    }


@st.cache_data(ttl=300, show_spinner=False)
def fetch_ticker_snapshot(symbol: str) -> dict | None:
    r = _last_prev_close(symbol)
    if r is None:
        return None
    last, prev = r
    try:
        info = yf.Ticker(symbol).info or {}
    except Exception:
        info = {}
    return {
        "symbol": symbol,
        "name": info.get("longName") or info.get("shortName") or symbol,
        "sector": info.get("sector"),
        "industry": info.get("industry"),
        "last": last,
        "prev_close": prev,
        "change": (last - prev) if prev is not None else None,
        "change_pct": ((last - prev) / prev * 100) if prev else None,
        "market_cap": info.get("marketCap"),
        "pe": info.get("trailingPE"),
        "forward_pe": info.get("forwardPE"),
        "eps": info.get("trailingEps"),
        # yfinance 1.3.x returns dividendYield already as a percentage (e.g. 0.38 = 0.38%).
        "dividend_yield": info.get("dividendYield"),
        "week52_high": info.get("fiftyTwoWeekHigh"),
        "week52_low": info.get("fiftyTwoWeekLow"),
        "volume": info.get("volume") or info.get("averageVolume"),
        "currency": info.get("currency") or "USD",
        "website": info.get("website"),
        "summary": info.get("longBusinessSummary"),
    }


@st.cache_data(ttl=120, show_spinner=False)
def fetch_ohlc(symbol: str, range_key: str) -> pd.DataFrame:
    cfg = RANGE_CONFIG[range_key]
    kwargs = {"interval": cfg["interval"], "auto_adjust": False}
    if "period" in cfg:
        kwargs["period"] = cfg["period"]
    elif "years" in cfg:
        kwargs["start"] = (datetime.now() - timedelta(days=365 * cfg["years"])).date()
    try:
        df = yf.Ticker(symbol).history(**kwargs)
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    df = df.reset_index()
    if "Datetime" in df.columns:
        df = df.rename(columns={"Datetime": "ts"})
    elif "Date" in df.columns:
        df = df.rename(columns={"Date": "ts"})
    return df[["ts", "Open", "High", "Low", "Close", "Volume"]]


CHART_BG = "#1e1a2c"
PAPER_BG = "#13111c"
GRID_COLOR = "#2e2740"
AXIS_COLOR = "#a096b8"
UP_COLOR = "#7fff7f"
DOWN_COLOR = "#ff5c6c"


def render_candlestick(df: pd.DataFrame, symbol: str, interval: str) -> go.Figure:
    fig = go.Figure(
        data=[
            go.Candlestick(
                x=df["ts"],
                open=df["Open"],
                high=df["High"],
                low=df["Low"],
                close=df["Close"],
                name=symbol,
                increasing_line_color=UP_COLOR,
                increasing_fillcolor=UP_COLOR,
                decreasing_line_color=DOWN_COLOR,
                decreasing_fillcolor=DOWN_COLOR,
            )
        ]
    )
    fig.update_layout(
        height=480,
        margin=dict(l=10, r=10, t=10, b=10),
        xaxis_rangeslider_visible=False,
        template="plotly_dark",
        paper_bgcolor=PAPER_BG,
        plot_bgcolor=CHART_BG,
        yaxis_title="Price",
        showlegend=False,
        font=dict(color="#e0dded"),
    )
    fig.update_yaxes(
        autorange=True,
        rangemode="normal",
        fixedrange=False,
        gridcolor=GRID_COLOR,
        zerolinecolor=GRID_COLOR,
        tickfont=dict(color=AXIS_COLOR),
    )
    fig.update_xaxes(
        gridcolor=GRID_COLOR,
        tickfont=dict(color=AXIS_COLOR),
    )
    if interval == "1d":
        fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])])
    return fig


def _money(v) -> str:
    return f"${v:,.2f}" if v is not None else "—"


def _large(v) -> str:
    if v is None:
        return "—"
    for unit, div in (("T", 1e12), ("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if abs(v) >= div:
            return f"${v / div:,.2f}{unit}"
    return f"${v:,.2f}"


def _num(v, fmt: str = "{:.2f}") -> str:
    return fmt.format(v) if v is not None else "—"


# ---------- Page ----------

st.title("finance_alpa")
st.caption("Local financial workspace — portfolio, news, screener, alerts.")

# --- US Market Conditions ------------------------------------------------
st.subheader("US Market Conditions")
cols = st.columns(len(INDICES))
for col, (sym, label) in zip(cols, INDICES):
    snap = fetch_index_snapshot(sym)
    with col:
        if snap is None:
            st.metric(label, "—", help=sym)
        else:
            st.metric(
                label,
                f"{snap['last']:,.2f}",
                f"{snap['change']:+,.2f} ({snap['change_pct']:+.2f}%)",
                help=sym,
            )
st.caption(
    f"Last refresh: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} · cached 60s · "
    "source: Yahoo Finance"
)

st.divider()

# --- Symbol lookup -------------------------------------------------------
st.subheader("Symbol Lookup")
with st.form("symbol_form", clear_on_submit=False):
    c1, c2 = st.columns([4, 1])
    raw_sym = c1.text_input(
        "Ticker symbol",
        placeholder="e.g. AAPL, MSFT, TSLA",
        label_visibility="collapsed",
    )
    submitted = c2.form_submit_button("Look up", use_container_width=True)

if submitted and raw_sym.strip():
    st.session_state["lookup_symbol"] = raw_sym.strip().upper()

current_sym: str | None = st.session_state.get("lookup_symbol")

if current_sym:
    with st.spinner(f"Fetching {current_sym}…"):
        snap = fetch_ticker_snapshot(current_sym)

    if snap is None:
        st.error(
            f"No price data for '{current_sym}'. Check the symbol and try again."
        )
    else:
        header_l, header_r = st.columns([3, 2])
        header_l.markdown(f"### {snap['name']} ({snap['symbol']})")
        if snap.get("sector"):
            sub = snap["sector"]
            if snap.get("industry"):
                sub += f" · {snap['industry']}"
            header_l.caption(sub)
        delta = (
            f"{snap['change']:+.2f} ({snap['change_pct']:+.2f}%)"
            if snap["change"] is not None
            else None
        )
        header_r.metric("Last", _money(snap["last"]), delta)

        row1 = st.columns(4)
        row1[0].metric("Market cap", _large(snap["market_cap"]))
        row1[1].metric("P/E (TTM)", _num(snap["pe"]))
        row1[2].metric("Fwd P/E", _num(snap["forward_pe"]))
        row1[3].metric("EPS (TTM)", _money(snap["eps"]))

        row2 = st.columns(4)
        dy = snap["dividend_yield"]
        row2[0].metric("Div yield", f"{dy:.2f}%" if dy is not None else "—")
        row2[1].metric("52w high", _money(snap["week52_high"]))
        row2[2].metric("52w low", _money(snap["week52_low"]))
        row2[3].metric(
            "Volume", f"{int(snap['volume']):,}" if snap["volume"] else "—"
        )

        # Range selector + candlestick chart
        selected_range = st.segmented_control(
            "Range",
            RANGE_OPTIONS,
            default=DEFAULT_RANGE,
            key="ohlc_range",
            label_visibility="collapsed",
        )
        if not selected_range:
            selected_range = DEFAULT_RANGE

        ohlc = fetch_ohlc(current_sym, selected_range)
        if ohlc.empty:
            st.warning(f"No OHLC data returned for {current_sym} over {selected_range}.")
        else:
            interval = RANGE_CONFIG[selected_range]["interval"]
            fig = render_candlestick(ohlc, current_sym, interval)
            st.plotly_chart(fig, use_container_width=True, config={"displaylogo": False})
            st.caption(
                f"{len(ohlc)} candles · interval {interval} · "
                f"{ohlc['ts'].iloc[0]} → {ohlc['ts'].iloc[-1]}"
            )

        if snap.get("summary"):
            with st.expander("Company overview"):
                st.write(snap["summary"])
                site = safe_link_url(snap.get("website"))
                if site:
                    st.markdown(f"[{site}]({site})")

        with connect() as con:
            related = con.execute(
                """
                SELECT source, published_at, title, author, url
                FROM articles
                WHERE list_contains(tickers, ?)
                ORDER BY published_at DESC NULLS LAST
                LIMIT 10
                """,
                [current_sym],
            ).fetchdf()
        if not related.empty:
            st.markdown("#### Recent articles mentioning this ticker")
            for _, r in related.iterrows():
                meta_bits = [str(r["source"]), str(r["published_at"])]
                if r.get("author"):
                    meta_bits.append(str(r["author"]))
                url = safe_link_url(r["url"])
                title = str(r["title"])
                line = f"- [{title}]({url})" if url else f"- {title}"
                st.markdown(line + f"  \n  _{' · '.join(meta_bits)}_")

st.divider()

# --- Local app status ---------------------------------------------------
cfg = load_app_config()
with connect() as con:
    n_quotes = con.execute("SELECT COUNT(*) FROM quotes_daily").fetchone()[0]
    n_articles = con.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    n_alerts = con.execute("SELECT COUNT(*) FROM alerts_log").fetchone()[0]
    last_quote = con.execute("SELECT MAX(date) FROM quotes_daily").fetchone()[0]

s1, s2, s3, s4, s5 = st.columns(5)
s1.metric("Tracked symbols", len(cfg.all_symbols()))
s2.metric("Quote rows", f"{n_quotes:,}")
s3.metric("Articles", f"{n_articles:,}")
s4.metric("Alerts fired", f"{n_alerts:,}")
s5.metric("Last quote", str(last_quote) if last_quote else "—")

# --- Scheduler status --------------------------------------------------
from finance_alpa.scheduler import get_scheduler_status  # noqa: E402

status = get_scheduler_status()
with st.expander(
    f"Scheduler · {len(status)} jobs" if status else "Scheduler · not running",
    expanded=False,
):
    if not status:
        st.caption(
            "Background scheduler not running. Re-render any page to start it."
        )
    else:
        import pandas as _pd

        status_df = _pd.DataFrame(status)
        st.dataframe(
            status_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "id": "Job",
                "name": "Name",
                "trigger": "Trigger",
                "next_run": st.column_config.DatetimeColumn("Next run"),
                "last_run": st.column_config.DatetimeColumn("Last run"),
                "last_ok": st.column_config.CheckboxColumn("OK"),
                "last_error": "Error",
            },
        )
