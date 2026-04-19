"""News page — SA RSS + Finnhub articles."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import streamlit as st

from finance_alpa.db import connect
from finance_alpa.ui._theme import bootstrap, safe_link_url

bootstrap()
st.title("News")

top = st.columns([1, 2, 1, 1])
with top[0]:
    days = st.slider("Lookback (days)", 1, 30, 7)
with top[1]:
    ticker = st.text_input(
        "Filter by ticker (optional)", placeholder="AAPL"
    ).strip().upper()
with top[2]:
    hide_read = st.toggle("Hide read", value=True)
with top[3]:
    source_filter = st.selectbox("Source", ["all", "sa_rss", "finnhub"], index=0)

cutoff = datetime.now(tz=timezone.utc).replace(tzinfo=None) - timedelta(days=days)

query = """
    SELECT source, url, published_at, title, author, tickers, summary, read
    FROM articles
    WHERE published_at >= ?
"""
params: list = [cutoff]
if ticker:
    query += " AND list_contains(tickers, ?)"
    params.append(ticker)
if source_filter != "all":
    query += " AND source = ?"
    params.append(source_filter)
if hide_read:
    query += " AND NOT read"
query += " ORDER BY published_at DESC NULLS LAST LIMIT 500"

# Also pull news-table rows (Finnhub) which live in a parallel table.
news_query = """
    SELECT source, url, published_at, title, NULL AS author, tickers, summary, read
    FROM news
    WHERE published_at >= ?
"""
news_params: list = [cutoff]
if ticker:
    news_query += " AND list_contains(tickers, ?)"
    news_params.append(ticker)
if source_filter != "all":
    news_query += " AND source = ?"
    news_params.append(source_filter)
if hide_read:
    news_query += " AND NOT read"
news_query += " ORDER BY published_at DESC NULLS LAST LIMIT 500"

with connect(read_only=True) as con:
    df_articles = con.execute(query, params).fetchdf()
    df_news = con.execute(news_query, news_params).fetchdf()

import pandas as pd  # noqa: E402

df = pd.concat([df_articles, df_news], ignore_index=True)
if not df.empty:
    df = df.sort_values("published_at", ascending=False, na_position="last").head(500)

if df.empty:
    st.info(
        "No articles match the current filters. Try widening the lookback or "
        "turning off 'Hide read'."
    )
    st.stop()

c_left, c_right = st.columns([3, 1])
c_left.caption(f"{len(df)} articles")
if c_right.button("Mark all shown as read", use_container_width=True):
    urls_by_source: dict[str, list[str]] = {}
    for src, url in zip(df["source"], df["url"]):
        urls_by_source.setdefault(src, []).append(url)
    with connect() as con:
        for src, urls in urls_by_source.items():
            table = "news" if src == "finnhub" else "articles"
            con.execute(
                f"UPDATE {table} SET read = TRUE WHERE source = ? AND url = ANY(?)",
                [src, urls],
            )
    st.success(f"Marked {len(df)} as read.")
    st.rerun()

for _, row in df.iterrows():
    with st.container(border=True):
        head, right = st.columns([6, 1])
        url = safe_link_url(row["url"])
        title = str(row["title"])
        head.markdown(f"**[{title}]({url})**" if url else f"**{title}**")
        bits = [str(row["source"]), str(row["published_at"])]
        if row.get("author"):
            bits.append(str(row["author"]))
        tix = row.get("tickers")
        if isinstance(tix, list) and tix:
            bits.append(", ".join(tix))
        head.caption(" · ".join(bits))
        if row.get("summary"):
            head.write(row["summary"])
        if right.button("Mark read", key=f"r_{row['source']}_{row['url']}"):
            table = "news" if row["source"] == "finnhub" else "articles"
            with connect() as con:
                con.execute(
                    f"UPDATE {table} SET read = TRUE WHERE source = ? AND url = ?",
                    [row["source"], row["url"]],
                )
            st.rerun()
