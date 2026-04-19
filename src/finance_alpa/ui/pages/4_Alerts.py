"""Alerts page — fired alerts log, configured rules, test notification."""
from __future__ import annotations

import json

import pandas as pd
import streamlit as st

from finance_alpa.alerts.notify import notify
from finance_alpa.alerts.rules import evaluate_all
from finance_alpa.config import load_app_config
from finance_alpa.db import connect
from finance_alpa.ui._theme import bootstrap

bootstrap()
st.title("Alerts")

cfg = load_app_config()

# --- Configured rules ---------------------------------------------------
st.subheader("Configured rules")
if not cfg.alerts:
    st.info("No alert rules in `config.yaml`. Add entries under `alerts:`.")
else:
    rules_df = pd.DataFrame([r.model_dump() for r in cfg.alerts])
    st.dataframe(rules_df, use_container_width=True, hide_index=True)

# --- Actions ------------------------------------------------------------
c1, c2 = st.columns(2)
with c1:
    if st.button("Evaluate rules now"):
        with st.spinner("Evaluating…"):
            n = evaluate_all()
        st.success(f"Evaluator fired {n} new alerts.")
with c2:
    if st.button("Send test desktop notification"):
        notify(
            "finance_alpa test",
            "If you can see this toast, desktop notifications work.",
            ["desktop"],
        )
        st.success("Test notification sent — check your system tray.")

st.divider()

# --- Fired alerts log ---------------------------------------------------
st.subheader("Recent alerts")
with connect(read_only=True) as con:
    df = con.execute(
        """
        SELECT id, fired_at, rule_name, symbol, payload
        FROM alerts_log
        ORDER BY fired_at DESC
        LIMIT 200
        """
    ).fetchdf()

if df.empty:
    st.info(
        "No alerts yet. The scheduler evaluates rules every 5 minutes when "
        "running (`python -m finance_alpa.scheduler`)."
    )
    st.stop()


def _payload_field(raw, key):
    if not raw:
        return None
    try:
        return json.loads(raw).get(key)
    except Exception:
        return None


df["title"] = df["payload"].apply(lambda p: _payload_field(p, "title"))
df["body"] = df["payload"].apply(lambda p: _payload_field(p, "body"))

st.dataframe(
    df[["fired_at", "rule_name", "symbol", "title", "body"]],
    use_container_width=True,
    hide_index=True,
    column_config={
        "fired_at": st.column_config.DatetimeColumn("Fired"),
        "rule_name": "Rule",
        "symbol": "Symbol",
        "title": "Title",
        "body": "Details",
    },
)
