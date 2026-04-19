"""Shared visual theme + per-page bootstrap for all Streamlit pages."""
from __future__ import annotations

import logging

import streamlit as st

logger = logging.getLogger(__name__)

_CSS = """
<style>
  h1 {
    color: #ff6ec7 !important;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    font-weight: 700;
  }
  h2, h3 {
    color: #ff6ec7 !important;
    letter-spacing: 0.05em;
    text-transform: uppercase;
  }
  [data-testid="stMetricLabel"] {
    color: #a096b8 !important;
    text-transform: uppercase;
    letter-spacing: 0.1em;
    font-size: 0.75rem;
  }
  [data-testid="stMetricValue"] {
    color: #e0dded !important;
    font-variant-numeric: tabular-nums;
    font-weight: 600;
  }
  [data-testid="stCaptionContainer"], .stCaption {
    color: #8a7fa3 !important;
    letter-spacing: 0.03em;
  }
  div[data-testid="stMetric"] {
    background: #1e1a2c;
    border: 1px solid #2e2740;
    border-radius: 6px;
    padding: 0.75rem 1rem;
  }
</style>
"""


def apply_theme() -> None:
    st.markdown(_CSS, unsafe_allow_html=True)


def bootstrap() -> None:
    """Call at the top of every Streamlit page.

    Applies the shared CSS theme and starts the embedded background scheduler
    (idempotent — safe to call on every page re-render).
    """
    apply_theme()
    # Lazy import: scheduler pulls APScheduler + ingest modules; keep it off the
    # import path for non-UI callers.
    from finance_alpa.scheduler import start_background_scheduler

    try:
        start_background_scheduler()
    except Exception:
        logger.exception("Failed to start background scheduler — UI still usable")
