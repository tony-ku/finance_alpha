"""Scheduler for finance_alpa — runs ingest + alert jobs on cadence.

Two modes:
  1. **Embedded** (default): `start_background_scheduler()` is called from the
     Streamlit app. Jobs run in daemon threads inside the UI process.
  2. **Standalone**: `python -m finance_alpa.scheduler` — a blocking scheduler
     suitable for running on a headless host without the UI.

The scheduler is the sole writer to data/finance.duckdb. The Streamlit UI
opens the DB read-only, so multiple readers + one writer is safe.
"""
from __future__ import annotations

import atexit
import logging
import threading
from datetime import datetime, timezone
from typing import Any

import duckdb
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.base import BaseScheduler
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from .alerts.rules import evaluate_all
from .config import DB_PATH
from .db import init_db
from .ingest import fmp as fmp_ingest
from .ingest import finnhub_news as finnhub_ingest
from .ingest import sa_rss as rss_ingest
from .ingest import yfinance_quotes as yfinance_ingest

logger = logging.getLogger(__name__)

TIMEZONE = "America/New_York"

_bg_scheduler: BackgroundScheduler | None = None
_bg_lock = threading.Lock()
_bg_startup_started = False
_last_runs: dict[str, dict[str, Any]] = {}
# Long-lived write connection that anchors the DuckDB instance in RW mode for
# the process lifetime. Without this, a UI page that opens a read-only
# connection first can lock the instance into RO mode, causing subsequent
# ingest-thread write connections to fail ("Cannot switch from read-only…").
_anchor_con: duckdb.DuckDBPyConnection | None = None


def _close_anchor() -> None:
    global _anchor_con
    if _anchor_con is not None:
        try:
            _anchor_con.close()
        except Exception:
            pass
        _anchor_con = None


def _on_job_event(event) -> None:
    _last_runs[event.job_id] = {
        "ts": datetime.now(tz=timezone.utc),
        "ok": event.exception is None,
        "error": repr(event.exception) if event.exception else None,
    }


def _safe(fn, name: str):
    def wrapped() -> None:
        logger.info("→ job %s", name)
        try:
            fn()
        except Exception:
            logger.exception("Job %s failed", name)
    wrapped.__name__ = name
    return wrapped


def _add_jobs(sched: BaseScheduler) -> None:
    common = dict(max_instances=1, coalesce=True, misfire_grace_time=300)
    sched.add_job(
        _safe(rss_ingest.main, "sa_rss"),
        IntervalTrigger(minutes=30),
        id="sa_rss", name="SA RSS ingest", **common,
    )
    sched.add_job(
        _safe(finnhub_ingest.main, "finnhub"),
        IntervalTrigger(minutes=30),
        id="finnhub", name="Finnhub news + reco", **common,
    )
    sched.add_job(
        _safe(yfinance_ingest.main, "yfinance_daily"),
        CronTrigger(hour=17, minute=0),
        id="yfinance_daily", name="yfinance daily OHLCV", **common,
    )
    sched.add_job(
        _safe(fmp_ingest.main, "fmp_daily"),
        CronTrigger(hour=17, minute=30),
        id="fmp_daily", name="FMP ratings / estimates / earnings", **common,
    )
    sched.add_job(
        _safe(evaluate_all, "alerts_eval"),
        IntervalTrigger(minutes=5),
        id="alerts_eval", name="Alert rule evaluator", **common,
    )


def _startup_pass() -> None:
    """Run quick ingests + alert evaluation once, so fresh data appears shortly
    after startup instead of waiting for the first scheduled tick."""
    for fn, name in (
        (rss_ingest.main, "sa_rss"),
        (yfinance_ingest.main, "yfinance_daily"),
        (evaluate_all, "alerts_eval"),
    ):
        try:
            fn()
        except Exception:
            logger.exception("Startup %s failed", name)


def start_background_scheduler() -> BackgroundScheduler:
    """Start (once) the embedded scheduler. Safe to call repeatedly.

    The startup pass runs on a daemon thread so UI rendering is never blocked.
    """
    global _bg_scheduler, _bg_startup_started, _anchor_con
    with _bg_lock:
        if _bg_scheduler is not None and _bg_scheduler.running:
            return _bg_scheduler
        init_db()
        if _anchor_con is None:
            _anchor_con = duckdb.connect(str(DB_PATH))
            atexit.register(_close_anchor)
        sched = BackgroundScheduler(timezone=TIMEZONE)
        _add_jobs(sched)
        sched.add_listener(_on_job_event, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
        sched.start()
        _bg_scheduler = sched
        if not _bg_startup_started:
            _bg_startup_started = True
            threading.Thread(
                target=_startup_pass,
                daemon=True,
                name="finance_alpa-startup",
            ).start()
    logger.info(
        "Background scheduler started — %d jobs (%s)",
        len(sched.get_jobs()),
        ", ".join(j.id for j in sched.get_jobs()),
    )
    return sched


def get_scheduler_status() -> list[dict[str, Any]]:
    """Return per-job status for the embedded scheduler.

    Each row: id, name, trigger, next_run (aware UTC), last_run, last_ok, last_error.
    Empty list if the scheduler isn't running.
    """
    s = _bg_scheduler
    if s is None or not s.running:
        return []
    out: list[dict[str, Any]] = []
    for j in s.get_jobs():
        lr = _last_runs.get(j.id) or {}
        out.append(
            {
                "id": j.id,
                "name": j.name,
                "trigger": str(j.trigger),
                "next_run": j.next_run_time,
                "last_run": lr.get("ts"),
                "last_ok": lr.get("ok"),
                "last_error": lr.get("error"),
            }
        )
    return out


def main() -> None:
    """Blocking standalone mode: python -m finance_alpa.scheduler"""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    init_db()
    sched = BlockingScheduler(timezone=TIMEZONE)
    _add_jobs(sched)

    logger.info("Scheduler starting (Ctrl-C to stop). Jobs:")
    for j in sched.get_jobs():
        logger.info("  %-20s %s", j.id, j.trigger)

    logger.info("Running startup pass: sa_rss → yfinance → alerts_eval")
    _startup_pass()

    try:
        sched.start()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Scheduler stopped.")


if __name__ == "__main__":
    main()
