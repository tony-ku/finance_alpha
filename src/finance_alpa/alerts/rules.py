"""Evaluate user-defined alert rules against the database."""
from __future__ import annotations

import json
import logging
from datetime import date, timedelta

from ..config import AlertRule, load_app_config
from ..db import connect
from .notify import notify

logger = logging.getLogger(__name__)


# --- Dedup + fire -------------------------------------------------------

def _already_fired(con, rule_name: str, dedup_key: str) -> bool:
    row = con.execute(
        """
        SELECT 1 FROM alerts_log
        WHERE rule_name = ?
          AND json_extract_string(payload, '$.dedup_key') = ?
        LIMIT 1
        """,
        [rule_name, dedup_key],
    ).fetchone()
    return row is not None


def _fire(
    con,
    rule: AlertRule,
    symbol: str,
    title: str,
    body: str,
    dedup_key: str,
    extra: dict | None = None,
) -> bool:
    if _already_fired(con, rule.name, dedup_key):
        return False
    payload: dict = {"title": title, "body": body, "dedup_key": dedup_key}
    if extra:
        payload.update(extra)
    con.execute(
        "INSERT INTO alerts_log (rule_name, symbol, payload) VALUES (?, ?, ?)",
        [rule.name, symbol, json.dumps(payload, default=str)],
    )
    notify(title, body, rule.notify)
    logger.info("Fired: %s (%s) — %s", rule.name, symbol, title)
    return True


def _scope_symbols(scope: str) -> set[str]:
    cfg = load_app_config()
    if scope == "portfolio":
        return {p.symbol for p in cfg.portfolio}
    if scope == "watchlist":
        return set(cfg.universe.watchlist)
    return set(cfg.all_symbols())


# --- Rule handlers ------------------------------------------------------

def _eval_upcoming_earnings(con, rule: AlertRule) -> int:
    days = rule.days or 5
    symbols = _scope_symbols(rule.scope)
    if not symbols:
        return 0
    end_date = (date.today() + timedelta(days=days)).isoformat()
    rows = con.execute(
        """
        SELECT symbol, report_date
        FROM earnings_calendar
        WHERE report_date BETWEEN CURRENT_DATE AND ?
        """,
        [end_date],
    ).fetchall()
    fired = 0
    for sym, rpt in rows:
        if sym not in symbols:
            continue
        delta = (rpt - date.today()).days
        dedup = f"{sym}:earnings:{rpt.isoformat()}"
        title = f"Earnings in {delta}d: {sym}"
        body = f"{sym} reports earnings on {rpt.isoformat()}."
        if _fire(con, rule, sym, title, body, dedup, {"report_date": rpt.isoformat()}):
            fired += 1
    return fired


def _eval_fmp_rating_change(con, rule: AlertRule) -> int:
    symbols = _scope_symbols(rule.scope)
    if not symbols:
        return 0
    fired = 0
    for sym in symbols:
        rows = con.execute(
            """
            SELECT as_of, rating, score
            FROM ratings
            WHERE symbol = ? AND source = 'fmp_rating'
            ORDER BY as_of DESC LIMIT 2
            """,
            [sym],
        ).fetchall()
        if len(rows) < 2:
            continue
        (new_as_of, new_rating, new_score), (_, old_rating, _) = rows[0], rows[1]
        if new_rating == old_rating:
            continue
        dedup = f"{sym}:fmp:{new_as_of}"
        title = f"FMP rating change: {sym} {old_rating} → {new_rating}"
        body = f"{sym} FMP rating changed to {new_rating} (score {new_score}) on {new_as_of}."
        if _fire(con, rule, sym, title, body, dedup, {"old": old_rating, "new": new_rating}):
            fired += 1
    return fired


def _eval_reco_change(con, rule: AlertRule) -> int:
    symbols = _scope_symbols(rule.scope)
    if not symbols:
        return 0
    direction = (rule.direction or "any").lower()
    delta_threshold = 0.3  # meaningful move on the 1–5 reco scale
    fired = 0
    for sym in symbols:
        rows = con.execute(
            """
            SELECT as_of, score, rating
            FROM ratings
            WHERE symbol = ? AND source = 'finnhub_reco'
            ORDER BY as_of DESC LIMIT 2
            """,
            [sym],
        ).fetchall()
        if len(rows) < 2:
            continue
        (new_as_of, new_score, new_label), (_, old_score, old_label) = rows[0], rows[1]
        if new_score is None or old_score is None:
            continue
        delta = float(new_score) - float(old_score)  # reco scale: lower = better
        if direction == "up" and delta >= -delta_threshold:
            continue
        if direction == "down" and delta <= delta_threshold:
            continue
        if direction == "any" and abs(delta) < delta_threshold:
            continue
        dedup = f"{sym}:reco:{new_as_of}"
        arrow = "↑" if delta < 0 else "↓"
        title = f"Reco {arrow} {sym}: {old_label} → {new_label}"
        body = f"{sym} Finnhub reco mean {old_score:.2f} → {new_score:.2f} ({new_label})."
        if _fire(con, rule, sym, title, body, dedup, {"old": old_score, "new": new_score}):
            fired += 1
    return fired


def _eval_price_change(con, rule: AlertRule) -> int:
    symbols = _scope_symbols(rule.scope)
    if not symbols:
        return 0
    threshold = abs(rule.threshold_pct or 3.0)
    direction = (rule.direction or "any").lower()
    fired = 0
    for sym in symbols:
        rows = con.execute(
            """
            SELECT date, close
            FROM quotes_daily
            WHERE symbol = ?
            ORDER BY date DESC LIMIT 2
            """,
            [sym],
        ).fetchall()
        if len(rows) < 2:
            continue
        (d_new, c_new), (_, c_prev) = rows[0], rows[1]
        if not c_prev:
            continue
        pct = (float(c_new) - float(c_prev)) / float(c_prev) * 100
        if abs(pct) < threshold:
            continue
        if direction == "up" and pct < 0:
            continue
        if direction == "down" and pct > 0:
            continue
        dedup = f"{sym}:move:{d_new.isoformat()}"
        arrow = "↑" if pct > 0 else "↓"
        title = f"{sym} {arrow} {pct:+.2f}%"
        body = f"{sym} closed at ${float(c_new):.2f} on {d_new.isoformat()} ({pct:+.2f}%)."
        if _fire(con, rule, sym, title, body, dedup, {"close": float(c_new), "pct": pct}):
            fired += 1
    return fired


HANDLERS = {
    "upcoming_earnings": _eval_upcoming_earnings,
    "fmp_rating_change": _eval_fmp_rating_change,
    "reco_change": _eval_reco_change,
    "price_change": _eval_price_change,
}


def evaluate_all() -> int:
    cfg = load_app_config()
    if not cfg.alerts:
        logger.info("No alert rules configured.")
        return 0
    total = 0
    with connect() as con:
        for rule in cfg.alerts:
            handler = HANDLERS.get(rule.type)
            if handler is None:
                logger.warning("Unknown rule type '%s' on rule '%s'", rule.type, rule.name)
                continue
            try:
                total += handler(con, rule)
            except Exception:
                logger.exception("Rule evaluation failed: %s", rule.name)
    return total


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    n = evaluate_all()
    logger.info("Alerts evaluator: fired %d alerts", n)


if __name__ == "__main__":
    main()
