"""Alert notifications: desktop (plyer) + optional SMTP email."""
from __future__ import annotations

import logging
import smtplib
from email.mime.text import MIMEText

from ..config import get_settings

logger = logging.getLogger(__name__)


def _desktop(title: str, body: str) -> None:
    try:
        from plyer import notification
        notification.notify(
            title=title,
            message=body,
            app_name="finance_alpa",
            timeout=10,
        )
    except Exception:
        logger.exception("Desktop notification failed")


def _email(title: str, body: str) -> None:
    s = get_settings()
    if not s.smtp_host or not s.smtp_from:
        logger.warning("SMTP not configured in .env — skipping email notification")
        return
    try:
        msg = MIMEText(body)
        msg["Subject"] = f"[finance_alpa] {title}"
        msg["From"] = s.smtp_from
        msg["To"] = s.smtp_from
        with smtplib.SMTP(s.smtp_host, s.smtp_port, timeout=15) as smtp:
            smtp.starttls()
            if s.smtp_user:
                smtp.login(s.smtp_user, s.smtp_password)
            smtp.send_message(msg)
    except Exception:
        logger.exception("Email notification failed")


def notify(title: str, body: str, channels: list[str]) -> None:
    for ch in channels:
        ch = ch.lower().strip()
        if ch == "desktop":
            _desktop(title, body)
        elif ch == "email":
            _email(title, body)
        else:
            logger.warning("Unknown notification channel: %s", ch)
