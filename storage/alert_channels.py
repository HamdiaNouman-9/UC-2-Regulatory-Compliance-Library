"""CHG-009 step 10: where an alert goes once it is on the run row.

The run store (`storage/run_store.py`) works out a run's alerts and stores
them with the run before anything here is called, so the alerts feed
(`GET /alerts`) shows every alert with nothing configured. This module is
only the delivery: one channel, chosen by `ALERT_CHANNEL`, and the message
text built once so every channel says the same words.

    ALERT_CHANNEL   none      store only (the default)
                    webhook   POST {run_id, job, alerts} to ALERT_WEBHOOK_URL,
                              the shape that existed before this module
                    email     SMTP, the organisation's choice (2026-11-16)
                    teams     placeholder: logged, not sent
                    slack     placeholder: logged, not sent

Email settings (all required for `email`):
    SMTP_HOST, SMTP_PORT (default 587, STARTTLS; 465 for SSL),
    SMTP_USER and SMTP_PASSWORD (both optional: omit for an unauthenticated
    relay), ALERT_EMAIL_FROM, ALERT_EMAIL_TO (comma-separated).
    ALERT_EMAIL_LEVEL (default `critical`): the lowest level that is mailed,
    `warning` to mail those too.

Webhook and email never raise: a delivery failure is logged and the run
record stands. The webhook sends warnings and criticals as before; email
sends criticals only unless ALERT_EMAIL_LEVEL says otherwise, because the
escalation this was built for (a site blocked in two consecutive cycles,
`site_blocked_repeatedly`) is critical and a weekly warning digest was not
asked for.
"""
from __future__ import annotations

import logging
import os
import smtplib
from email.message import EmailMessage
from typing import List

logger = logging.getLogger(__name__)

CHANNELS = ("none", "webhook", "email", "teams", "slack")
_ORDER = {"info": 1, "warning": 2, "critical": 3}


def channel() -> str:
    name = os.getenv("ALERT_CHANNEL", "none").strip().lower() or "none"
    if name not in CHANNELS:
        logger.warning("ALERT_CHANNEL=%r is not one of %s; alerts are stored only", name, CHANNELS)
        return "none"
    return name


def worth_sending(alerts: List[dict], lowest: str = "warning") -> List[dict]:
    floor = _ORDER.get(lowest, 2)
    return [a for a in alerts if _ORDER.get(a.get("level"), 0) >= floor]


def subject(job: str, alerts: List[dict]) -> str:
    top = max(alerts, key=lambda a: _ORDER.get(a.get("level"), 0))
    return f"UC-02 {job}: {str(top.get('level', '')).upper()} {top.get('code', '')}"


def body(run_id: int, job: str, alerts: List[dict]) -> str:
    lines = [f"UC-02 monitor job {job}, run {run_id}", ""]
    for a in alerts:
        lines.append(f"[{str(a.get('level', '')).upper()}] {a.get('code', '')}: {a.get('message', '')}")
    lines += ["", f"Run record: GET /runs/{run_id}; all alerts: GET /alerts"]
    return "\n".join(lines)


def send(run_id: int, job: str, alerts: List[dict]) -> str:
    """Deliver through the configured channel. Returns the channel used, or
    'none' when nothing was sent (no channel, nothing worth sending, or the
    channel is a placeholder)."""
    name = channel()
    if name == "none":
        return "none"
    if name == "webhook":
        return _webhook(run_id, job, worth_sending(alerts, "warning"))
    if name == "email":
        return _email(run_id, job, worth_sending(alerts, _email_level()))
    # teams / slack: the organisation has not chosen them; the hook is here so
    # choosing one later is a formatter, not a redesign.
    worth = worth_sending(alerts, "warning")
    if worth:
        logger.warning("ALERT_CHANNEL=%s is a placeholder; %d alert(s) for %s run %s stored only",
                       name, len(worth), job, run_id)
    return "none"


def _email_level() -> str:
    level = os.getenv("ALERT_EMAIL_LEVEL", "critical").strip().lower() or "critical"
    if level not in _ORDER:
        logger.warning("ALERT_EMAIL_LEVEL=%r is not one of %s; mailing criticals only",
                       level, tuple(_ORDER))
        return "critical"
    return level


def _webhook(run_id: int, job: str, worth: List[dict]) -> str:
    url = os.getenv("ALERT_WEBHOOK_URL", "").strip()
    if not url or not worth:
        return "none"
    try:
        import requests
        requests.post(url, json={"run_id": run_id, "job": job, "alerts": worth}, timeout=5)
        return "webhook"
    except Exception as e:                         # an alert must never break the run record
        logger.warning("alert webhook failed: %s", e)
        return "none"


def _email(run_id: int, job: str, worth: List[dict]) -> str:
    if not worth:
        return "none"
    host = os.getenv("SMTP_HOST", "").strip()
    sender = os.getenv("ALERT_EMAIL_FROM", "").strip()
    to = [x.strip() for x in os.getenv("ALERT_EMAIL_TO", "").split(",") if x.strip()]
    if not (host and sender and to):
        logger.warning("ALERT_CHANNEL=email but SMTP_HOST, ALERT_EMAIL_FROM or ALERT_EMAIL_TO is unset; "
                       "%d alert(s) for %s run %s stored only", len(worth), job, run_id)
        return "none"
    try:
        port = int(os.getenv("SMTP_PORT", "587").strip() or 587)
    except ValueError:
        logger.warning("SMTP_PORT=%r is not a number; %d alert(s) for %s run %s stored only",
                       os.getenv("SMTP_PORT"), len(worth), job, run_id)
        return "none"
    user, password = os.getenv("SMTP_USER", ""), os.getenv("SMTP_PASSWORD", "")
    msg = EmailMessage()
    msg["Subject"] = subject(job, worth)
    msg["From"], msg["To"] = sender, ", ".join(to)
    msg.set_content(body(run_id, job, worth))
    try:
        if port == 465:
            with smtplib.SMTP_SSL(host, port, timeout=10) as smtp:
                if user:
                    smtp.login(user, password)
                smtp.send_message(msg)
        else:
            with smtplib.SMTP(host, port, timeout=10) as smtp:
                smtp.starttls()
                if user:
                    smtp.login(user, password)
                smtp.send_message(msg)
        return "email"
    except Exception as e:                         # an alert must never break the run record
        logger.warning("alert email failed: %s", e)
        return "none"
