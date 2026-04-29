"""
Notification helpers for dremio-load — ported from Transform Studio's alert_runner.py.

Supports email (SMTP) and Slack (incoming webhook).  All calls are synchronous
since the load engine runs in plain threads, not an async loop.
"""
from __future__ import annotations

import logging
import smtplib
import threading
from email.mime.text import MIMEText
from typing import Optional

logger = logging.getLogger(__name__)


def send_notification(job_name: str, status: str, message: str, settings: dict) -> None:
    """
    Fire email and/or Slack notifications in a background thread so the engine
    thread is never blocked by SMTP/HTTP latency.
    """
    t = threading.Thread(
        target=_send_sync,
        args=(job_name, status, message, settings),
        daemon=True,
    )
    t.start()


def _send_sync(job_name: str, status: str, message: str, settings: dict) -> None:
    icon = ":white_check_mark:" if status == "ok" else ":x:"
    subject = f"Dremio Load — job '{job_name}' {status}"
    body = f"Job '{job_name}' finished with status: {status}\n\n{message}"

    _send_email(subject, body, settings)
    _send_slack(icon, subject, message, settings)


def _send_email(subject: str, body: str, settings: dict) -> None:
    if not (settings.get("notify_email_enabled") and settings.get("notify_email_smtp_host")):
        return
    try:
        smtp_host = settings["notify_email_smtp_host"]
        smtp_port = int(settings.get("notify_email_smtp_port") or 587)
        smtp_user = settings.get("notify_email_smtp_user", "")
        smtp_pass = settings.get("notify_email_smtp_pass", "")
        from_addr = settings.get("notify_email_from", smtp_user)
        to_addr   = settings.get("notify_email_to", "")
        if not to_addr:
            return
        msg = MIMEText(body)
        msg["Subject"] = subject
        msg["From"]    = from_addr
        msg["To"]      = to_addr
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            if smtp_user and smtp_pass:
                server.login(smtp_user, smtp_pass)
            server.sendmail(from_addr, [to_addr], msg.as_string())
        logger.info("[notifier] Email sent to %s", to_addr)
    except Exception as exc:
        logger.error("[notifier] Email failed: %s", exc)


def _send_slack(icon: str, subject: str, message: str, settings: dict) -> None:
    if not (settings.get("notify_slack_enabled") and settings.get("notify_slack_webhook_url")):
        return
    try:
        import urllib.request, json as _json
        payload = _json.dumps({"text": f"{icon} *{subject}*\n{message}"}).encode()
        req = urllib.request.Request(
            settings["notify_slack_webhook_url"],
            data=payload,
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=10)
        logger.info("[notifier] Slack notification sent")
    except Exception as exc:
        logger.error("[notifier] Slack failed: %s", exc)


def fire_webhook(url: str, payload: dict) -> None:
    """POST a JSON payload to a URL (used for on_success_url / on_failure_url)."""
    def _post():
        try:
            import urllib.request, json as _json
            data = _json.dumps(payload).encode()
            req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
            urllib.request.urlopen(req, timeout=15)
            logger.info("[notifier] Webhook fired: %s", url)
        except Exception as exc:
            logger.warning("[notifier] Webhook %s failed: %s", url, exc)
    threading.Thread(target=_post, daemon=True).start()
