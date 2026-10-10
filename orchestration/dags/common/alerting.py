"""Failure alerts for the DAGs.

When a task has used up its retries, `notify_failure` runs. It always writes
the alert to the task log and to an alert file; if a webhook URL is
configured (Slack, Mattermost, Teams and Google Chat all accept this
format), it also posts the message there.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

WEBHOOK_URL_VARIABLE = "ALERT_WEBHOOK_URL"
ALERT_LOG_VARIABLE = "ALERT_LOG"
DEFAULT_ALERT_LOG = "data/alerts/alerts.jsonl"


def build_alert(context: dict) -> dict:
    """The facts someone on call needs: what failed, when, how often it was tried, why."""
    ti = context["ti"]
    dag_run = context.get("dag_run")
    exception = context.get("exception")
    return {
        "raised_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "run_id": getattr(dag_run, "run_id", None) or getattr(ti, "run_id", None),
        "attempts": ti.try_number,
        "error": str(exception)[:500] if exception else None,
        "log_url": getattr(ti, "log_url", None),
    }


def format_message(alert: dict) -> str:
    lines = [
        f":red_circle: {alert['dag_id']}.{alert['task_id']} failed "
        f"after {alert['attempts']} attempt(s)",
        f"Run: {alert['run_id']}",
    ]
    if alert["error"]:
        lines.append(f"Error: {alert['error']}")
    if alert["log_url"]:
        lines.append(f"Log: {alert['log_url']}")
    return "\n".join(lines)


def notify_failure(context: dict) -> None:
    alert = build_alert(context)
    message = format_message(alert)
    log.error("ALERT\n%s", message)

    # An alert that cannot be delivered must not hide the original failure:
    # each channel is tried on its own and only logs if it fails.
    try:
        path = Path(os.environ.get(ALERT_LOG_VARIABLE, DEFAULT_ALERT_LOG))
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as handle:
            handle.write(json.dumps(alert, ensure_ascii=False) + "\n")
    except OSError as error:
        log.warning("could not write the alert to the alert log: %s", error)

    url = os.environ.get(WEBHOOK_URL_VARIABLE)
    if not url:
        return
    try:
        request = urllib.request.Request(
            url,
            data=json.dumps({"text": message}).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=10) as response:  # noqa: S310
            log.info("alert posted to the webhook (HTTP %s)", response.status)
    except Exception as error:  # noqa: BLE001
        log.warning("could not post the alert to the webhook: %s", error)
