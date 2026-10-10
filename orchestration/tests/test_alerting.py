from __future__ import annotations

import http.server
import json
import threading
from types import SimpleNamespace

import pytest

from common import alerting


def _context(error: Exception | None = RuntimeError("API answered 503 six times")) -> dict:
    ti = SimpleNamespace(
        dag_id="ecommerce_daily", task_id="bronze_api", try_number=3, run_id="scheduled__x",
        log_url="http://localhost:8080/dags/ecommerce_daily/runs/x/tasks/bronze_api",
    )
    return {"ti": ti, "dag_run": SimpleNamespace(run_id="scheduled__2026-10-09"), "exception": error}


@pytest.fixture()
def webhook():
    """A local HTTP server standing in for Slack: records what is posted to it."""
    received: list[dict] = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self):  # noqa: N802
            body = self.rfile.read(int(self.headers["Content-Length"]))
            received.append({"type": self.headers["Content-Type"], "body": json.loads(body)})
            self.send_response(200)
            self.end_headers()

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_port}/hook", received
    server.shutdown()


def test_alert_says_what_failed_how_often_and_why():
    alert = alerting.build_alert(_context())
    assert (alert["dag_id"], alert["task_id"], alert["attempts"]) == (
        "ecommerce_daily", "bronze_api", 3)
    assert alert["run_id"] == "scheduled__2026-10-09"
    assert alert["error"] == "API answered 503 six times"

    message = alerting.format_message(alert)
    assert "ecommerce_daily.bronze_api failed after 3 attempt(s)" in message
    assert "API answered 503 six times" in message
    assert "http://localhost:8080/dags/ecommerce_daily" in message


def test_a_long_error_is_shortened():
    alert = alerting.build_alert(_context(RuntimeError("x" * 5000)))
    assert len(alert["error"]) == 500


def test_alert_is_written_to_the_alert_log(tmp_path, monkeypatch):
    log_file = tmp_path / "alerts" / "alerts.jsonl"
    monkeypatch.setenv("ALERT_LOG", str(log_file))
    monkeypatch.delenv("ALERT_WEBHOOK_URL", raising=False)

    alerting.notify_failure(_context())
    alerting.notify_failure(_context())

    lines = [json.loads(line) for line in log_file.read_text(encoding="utf-8").splitlines()]
    assert len(lines) == 2                                   # appended, one per alert
    assert lines[0]["task_id"] == "bronze_api" and lines[0]["attempts"] == 3


def test_alert_is_posted_to_the_webhook_when_one_is_configured(tmp_path, monkeypatch, webhook):
    url, received = webhook
    monkeypatch.setenv("ALERT_LOG", str(tmp_path / "alerts.jsonl"))
    monkeypatch.setenv("ALERT_WEBHOOK_URL", url)

    alerting.notify_failure(_context())

    (post,) = received
    assert post["type"] == "application/json"
    assert "ecommerce_daily.bronze_api failed" in post["body"]["text"]


def test_an_unreachable_webhook_does_not_raise(tmp_path, monkeypatch):
    # Nothing listens on this port. The alert must not turn into a second failure.
    monkeypatch.setenv("ALERT_LOG", str(tmp_path / "alerts.jsonl"))
    monkeypatch.setenv("ALERT_WEBHOOK_URL", "http://127.0.0.1:9/hook")
    alerting.notify_failure(_context())
    assert (tmp_path / "alerts.jsonl").exists()              # the log entry is still written
