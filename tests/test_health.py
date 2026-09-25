from __future__ import annotations

import threading
import urllib.error
import urllib.request
from typing import Any
from unittest import mock

import pytest
from django.core.management import call_command
from mypy_boto3_sqs import SQSClient

from django_tasks_sqs import Worker, WorkerOptions
from django_tasks_sqs.health import HealthServer, WorkerStats, render_metrics
from django_tasks_sqs.signals import message_processed
from django_tasks_sqs.worker import Outcome
from tests import tasks

NO_WAIT = WorkerOptions(wait_time_seconds=0, heartbeat=False)


@pytest.fixture
def now(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    """A fake clock for the health module: move it with ``now[0] += seconds``."""
    clock = [1000.0]
    monkeypatch.setattr("django_tasks_sqs.health.monotonic", lambda: clock[0])
    return clock


# ------------------------------------------------------------------ stats


def test_worker_counts_outcomes(sqs: SQSClient) -> None:
    tasks.add.enqueue(1, 1)
    tasks.explode.enqueue()
    w = Worker(options=NO_WAIT)
    w.run_once("default")
    w.run_once("default")
    messages = w.stats.messages
    assert messages["default", Outcome.SUCCEEDED] == 1
    assert messages["default", Outcome.FAILED] == 1
    assert messages["emails", Outcome.SUCCEEDED] == 0  # every series starts at zero
    assert w.stats.busy_threads == 0


def test_message_processed_signal(sqs: SQSClient) -> None:
    received: list[dict[str, Any]] = []

    def handler(sender: Any, **kwargs: Any) -> None:
        received.append({"sender": sender, **kwargs})

    tasks.add.enqueue(1, 1)
    w = Worker(options=NO_WAIT)
    message_processed.connect(handler)
    try:
        w.run_once("default")
    finally:
        message_processed.disconnect(handler)
    [event] = received
    assert event["sender"] is Worker
    assert event["worker"] is w
    assert event["queue_name"] == "default"
    assert event["outcome"] == Outcome.SUCCEEDED
    assert event["duration"] >= 0


def test_threads_are_busy_while_running_tasks(sqs: SQSClient) -> None:
    tasks.add.enqueue(1, 1)
    w = Worker(options=NO_WAIT)
    seen: list[int] = []
    execute = w._execute

    def spy(*args: Any) -> Any:
        seen.append(w.stats.busy_threads)
        return execute(*args)

    with mock.patch.object(w, "_execute", spy):
        w.run_once("default")
    assert seen == [1]
    assert w.stats.busy_threads == 0


def test_poll_errors_are_counted(sqs: SQSClient) -> None:
    w = Worker(options=NO_WAIT)
    calls = 0

    def flaky(queue_name: str) -> list[str]:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("network down")
        w.stop()
        return []

    with mock.patch.object(w, "run_once", flaky), mock.patch.object(w._stop, "wait"):
        w._poll_forever("default")
    assert w.stats.poll_errors == {"default": 1, "emails": 0, "orders.fifo": 0}


def test_liveness_ignores_busy_threads(now: list[float]) -> None:
    stats = WorkerStats(["default"])
    assert stats.last_poll_age() is None
    assert stats.is_healthy(max_age=10)

    stats.register_thread()
    now[0] += 30
    assert stats.last_poll_age() == 30
    assert not stats.is_healthy(max_age=10)

    with stats.busy():  # a long task is not a sign of trouble
        assert stats.last_poll_age() is None
        assert stats.is_healthy(max_age=10)

    stats.polled()
    assert stats.is_healthy(max_age=10)


def test_liveness_tracks_each_thread(now: list[float]) -> None:
    stats = WorkerStats()
    stats.polled()
    thread = threading.Thread(target=stats.polled)
    now[0] += 5
    thread.start()
    thread.join()
    assert stats.last_poll_age() == 5  # the stalest thread counts


# ---------------------------------------------------------------- metrics


def test_render_metrics(now: list[float]) -> None:
    stats = WorkerStats(['we"ird'])
    stats.processed('we"ird', Outcome.SUCCEEDED)
    stats.poll_failed('we"ird')
    stats.polled()
    now[0] += 2.5
    text = render_metrics(stats, max_age=60)
    assert 'django_tasks_sqs_messages_total{queue="we\\"ird",outcome="succeeded"} 1\n' in text
    assert 'django_tasks_sqs_messages_total{queue="we\\"ird",outcome="failed"} 0\n' in text
    assert 'django_tasks_sqs_poll_errors_total{queue="we\\"ird"} 1\n' in text
    assert "django_tasks_sqs_busy_threads 0\n" in text
    assert "django_tasks_sqs_last_poll_age_seconds 2.5\n" in text
    assert "django_tasks_sqs_healthy 1\n" in text
    assert "# TYPE django_tasks_sqs_messages_total counter\n" in text


def test_render_metrics_before_any_poll() -> None:
    text = render_metrics(WorkerStats(), max_age=60)
    assert "django_tasks_sqs_last_poll_age_seconds 0.0\n" in text


# ----------------------------------------------------------------- server


def get(port: int, path: str) -> tuple[int, str, str]:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}{path}") as response:
            return response.status, response.headers["Content-Type"], response.read().decode()
    except urllib.error.HTTPError as exc:
        with exc:  # close the response, or Python 3.14 warns about the leak
            return exc.code, exc.headers["Content-Type"], exc.read().decode()


def test_health_server(sqs: SQSClient) -> None:
    w = Worker(options=NO_WAIT)
    server = HealthServer(w, host="127.0.0.1", port=0, max_age=10)
    server.start()
    try:
        assert get(server.port, "/healthz") == (200, "text/plain; charset=utf-8", "ok\n")
        status, content_type, body = get(server.port, "/metrics")
        assert status == 200
        assert content_type == "text/plain; version=0.0.4; charset=utf-8"
        assert "django_tasks_sqs_healthy 1" in body
        assert get(server.port, "/nope")[0] == 404

        with mock.patch.object(w.stats, "is_healthy", return_value=False):
            assert get(server.port, "/healthz")[::2] == (503, "unhealthy\n")
    finally:
        server.stop()


def test_command_serves_health_while_running(sqs: SQSClient) -> None:
    seen: list[tuple[int, str, str]] = []
    server: list[HealthServer] = []
    original_init = HealthServer.__init__

    def spy_init(self: HealthServer, *args: Any, **kwargs: Any) -> None:
        original_init(self, *args, **kwargs)
        server.append(self)

    def fake_run(self: Worker) -> None:
        seen.append(get(server[0].port, "/healthz"))

    with (
        mock.patch.object(HealthServer, "__init__", spy_init),
        mock.patch.object(Worker, "run", fake_run),
    ):
        call_command("sqs_worker", "--health-port", "0", "--health-host", "127.0.0.1")
    assert seen == [(200, "text/plain; charset=utf-8", "ok\n")]
    with pytest.raises(urllib.error.URLError, match="refused"):
        get(server[0].port, "/healthz")  # stopped with the worker
