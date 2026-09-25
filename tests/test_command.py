from __future__ import annotations

import signal
from typing import Any
from unittest import mock

from django.core.management import call_command
from mypy_boto3_sqs import SQSClient

from django_tasks_sqs.worker import Worker


def test_command_builds_worker_and_handles_signals(sqs: SQSClient) -> None:
    captured: dict[str, Any] = {}

    def fake_run(self: Worker) -> None:
        captured["worker"] = self
        # Simulate SIGTERM arriving while running.
        handler = signal.getsignal(signal.SIGTERM)
        assert callable(handler)
        handler(signal.SIGTERM, None)

    with mock.patch.object(Worker, "run", fake_run):
        call_command(
            "sqs_worker",
            "--queue",
            "emails",
            "--concurrency",
            "3",
            "--wait-time",
            "5",
            "--max-messages",
            "2",
            "--visibility-timeout",
            "60",
            "--retry-backoff",
            "15",
            "--no-heartbeat",
        )

    worker = captured["worker"]
    assert worker.queue_names == ["emails"]
    assert worker.concurrency == 3
    assert worker.options.wait_time_seconds == 5
    assert worker.options.max_messages == 2
    assert worker.options.visibility_timeout == 60
    assert worker.options.retry_backoff == 15
    assert worker.options.heartbeat is False
    assert worker.stopping
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGINT, signal.default_int_handler)
