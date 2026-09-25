from __future__ import annotations

import json
import threading
import time
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest import mock

import pytest
from django.tasks import TaskResultStatus
from django.tasks.signals import task_finished, task_started
from django.utils import timezone
from mypy_boto3_sqs import SQSClient

from django_tasks_sqs import Worker, WorkerOptions
from django_tasks_sqs.message import TaskMessage
from django_tasks_sqs.worker import MAX_VISIBILITY_TIMEOUT, Outcome, backoff_seconds
from tests import tasks
from tests.conftest import sqs_backend

NO_WAIT = WorkerOptions(wait_time_seconds=0, heartbeat=False)


def worker(**kwargs: Any) -> Worker:
    kwargs.setdefault("options", NO_WAIT)
    return Worker(**kwargs)


def pending(sqs: SQSClient, queue: str = "test-default") -> int:
    url = sqs.get_queue_url(QueueName=queue)["QueueUrl"]
    attrs = sqs.get_queue_attributes(
        QueueUrl=url,
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed",
        ],
    )["Attributes"]
    return sum(int(value) for value in attrs.values())


def send_raw(sqs: SQSClient, body: str, queue: str = "test-default") -> None:
    url = sqs.get_queue_url(QueueName=queue)["QueueUrl"]
    sqs.send_message(QueueUrl=url, MessageBody=body)


def raw_message(**overrides: Any) -> str:
    message = TaskMessage(
        id="raw",
        task_path=overrides.pop("task_path", "tests.tasks.add"),
        args=[1, 1],
        kwargs={},
        queue_name=overrides.pop("queue_name", "default"),
        backend=overrides.pop("backend", "default"),
        enqueued_at=timezone.now(),
        run_after=overrides.pop("run_after", None),
    )
    return message.to_json()


# ------------------------------------------------------------------- success


def test_success_runs_task_and_deletes_message(sqs: SQSClient) -> None:
    tasks.add.enqueue(2, 3)
    assert worker().run_once("default") == [Outcome.SUCCEEDED]
    assert tasks.calls == [("add", (2, 3))]
    assert pending(sqs) == 0


def test_kwargs_and_other_queue(sqs: SQSClient) -> None:
    tasks.send_email.enqueue("a@b.c", subject="hi")
    assert worker(queue_names=["emails"]).run_once("emails") == [Outcome.SUCCEEDED]
    assert tasks.calls == [("send_email", ("a@b.c", "hi"))]


def test_async_task(sqs: SQSClient) -> None:
    tasks.async_add.enqueue(4, 5)
    assert worker().run_once("default") == [Outcome.SUCCEEDED]
    assert tasks.calls == [("async_add", (4, 5))]


def test_fifo_queue(sqs: SQSClient) -> None:
    tasks.place_order.enqueue(7)
    assert worker(queue_names=["orders.fifo"]).run_once("orders.fifo") == [Outcome.SUCCEEDED]
    assert tasks.calls == [("place_order", 7)]


def test_signals_describe_the_run(sqs: SQSClient) -> None:
    seen: list[tuple[str, Any]] = []

    def on_started(sender: Any, task_result: Any, **kwargs: Any) -> None:
        seen.append(("started", task_result.status))

    def on_finished(sender: Any, task_result: Any, **kwargs: Any) -> None:
        seen.append(("finished", task_result.status))
        seen.append(("value", task_result.return_value))

    enqueued = tasks.add.enqueue(2, 2)
    task_started.connect(on_started)
    task_finished.connect(on_finished)
    try:
        worker().run_once("default")
    finally:
        task_started.disconnect(on_started)
        task_finished.disconnect(on_finished)
    assert seen == [
        ("started", TaskResultStatus.RUNNING),
        ("finished", TaskResultStatus.SUCCESSFUL),
        ("value", 4),
    ]
    assert enqueued.id  # same id travels in the message


# ------------------------------------------------------------------- failure


def test_failure_keeps_message_for_retry(sqs: SQSClient) -> None:
    errors: list[Any] = []

    def on_finished(sender: Any, task_result: Any, **kwargs: Any) -> None:
        errors.extend(task_result.errors)

    tasks.explode.enqueue()
    task_finished.connect(on_finished)
    try:
        assert worker().run_once("default") == [Outcome.FAILED]
    finally:
        task_finished.disconnect(on_finished)
    assert pending(sqs) == 1
    [error] = errors
    assert error.exception_class is RuntimeError
    assert "boom" in error.traceback


def test_attempts_come_from_receive_count(sqs: SQSClient) -> None:
    tasks.with_context.enqueue()
    # retry_backoff=0 makes a failed message visible again immediately.
    w = worker(options=WorkerOptions(wait_time_seconds=0, heartbeat=False, retry_backoff=0))
    with mock.patch.object(type(tasks.with_context), "call", side_effect=RuntimeError):
        assert w.run_once("default") == [Outcome.FAILED]
    assert w.run_once("default") == [Outcome.SUCCEEDED]
    assert tasks.calls == [("with_context", 2)]


def test_retry_backoff_sets_visibility(sqs: SQSClient) -> None:
    tasks.explode.enqueue()
    w = worker(options=WorkerOptions(wait_time_seconds=0, heartbeat=False, retry_backoff=10))
    client = sqs_backend().client
    with mock.patch.object(
        client, "change_message_visibility", wraps=client.change_message_visibility
    ) as spy:
        w.run_once("default")
    assert spy.call_args.kwargs["VisibilityTimeout"] == 10


@pytest.mark.parametrize(
    ("attempt", "expected"), [(1, 10), (2, 20), (4, 80), (30, MAX_VISIBILITY_TIMEOUT)]
)
def test_backoff_seconds(attempt: int, expected: int) -> None:
    assert backoff_seconds(10, attempt) == expected


# ------------------------------------------------------------ bad messages


@pytest.mark.parametrize(
    "body",
    [
        "garbage",
        raw_message(task_path="tests.tasks.does_not_exist"),
        raw_message(task_path="nonexistent_module.task"),
        raw_message(task_path="tests.tasks.NOT_A_TASK"),
        raw_message(queue_name="not-configured"),
        raw_message(backend="no-such-backend"),
    ],
)
def test_invalid_messages_are_left_for_the_dlq(sqs: SQSClient, body: str) -> None:
    send_raw(sqs, body)
    assert worker().run_once("default") == [Outcome.INVALID]
    assert pending(sqs) == 1


def test_keyboard_interrupt_propagates_and_keeps_message(sqs: SQSClient) -> None:
    tasks.add.enqueue(1, 2)
    with (
        mock.patch.object(type(tasks.add), "call", side_effect=KeyboardInterrupt),
        pytest.raises(KeyboardInterrupt),
    ):
        worker().run_once("default")
    assert pending(sqs) == 1


# ------------------------------------------------------------------- defer


def test_message_not_yet_due_is_rescheduled(sqs: SQSClient) -> None:
    send_raw(sqs, raw_message(run_after=timezone.now() + timedelta(hours=2)))
    backend = sqs_backend()
    with mock.patch.object(
        backend.client, "send_message", wraps=backend.client.send_message
    ) as spy:
        assert worker().run_once("default") == [Outcome.DEFERRED]
    assert spy.call_args.kwargs["DelaySeconds"] == 900
    assert tasks.calls == []
    assert pending(sqs) == 1  # the new, delayed copy


def test_due_message_runs(sqs: SQSClient) -> None:
    send_raw(sqs, raw_message(run_after=datetime.now(UTC) - timedelta(seconds=1)))
    assert worker().run_once("default") == [Outcome.SUCCEEDED]


# --------------------------------------------------------------- heartbeat


def test_heartbeat_extends_visibility_of_long_tasks(sqs: SQSClient) -> None:
    tasks.add.enqueue(1, 2)
    w = worker(options=WorkerOptions(wait_time_seconds=0, visibility_timeout=2))
    client = sqs_backend().client
    real_call = type(tasks.add).call

    def slow_call(self: Any, *args: Any, **kwargs: Any) -> Any:
        time.sleep(1.3)
        return real_call(self, *args, **kwargs)

    with (
        mock.patch.object(type(tasks.add), "call", slow_call),
        mock.patch.object(
            client, "change_message_visibility", wraps=client.change_message_visibility
        ) as spy,
    ):
        assert w.run_once("default") == [Outcome.SUCCEEDED]
    assert spy.call_count >= 1
    assert spy.call_args.kwargs["VisibilityTimeout"] == 2


def test_heartbeat_reads_queue_timeout_once(sqs: SQSClient) -> None:
    w = worker(options=WorkerOptions(wait_time_seconds=0))
    client = sqs_backend().client
    url = sqs_backend().get_queue_url("default")
    with mock.patch.object(
        client, "get_queue_attributes", wraps=client.get_queue_attributes
    ) as spy:
        assert w._queue_visibility_timeout(url) == 30
        assert w._queue_visibility_timeout(url) == 30
    assert spy.call_count == 1


def test_heartbeat_errors_are_logged_not_raised(
    sqs: SQSClient, caplog: pytest.LogCaptureFixture
) -> None:
    tasks.add.enqueue(1, 2)
    w = worker(options=WorkerOptions(wait_time_seconds=0, visibility_timeout=2))
    client = sqs_backend().client
    real_call = type(tasks.add).call

    def slow_call(self: Any, *args: Any, **kwargs: Any) -> Any:
        time.sleep(1.3)
        return real_call(self, *args, **kwargs)

    with (
        mock.patch.object(type(tasks.add), "call", slow_call),
        mock.patch.object(client, "change_message_visibility", side_effect=RuntimeError("x")),
    ):
        assert w.run_once("default") == [Outcome.SUCCEEDED]
    assert "Could not extend visibility" in caplog.text


# --------------------------------------------------------------- lifecycle


def test_run_until_stopped(sqs: SQSClient) -> None:
    for i in range(3):
        tasks.add.enqueue(i, i)
    tasks.send_email.enqueue("x@y.z")
    w = Worker(options=WorkerOptions(wait_time_seconds=1, heartbeat=False), concurrency=2)
    thread = threading.Thread(target=w.run)
    thread.start()
    deadline = time.monotonic() + 10
    while len(tasks.calls) < 4 and time.monotonic() < deadline:
        time.sleep(0.05)
    w.stop()
    thread.join(timeout=10)
    assert not thread.is_alive()
    assert w.stopping
    assert sorted(name for name, _ in tasks.calls) == ["add", "add", "add", "send_email"]


def test_polling_errors_are_retried(sqs: SQSClient, caplog: pytest.LogCaptureFixture) -> None:
    w = worker()
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
    assert calls == 2
    assert "Error polling queue default" in caplog.text


def test_worker_validation(sqs: SQSClient) -> None:
    with pytest.raises(TypeError, match="not an SQSBackend"):
        Worker(backend_alias="immediate")
    with pytest.raises(ValueError, match="not configured"):
        Worker(queue_names=["nope"])
    with pytest.raises(ValueError, match="concurrency"):
        Worker(concurrency=0)


def test_default_consumes_all_backend_queues(sqs: SQSClient) -> None:
    assert Worker().queue_names == ["default", "emails", "orders.fifo"]


def test_body_is_json(sqs: SQSClient) -> None:
    tasks.add.enqueue(1, 2)
    url = sqs.get_queue_url(QueueName="test-default")["QueueUrl"]
    [m] = sqs.receive_message(QueueUrl=url)["Messages"]
    assert json.loads(m["Body"])["task"] == "tests.tasks.add"


def test_failing_task_ends_in_dead_letter_queue(sqs: SQSClient, settings: Any) -> None:
    dlq_url = sqs.create_queue(QueueName="test-dlq")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    url = sqs.create_queue(
        QueueName="test-redrive",
        Attributes={
            "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "2"})
        },
    )["QueueUrl"]
    settings.TASKS = {
        "default": {
            "BACKEND": "django_tasks_sqs.SQSBackend",
            "QUEUES": ["default"],
            "OPTIONS": {"queue_urls": {"default": url}},
        }
    }
    tasks.explode.enqueue()
    w = worker(options=WorkerOptions(wait_time_seconds=0, heartbeat=False, retry_backoff=0))
    assert w.run_once("default") == [Outcome.FAILED]
    assert w.run_once("default") == [Outcome.FAILED]
    assert w.run_once("default") == []  # moved to the DLQ after 2 receives
    [dead] = sqs.receive_message(QueueUrl=dlq_url)["Messages"]
    assert json.loads(dead["Body"])["task"] == "tests.tasks.explode"
