"""End-to-end: enqueue through the backend, run through the worker, on LocalStack."""

from __future__ import annotations

import json
import threading
import time
from datetime import timedelta

from django.utils import timezone
from mypy_boto3_sqs import SQSClient

from django_tasks_sqs import Worker, WorkerOptions
from django_tasks_sqs.worker import Outcome
from tests import tasks

NO_WAIT = WorkerOptions(wait_time_seconds=0, heartbeat=False)


def queue_url(sqs: SQSClient, name: str) -> str:
    return sqs.get_queue_url(QueueName=name)["QueueUrl"]


def pending(sqs: SQSClient, name: str) -> int:
    attrs = sqs.get_queue_attributes(
        QueueUrl=queue_url(sqs, name),
        AttributeNames=[
            "ApproximateNumberOfMessages",
            "ApproximateNumberOfMessagesNotVisible",
            "ApproximateNumberOfMessagesDelayed",
        ],
    )["Attributes"]
    return sum(int(value) for value in attrs.values())


def test_enqueue_and_run(sqs: SQSClient, prefix: str) -> None:
    tasks.add.enqueue(2, 3)
    assert Worker(options=NO_WAIT).run_once("default") == [Outcome.SUCCEEDED]
    assert tasks.calls == [("add", (2, 3))]
    assert pending(sqs, f"{prefix}default") == 0


def test_fifo_queue(sqs: SQSClient, prefix: str) -> None:
    for order_id in (1, 2, 3):
        tasks.place_order.enqueue(order_id)
    w = Worker(options=NO_WAIT)
    for _ in range(3):
        assert w.run_once("orders.fifo") == [Outcome.SUCCEEDED]
    assert tasks.calls == [("place_order", 1), ("place_order", 2), ("place_order", 3)]


def test_failure_is_redelivered_after_backoff(sqs: SQSClient, prefix: str) -> None:
    tasks.explode.enqueue()
    w = Worker(options=WorkerOptions(wait_time_seconds=0, heartbeat=False, retry_backoff=1))
    assert w.run_once("default") == [Outcome.FAILED]
    assert w.run_once("default") == []  # invisible during the backoff
    assert w.run_once("default", wait_time_seconds=5) == [Outcome.FAILED]


def test_failing_task_ends_in_dead_letter_queue(sqs: SQSClient, prefix: str) -> None:
    dlq_url = sqs.create_queue(QueueName=f"{prefix}dlq")["QueueUrl"]
    dlq_arn = sqs.get_queue_attributes(QueueUrl=dlq_url, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    sqs.set_queue_attributes(
        QueueUrl=queue_url(sqs, f"{prefix}default"),
        Attributes={
            "RedrivePolicy": json.dumps({"deadLetterTargetArn": dlq_arn, "maxReceiveCount": "2"})
        },
    )
    try:
        tasks.explode.enqueue()
        w = Worker(options=WorkerOptions(wait_time_seconds=0, heartbeat=False, retry_backoff=0))
        assert w.run_once("default", wait_time_seconds=2) == [Outcome.FAILED]
        assert w.run_once("default", wait_time_seconds=2) == [Outcome.FAILED]
        assert w.run_once("default", wait_time_seconds=1) == []
        dead = sqs.receive_message(QueueUrl=dlq_url, WaitTimeSeconds=5)["Messages"]
        assert json.loads(dead[0]["Body"])["task"] == "tests.tasks.explode"
    finally:
        sqs.delete_queue(QueueUrl=dlq_url)


def test_run_after_uses_sqs_delay(sqs: SQSClient, prefix: str) -> None:
    tasks.add.using(run_after=timezone.now() + timedelta(seconds=2)).enqueue(1, 1)
    w = Worker(options=NO_WAIT)
    assert w.run_once("default") == []
    assert w.run_once("default", wait_time_seconds=10) == [Outcome.SUCCEEDED]
    assert tasks.calls == [("add", (1, 1))]


def test_heartbeat_keeps_long_task_invisible(sqs: SQSClient, prefix: str) -> None:
    tasks.nap.enqueue(4)
    w = Worker(options=WorkerOptions(wait_time_seconds=0, visibility_timeout=2))
    other = Worker(options=NO_WAIT)
    outcome: list[str] = []
    thread = threading.Thread(target=lambda: outcome.extend(w.run_once("default")))
    thread.start()
    time.sleep(3)  # past the original visibility timeout
    assert other.run_once("default") == []  # not redelivered mid-run
    thread.join(timeout=10)
    assert outcome == [Outcome.SUCCEEDED]
    assert pending(sqs, f"{prefix}default") == 0


def test_run_until_stopped(sqs: SQSClient, prefix: str) -> None:
    for i in range(3):
        tasks.add.enqueue(i, i)
    tasks.send_email.enqueue("x@y.z")
    w = Worker(options=WorkerOptions(wait_time_seconds=1, heartbeat=False), concurrency=2)
    thread = threading.Thread(target=w.run)
    thread.start()
    deadline = time.monotonic() + 20
    while len(tasks.calls) < 4 and time.monotonic() < deadline:
        time.sleep(0.1)
    w.stop()
    thread.join(timeout=10)
    assert not thread.is_alive()
    assert sorted(name for name, _ in tasks.calls) == ["add", "add", "add", "send_email"]
