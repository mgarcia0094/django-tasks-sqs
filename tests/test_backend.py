from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest import mock

import pytest
from django.core.checks import Warning as CheckWarning
from django.tasks import TaskResultStatus
from django.tasks.exceptions import InvalidTask
from django.tasks.signals import task_enqueued
from mypy_boto3_sqs import SQSClient

from django_tasks_sqs import SQSBackend
from django_tasks_sqs.backend import MAX_DELAY_SECONDS, delay_seconds
from tests import tasks
from tests.conftest import sqs_backend


def receive(sqs: SQSClient, queue: str = "test-default") -> list[Any]:
    url = sqs.get_queue_url(QueueName=queue)["QueueUrl"]
    return sqs.receive_message(
        QueueUrl=url, MaxNumberOfMessages=10, MessageAttributeNames=["All"]
    ).get("Messages", [])


def test_enqueue_sends_a_message(sqs: SQSClient) -> None:
    result = tasks.add.enqueue(2, 3)

    assert result.status == TaskResultStatus.READY
    assert result.enqueued_at is not None
    assert result.backend == "default"
    [message] = receive(sqs)
    body = json.loads(message["Body"])
    assert body["id"] == result.id
    assert body["task"] == "tests.tasks.add"
    assert body["args"] == [2, 3]
    assert message["MessageAttributes"]["task"]["StringValue"] == "tests.tasks.add"
    assert tasks.calls == []  # nothing ran yet


def test_enqueue_routes_by_queue_name(sqs: SQSClient) -> None:
    tasks.send_email.enqueue("a@b.c", subject="hi")
    assert receive(sqs, "test-default") == []
    [message] = receive(sqs, "test-emails")
    assert json.loads(message["Body"])["kwargs"] == {"subject": "hi"}


def test_enqueue_sends_signal(sqs: SQSClient) -> None:
    received: list[Any] = []

    def handler(sender: Any, task_result: Any, **kwargs: Any) -> None:
        received.append(task_result)

    task_enqueued.connect(handler)
    try:
        result = tasks.add.enqueue(1, 1)
    finally:
        task_enqueued.disconnect(handler)
    assert received == [result]


def test_short_defer_uses_delay_seconds(sqs: SQSClient) -> None:
    backend = sqs_backend()
    with mock.patch.object(
        backend.client, "send_message", wraps=backend.client.send_message
    ) as spy:
        tasks.add.using(run_after=datetime.now(UTC) + timedelta(seconds=60)).enqueue(1, 2)
    assert 59 <= spy.call_args.kwargs["DelaySeconds"] <= 60


def test_long_defer_is_capped_and_keeps_run_after(sqs: SQSClient) -> None:
    run_after = datetime.now(UTC) + timedelta(hours=3)
    backend = sqs_backend()
    with mock.patch.object(
        backend.client, "send_message", wraps=backend.client.send_message
    ) as spy:
        tasks.add.using(run_after=run_after).enqueue(1, 2)
    assert spy.call_args.kwargs["DelaySeconds"] == MAX_DELAY_SECONDS
    body = json.loads(spy.call_args.kwargs["MessageBody"])
    assert datetime.fromisoformat(body["run_after"]) == run_after


@pytest.mark.parametrize(
    ("offset", "expected"),
    [(None, 0), (-10, 0), (0.2, 1), (60, 60), (10_000, MAX_DELAY_SECONDS)],
)
def test_delay_seconds(offset: float | None, expected: int) -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)
    run_after = None if offset is None else now + timedelta(seconds=offset)
    assert delay_seconds(run_after, now) == expected


def test_fifo_queue_sets_group_and_deduplication(sqs: SQSClient) -> None:
    backend = sqs_backend()
    with mock.patch.object(
        backend.client, "send_message", wraps=backend.client.send_message
    ) as spy:
        result = tasks.place_order.enqueue(7)
    kwargs = spy.call_args.kwargs
    assert kwargs["MessageGroupId"] == "default"
    assert kwargs["MessageDeduplicationId"] == result.id
    assert "DelaySeconds" not in kwargs
    assert len(receive(sqs, "test-orders.fifo")) == 1


def test_fifo_rejects_run_after_on_enqueue(sqs: SQSClient) -> None:
    deferred = tasks.place_order.using(run_after=datetime.now(UTC) + timedelta(minutes=1))
    with pytest.raises(InvalidTask, match="FIFO"):
        deferred.enqueue(1)


def test_fifo_rejects_run_after_on_validation_with_explicit_url(
    sqs: SQSClient, settings: Any
) -> None:
    url = sqs.get_queue_url(QueueName="test-orders.fifo")["QueueUrl"]
    settings.TASKS = {
        "default": {
            "BACKEND": "django_tasks_sqs.SQSBackend",
            "QUEUES": ["orders.fifo"],
            "OPTIONS": {"queue_urls": {"orders.fifo": url}},
        }
    }
    with pytest.raises(InvalidTask, match="FIFO"):
        tasks.place_order.using(run_after=datetime.now(UTC) + timedelta(minutes=1))


def test_priority_is_not_supported(sqs: SQSClient) -> None:
    with pytest.raises(InvalidTask, match="priority"):
        tasks.add.using(priority=10)


def test_unknown_queue_is_rejected(sqs: SQSClient) -> None:
    with pytest.raises(InvalidTask, match="Queue"):
        tasks.add.using(queue_name="nope")


def test_get_result_is_not_supported(sqs: SQSClient) -> None:
    result = tasks.add.enqueue(1, 2)
    with pytest.raises(NotImplementedError):
        tasks.add.get_result(result.id)


def test_queue_url_is_resolved_once(sqs: SQSClient) -> None:
    backend = sqs_backend()
    with mock.patch.object(
        backend.client, "get_queue_url", wraps=backend.client.get_queue_url
    ) as spy:
        tasks.add.enqueue(1, 2)
        tasks.add.enqueue(3, 4)
    assert spy.call_count == 1


def test_client_options_are_passed_to_boto3() -> None:
    backend = SQSBackend(
        "x",
        {
            "OPTIONS": {
                "region_name": "us-east-2",
                "endpoint_url": "http://localhost:4566",
                "client_kwargs": {"aws_access_key_id": "k", "aws_secret_access_key": "s"},
            }
        },
    )
    with mock.patch("django_tasks_sqs.backend.boto3.client") as client:
        assert backend.client is backend.client
    client.assert_called_once_with(
        "sqs",
        region_name="us-east-2",
        endpoint_url="http://localhost:4566",
        aws_access_key_id="k",
        aws_secret_access_key="s",
    )


def test_check_warns_about_unknown_options_and_queues() -> None:
    backend = SQSBackend(
        "x",
        {
            "QUEUES": ["default"],
            "OPTIONS": {"typo_option": 1, "queue_urls": {"other": "https://q"}},
        },
    )
    messages = backend.check()
    assert [m.id for m in messages] == ["django_tasks_sqs.W001", "django_tasks_sqs.W002"]
    assert all(isinstance(m, CheckWarning) for m in messages)


def test_check_is_clean_by_default() -> None:
    assert SQSBackend("x", {}).check() == []
