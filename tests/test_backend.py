from __future__ import annotations

import asyncio
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

from django_tasks_sqs import EnqueueBatchError, SQSBackend
from django_tasks_sqs.backend import MAX_DELAY_SECONDS, TaskCall, _request_size, delay_seconds
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


# -------------------------------------------------------------- enqueue_many


def spy_batches(backend: SQSBackend) -> Any:
    return mock.patch.object(
        backend.client, "send_message_batch", wraps=backend.client.send_message_batch
    )


def test_enqueue_many_sends_one_batch_per_queue(sqs: SQSClient) -> None:
    backend = sqs_backend()
    received: list[Any] = []

    def handler(sender: Any, task_result: Any, **kwargs: Any) -> None:
        received.append(task_result)

    task_enqueued.connect(handler)
    try:
        with spy_batches(backend) as spy:
            results = backend.enqueue_many(
                [
                    (tasks.add, [1, 2], {}),
                    (tasks.send_email, ["a@b.c"], {"subject": "hi"}),
                    (tasks.add, (3, 4), {}),
                ]
            )
    finally:
        task_enqueued.disconnect(handler)

    assert spy.call_count == 2
    assert [r.task.name for r in results] == ["add", "send_email", "add"]
    assert all(r.status == TaskResultStatus.READY for r in results)
    assert sorted(r.id for r in received) == sorted(r.id for r in results)
    default = sorted(json.loads(m["Body"])["args"] for m in receive(sqs))
    assert default == [[1, 2], [3, 4]]
    [email] = receive(sqs, "test-emails")
    body = json.loads(email["Body"])
    assert body["id"] == results[1].id
    assert body["kwargs"] == {"subject": "hi"}
    assert email["MessageAttributes"]["task"]["StringValue"] == "tests.tasks.send_email"


def test_enqueue_many_splits_into_batches_of_ten(sqs: SQSClient) -> None:
    backend = sqs_backend()
    with spy_batches(backend) as spy:
        results = backend.enqueue_many((tasks.add, [i, i], {}) for i in range(23))
    assert [len(c.kwargs["Entries"]) for c in spy.call_args_list] == [10, 10, 3]
    assert len(results) == 23


def test_enqueue_many_splits_by_size(sqs: SQSClient) -> None:
    backend = sqs_backend()
    calls: list[TaskCall] = [(tasks.send_email, ["x" * 1000], {}) for _ in range(3)]
    with spy_batches(backend) as spy:
        backend.enqueue_many(calls[:1])
    size = _request_size(spy.call_args.kwargs["Entries"][0])
    # Room for two messages, not three.
    with (
        spy_batches(backend) as spy,
        mock.patch("django_tasks_sqs.backend.MAX_BATCH_BYTES", int(size * 2.5)),
    ):
        backend.enqueue_many(calls)
    assert [len(c.kwargs["Entries"]) for c in spy.call_args_list] == [2, 1]


def test_enqueue_many_fifo_keeps_order_and_deduplicates(sqs: SQSClient) -> None:
    backend = sqs_backend()
    with spy_batches(backend) as spy:
        results = backend.enqueue_many([(tasks.place_order, [i], {}) for i in range(3)])
    entries = spy.call_args.kwargs["Entries"]
    assert [e["MessageDeduplicationId"] for e in entries] == [r.id for r in results]
    assert {e["MessageGroupId"] for e in entries} == {"default"}
    assert all("DelaySeconds" not in e for e in entries)


def test_enqueue_many_defers(sqs: SQSClient) -> None:
    backend = sqs_backend()
    deferred = tasks.add.using(run_after=datetime.now(UTC) + timedelta(seconds=60))
    with spy_batches(backend) as spy:
        backend.enqueue_many([(deferred, [1, 1], {}), (tasks.add, [2, 2], {})])
    delays = [e["DelaySeconds"] for e in spy.call_args.kwargs["Entries"]]
    assert 59 <= delays[0] <= 60
    assert delays[1] == 0


@pytest.mark.parametrize(
    "bad",
    [
        tasks.add.using(backend="immediate"),
        tasks.place_order.using(run_after=datetime.now(UTC) + timedelta(minutes=1)),
    ],
)
def test_enqueue_many_validates_everything_before_sending(sqs: SQSClient, bad: Any) -> None:
    backend = sqs_backend()
    with spy_batches(backend) as spy, pytest.raises(InvalidTask):
        backend.enqueue_many([(tasks.add, [1, 1], {}), (bad, [1], {})])
    assert spy.call_count == 0
    assert receive(sqs) == []


def test_enqueue_many_reports_partial_failures(sqs: SQSClient) -> None:
    backend = sqs_backend()
    received: list[Any] = []

    def handler(sender: Any, task_result: Any, **kwargs: Any) -> None:
        received.append(task_result)

    response = {
        "Successful": [],
        "Failed": [{"Id": "1", "SenderFault": True, "Code": "Throttled", "Message": "slow down"}],
    }
    task_enqueued.connect(handler)
    try:
        with (
            mock.patch.object(backend.client, "send_message_batch", return_value=response),
            pytest.raises(EnqueueBatchError, match="1 of 3 tasks were not sent") as excinfo,
        ):
            backend.enqueue_many([(tasks.add, [i, i], {}) for i in range(3)])
    finally:
        task_enqueued.disconnect(handler)

    error = excinfo.value
    assert [r.args for r in error.enqueued] == [[0, 0], [2, 2]]
    [(failed, reason)] = error.failed
    assert failed.args == [1, 1]
    assert reason == "Throttled: slow down"
    assert received == error.enqueued


def test_enqueue_many_with_nothing_sends_nothing(sqs: SQSClient) -> None:
    backend = sqs_backend()
    with spy_batches(backend) as spy:
        assert backend.enqueue_many([]) == []
    assert spy.call_count == 0


def test_aenqueue_many(sqs: SQSClient) -> None:
    results = asyncio.run(sqs_backend().aenqueue_many([(tasks.add, [5, 6], {})]))
    [message] = receive(sqs)
    assert json.loads(message["Body"])["id"] == results[0].id
