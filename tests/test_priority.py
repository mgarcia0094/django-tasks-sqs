from __future__ import annotations

import json
import random
from collections import Counter
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest import mock

import pytest
from django.core.exceptions import ImproperlyConfigured
from django.tasks.exceptions import InvalidTask
from django.tasks.signals import task_started
from django.utils import timezone
from mypy_boto3_sqs import SQSClient

from django_tasks_sqs import SQSBackend, Worker, WorkerOptions
from django_tasks_sqs.backend import PriorityLevel, parse_priority_levels, with_suffix
from django_tasks_sqs.message import TaskMessage
from django_tasks_sqs.worker import Outcome
from tests import tasks
from tests.conftest import sqs_backend

LEVELS = [(50, "-high"), (0, ""), (-100, "-low")]
NO_WAIT = WorkerOptions(wait_time_seconds=0, heartbeat=False)


@pytest.fixture
def prio(sqs: SQSClient, settings: Any) -> SQSClient:
    """The test queues, split into three priority levels."""
    for queue in ("default", "emails"):
        for suffix in ("-high", "-low"):
            sqs.create_queue(QueueName=f"test-{queue}{suffix}")
    for suffix in ("-high", "-low"):
        sqs.create_queue(QueueName=f"test-orders{suffix}.fifo", Attributes={"FifoQueue": "true"})
    settings.TASKS = {
        "default": {
            "BACKEND": "django_tasks_sqs.SQSBackend",
            "QUEUES": ["default", "emails", "orders.fifo"],
            "OPTIONS": {
                "queue_name_prefix": "test-",
                "region_name": "eu-west-1",
                "priority_levels": LEVELS,
            },
        }
    }
    return sqs


def bodies(sqs: SQSClient, queue: str) -> list[dict[str, Any]]:
    url = sqs.get_queue_url(QueueName=queue)["QueueUrl"]
    messages = sqs.receive_message(QueueUrl=url, MaxNumberOfMessages=10).get("Messages", [])
    return [json.loads(m["Body"]) for m in messages]


# ------------------------------------------------------------ configuration


def test_parse_priority_levels_default_weights() -> None:
    assert parse_priority_levels(LEVELS) == (
        PriorityLevel(50, "-high", 4),
        PriorityLevel(0, "", 2),
        PriorityLevel(-100, "-low", 1),
    )
    assert parse_priority_levels([(0, "-a", 10), (-100, "-b", 1)])[0].weight == 10
    assert parse_priority_levels(()) == ()


@pytest.mark.parametrize(
    ("levels", "match"),
    [
        ([(0,)], "min_priority, suffix"),
        ([(0, "", 1, 2)], "min_priority, suffix"),
        ([("0", "")], "Invalid priority level"),
        ([(0, None)], "Invalid priority level"),
        ([(0, "", 0)], "positive"),
        ([(0, "", 1.5)], "positive"),
        ([(0, "-a"), (10, "-b")], "sorted"),
        ([(0, "-a"), (0, "-b")], "sorted"),
        ([(10, "-a"), (0, "-a")], "unique"),
    ],
)
def test_parse_priority_levels_rejects_bad_config(levels: Any, match: str) -> None:
    with pytest.raises(ImproperlyConfigured, match=match):
        parse_priority_levels(levels)


def test_with_suffix_keeps_fifo_last() -> None:
    assert with_suffix("emails", "-high") == "emails-high"
    assert with_suffix("orders.fifo", "-high") == "orders-high.fifo"
    assert with_suffix("emails", "") == "emails"


def test_priority_is_only_supported_with_levels() -> None:
    assert not SQSBackend("x", {}).supports_priority
    assert SQSBackend("x", {"OPTIONS": {"priority_levels": LEVELS}}).supports_priority


def test_sqs_queue_name_picks_the_first_matching_level() -> None:
    backend = SQSBackend("x", {"OPTIONS": {"priority_levels": [(50, "-high"), (0, "")]}})
    names = {p: backend.sqs_queue_name("q", p) for p in (100, 50, 49, 0, -1, -100)}
    # Priorities below every level fall into the last one.
    assert names == {100: "q-high", 50: "q-high", 49: "q", 0: "q", -1: "q", -100: "q"}
    assert backend.sqs_queue_names("q") == [("q-high", 2), ("q", 1)]
    assert SQSBackend("x", {}).sqs_queue_names("q") == [("q", 1)]


def test_check_accepts_queue_urls_for_levels() -> None:
    backend = SQSBackend(
        "x",
        {
            "QUEUES": ["default"],
            "OPTIONS": {
                "priority_levels": LEVELS,
                "queue_urls": {"default-high": "https://q1", "default": "https://q2"},
            },
        },
    )
    assert backend.check() == []


# -------------------------------------------------------------- enqueueing


@pytest.mark.parametrize(
    ("priority", "queue"),
    [(100, "test-default-high"), (50, "test-default-high"), (0, "test-default"),
     (-1, "test-default-low"), (-100, "test-default-low")],
)  # fmt: skip
def test_enqueue_routes_by_priority(prio: SQSClient, priority: int, queue: str) -> None:
    result = tasks.add.using(priority=priority).enqueue(1, 2)
    [body] = bodies(prio, queue)
    assert body["id"] == result.id
    assert body["priority"] == priority


def test_fifo_levels(prio: SQSClient) -> None:
    tasks.place_order.using(priority=60).enqueue(1)
    assert len(bodies(prio, "test-orders-high.fifo")) == 1
    with pytest.raises(InvalidTask, match="FIFO"):
        tasks.place_order.using(
            priority=60, run_after=datetime.now(UTC) + timedelta(minutes=1)
        ).enqueue(1)


def test_fifo_run_after_is_checked_against_the_level_url(prio: SQSClient, settings: Any) -> None:
    settings.TASKS["default"]["OPTIONS"]["queue_urls"] = {
        "default-high": prio.get_queue_url(QueueName="test-orders-high.fifo")["QueueUrl"]
    }
    run_after = datetime.now(UTC) + timedelta(minutes=1)
    with pytest.raises(InvalidTask, match="FIFO"):
        tasks.add.using(priority=60, run_after=run_after)
    tasks.add.using(priority=0, run_after=run_after)  # the normal level is not FIFO


def test_enqueue_many_batches_per_level(prio: SQSClient) -> None:
    backend = sqs_backend()
    with mock.patch.object(
        backend.client, "send_message_batch", wraps=backend.client.send_message_batch
    ) as spy:
        backend.enqueue_many(
            [
                (tasks.add.using(priority=90), [1, 1], {}),
                (tasks.add, [2, 2], {}),
                (tasks.add.using(priority=70), [3, 3], {}),
            ]
        )
    assert spy.call_count == 2
    assert sorted(b["args"] for b in bodies(prio, "test-default-high")) == [[1, 1], [3, 3]]
    assert [b["args"] for b in bodies(prio, "test-default")] == [[2, 2]]


# ------------------------------------------------------------------ worker


def test_worker_knows_every_level(prio: SQSClient) -> None:
    w = Worker(options=NO_WAIT)
    assert w.sqs_queues["default"] == [("default-high", 4), ("default", 2), ("default-low", 1)]
    assert ("default-low", Outcome.SUCCEEDED) in w.stats.messages
    assert ("orders-high.fifo", Outcome.SUCCEEDED) in w.stats.messages


def test_polling_order_is_weighted(prio: SQSClient) -> None:
    w = Worker(options=NO_WAIT)
    w._random = random.Random(42)
    first = Counter(w._polling_order("default")[0] for _ in range(7000))
    # Each level comes first with probability weight / total weight: 4/7, 2/7, 1/7.
    assert first["default-high"] == pytest.approx(4000, rel=0.1)
    assert first["default"] == pytest.approx(2000, rel=0.1)
    assert first["default-low"] == pytest.approx(1000, rel=0.1)


def test_only_the_last_level_is_long_polled(prio: SQSClient) -> None:
    w = Worker(options=WorkerOptions(wait_time_seconds=7, heartbeat=False))
    with (
        mock.patch.object(w, "_polling_order", return_value=["default-high", "default", "x"]),
        mock.patch.object(w, "_receive", return_value=[]) as receive,
    ):
        assert w.run_once("default") == []
    assert receive.call_args_list == [
        mock.call("default-high", 0),
        mock.call("default", 0),
        mock.call("x", 7),
    ]


def test_worker_serves_every_level(prio: SQSClient) -> None:
    tasks.add.using(priority=-50).enqueue(1, 1)
    w = Worker(options=NO_WAIT)
    # Empty levels are skipped without waiting until the one with work is found.
    assert w.run_once("default") == [Outcome.SUCCEEDED]
    assert w.stats.messages["default-low", Outcome.SUCCEEDED] == 1


def test_worker_prefers_higher_levels_under_load(prio: SQSClient) -> None:
    for i in range(10):
        tasks.add.using(priority=90).enqueue(i, i)
        tasks.add.using(priority=-90).enqueue(i, i)
    w = Worker(options=NO_WAIT)
    w._random = random.Random(1)
    for _ in range(10):
        w.run_once("default")
    served = w.stats.messages
    assert served["default-high", Outcome.SUCCEEDED] > served["default-low", Outcome.SUCCEEDED]


def test_task_runs_with_its_priority(prio: SQSClient) -> None:
    seen: list[int] = []

    def handler(sender: Any, task_result: Any, **kwargs: Any) -> None:
        seen.append(task_result.task.priority)

    tasks.add.using(priority=60).enqueue(1, 1)
    task_started.connect(handler)
    try:
        Worker(options=NO_WAIT).run_once("default")
    finally:
        task_started.disconnect(handler)
    assert seen == [60]


def test_long_deferral_stays_on_its_level(prio: SQSClient) -> None:
    message = TaskMessage(
        id="deferred",
        task_path="tests.tasks.add",
        args=[1, 1],
        kwargs={},
        queue_name="default",
        backend="default",
        enqueued_at=timezone.now(),
        run_after=timezone.now() + timedelta(hours=1),
        priority=60,
    )
    url = prio.get_queue_url(QueueName="test-default-high")["QueueUrl"]
    prio.send_message(QueueUrl=url, MessageBody=message.to_json())
    w = Worker(options=NO_WAIT)
    client = w.backend.client
    with (
        mock.patch.object(w, "_polling_order", return_value=["default-high"]),
        mock.patch.object(client, "send_message", wraps=client.send_message) as send,
    ):
        assert w.run_once("default") == [Outcome.DEFERRED]
    assert send.call_args.kwargs["QueueUrl"] == url
    assert json.loads(send.call_args.kwargs["MessageBody"])["priority"] == 60


def test_priority_message_on_a_backend_without_levels_is_invalid(sqs: SQSClient) -> None:
    body = TaskMessage(
        id="x",
        task_path="tests.tasks.add",
        args=[1, 1],
        kwargs={},
        queue_name="default",
        backend="default",
        enqueued_at=timezone.now(),
        priority=60,
    ).to_json()
    sqs.send_message(
        QueueUrl=sqs.get_queue_url(QueueName="test-default")["QueueUrl"], MessageBody=body
    )
    assert Worker(options=NO_WAIT).run_once("default") == [Outcome.INVALID]
