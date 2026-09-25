"""A ``django.tasks`` backend that enqueues tasks on Amazon SQS."""

from __future__ import annotations

import threading
from datetime import datetime
from typing import TYPE_CHECKING, Any

import boto3
from django.core import checks
from django.tasks import Task, TaskResult, TaskResultStatus
from django.tasks.backends.base import BaseTaskBackend
from django.tasks.exceptions import InvalidTask
from django.tasks.signals import task_enqueued
from django.utils import timezone
from django.utils.crypto import get_random_string

from .message import TaskMessage

if TYPE_CHECKING:
    from mypy_boto3_sqs import SQSClient

#: SQS refuses per-message delays longer than 15 minutes.
MAX_DELAY_SECONDS = 900

KNOWN_OPTIONS = {
    "queue_urls",
    "queue_name_prefix",
    "region_name",
    "endpoint_url",
    "client_kwargs",
    "fifo_message_group_id",
}


def delay_seconds(run_after: datetime | None, now: datetime) -> int:
    """Seconds to delay delivery, capped at the SQS maximum of 15 minutes.

    Longer deferrals are handled by the worker, which re-delays the message
    until ``run_after`` is reached.
    """
    if run_after is None:
        return 0
    remaining = (run_after - now).total_seconds()
    return max(0, min(MAX_DELAY_SECONDS, int(remaining + 0.999)))


class SQSBackend(BaseTaskBackend):
    """Enqueue ``django.tasks`` on Amazon SQS. Run them with ``manage.py sqs_worker``.

    Settings::

        TASKS = {
            "default": {
                "BACKEND": "django_tasks_sqs.SQSBackend",
                "QUEUES": ["default", "emails"],
                "OPTIONS": {
                    # Either map queue names to URLs explicitly...
                    "queue_urls": {"default": "https://sqs.eu-west-1.amazonaws.com/123/app"},
                    # ...or resolve "<prefix><queue_name>" through the SQS API.
                    "queue_name_prefix": "myapp-",
                    "region_name": "eu-west-1",
                    "endpoint_url": None,  # e.g. LocalStack
                },
            }
        }
    """

    supports_defer = True
    supports_async_task = True
    supports_get_result = False
    supports_priority = False

    def __init__(self, alias: str, params: dict[str, Any]) -> None:
        super().__init__(alias, params)
        self.queue_urls: dict[str, str] = dict(self.options.get("queue_urls", {}))
        self.queue_name_prefix: str = self.options.get("queue_name_prefix", "")
        self.fifo_message_group_id: str = self.options.get("fifo_message_group_id", "default")
        self._client: SQSClient | None = None
        self._lock = threading.Lock()

    # ----------------------------------------------------------------- SQS

    @property
    def client(self) -> SQSClient:
        """Lazily created boto3 SQS client (boto3 clients are thread-safe)."""
        if self._client is None:
            with self._lock:
                if self._client is None:
                    kwargs: dict[str, Any] = dict(self.options.get("client_kwargs", {}))
                    for key in ("region_name", "endpoint_url"):
                        if self.options.get(key):
                            kwargs[key] = self.options[key]
                    self._client = boto3.client("sqs", **kwargs)
        return self._client

    def get_queue_url(self, queue_name: str) -> str:
        """URL of the SQS queue behind a ``django.tasks`` queue name (cached)."""
        url = self.queue_urls.get(queue_name)
        if url is None:
            response = self.client.get_queue_url(QueueName=self.queue_name_prefix + queue_name)
            url = response["QueueUrl"]
            self.queue_urls[queue_name] = url
        return url

    @staticmethod
    def is_fifo(queue_url: str) -> bool:
        return queue_url.endswith(".fifo")

    def send(self, message: TaskMessage, *, now: datetime | None = None) -> str:
        """Send a task message and return the SQS message id."""
        queue_url = self.get_queue_url(message.queue_name)
        request: dict[str, Any] = {
            "QueueUrl": queue_url,
            "MessageBody": message.to_json(),
            "MessageAttributes": {
                "task": {"DataType": "String", "StringValue": message.task_path},
            },
        }
        if self.is_fifo(queue_url):
            request["MessageGroupId"] = self.fifo_message_group_id
            request["MessageDeduplicationId"] = message.id
        else:
            request["DelaySeconds"] = delay_seconds(message.run_after, now or timezone.now())
        return self.client.send_message(**request)["MessageId"]

    # ------------------------------------------------------- django.tasks

    def validate_task(self, task: Task[..., Any]) -> None:
        super().validate_task(task)
        explicit_url = self.queue_urls.get(task.queue_name)
        if task.run_after is not None and explicit_url and self.is_fifo(explicit_url):
            raise InvalidTask("FIFO queues do not support run_after (per-message delays).")

    def enqueue[**P, R](
        self, task: Task[P, R], args: list[Any], kwargs: dict[str, Any]
    ) -> TaskResult[P, R]:
        self.validate_task(task)
        now = timezone.now()
        result: TaskResult[P, R] = TaskResult(
            task=task,
            id=get_random_string(32),
            status=TaskResultStatus.READY,
            enqueued_at=now,
            started_at=None,
            last_attempted_at=None,
            finished_at=None,
            args=list(args),
            kwargs=dict(kwargs),
            backend=self.alias,
            errors=[],
            worker_ids=[],
        )
        message = TaskMessage(
            id=result.id,
            task_path=task.module_path,
            args=result.args,  # already JSON-normalised by TaskResult
            kwargs=result.kwargs,
            queue_name=task.queue_name,
            backend=self.alias,
            enqueued_at=now,
            run_after=task.run_after,
        )
        if message.run_after is not None and self.is_fifo(self.get_queue_url(task.queue_name)):
            raise InvalidTask("FIFO queues do not support run_after (per-message delays).")
        self.send(message, now=now)
        task_enqueued.send(type(self), task_result=result)
        return result

    def check(self, **kwargs: Any) -> list[checks.CheckMessage]:
        messages: list[checks.CheckMessage] = []
        unknown = set(self.options) - KNOWN_OPTIONS
        if unknown:
            messages.append(
                checks.Warning(
                    f"Unknown OPTIONS for task backend {self.alias!r}: {sorted(unknown)}",
                    hint=f"Supported options: {sorted(KNOWN_OPTIONS)}",
                    id="django_tasks_sqs.W001",
                )
            )
        extra = set(self.queue_urls) - self.queues
        if extra:
            messages.append(
                checks.Warning(
                    f"queue_urls has entries not listed in QUEUES: {sorted(extra)}",
                    hint="Tasks can only be enqueued on queues listed in QUEUES.",
                    id="django_tasks_sqs.W002",
                )
            )
        return messages
