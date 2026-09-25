"""A ``django.tasks`` backend that enqueues tasks on Amazon SQS."""

from __future__ import annotations

import threading
from collections.abc import Iterable, Iterator, Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any

import boto3
from asgiref.sync import sync_to_async
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

#: ``SendMessageBatch`` takes at most 10 messages per request...
MAX_BATCH_ENTRIES = 10
#: ...and caps their combined size. 256 KiB is the historical (and most portable) limit.
MAX_BATCH_BYTES = 262_144

#: A task call for :meth:`SQSBackend.enqueue_many`: ``(task, args, kwargs)``.
type TaskCall = tuple[Task[..., Any], Sequence[Any], Mapping[str, Any]]

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


class EnqueueBatchError(Exception):
    """Some tasks of an :meth:`SQSBackend.enqueue_many` call were not sent.

    ``enqueued`` holds the results of the tasks that *were* sent; ``failed`` pairs
    each unsent task's result with the error SQS reported for it.
    """

    def __init__(
        self,
        enqueued: list[TaskResult[..., Any]],
        failed: list[tuple[TaskResult[..., Any], str]],
    ) -> None:
        super().__init__(f"{len(failed)} of {len(enqueued) + len(failed)} tasks were not sent")
        self.enqueued = enqueued
        self.failed = failed


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

    def _message_request(
        self, message: TaskMessage, queue_url: str, now: datetime
    ) -> dict[str, Any]:
        """``SendMessage`` parameters for ``message`` (also valid as a batch entry)."""
        request: dict[str, Any] = {
            "MessageBody": message.to_json(),
            "MessageAttributes": {
                "task": {"DataType": "String", "StringValue": message.task_path},
            },
        }
        if self.is_fifo(queue_url):
            request["MessageGroupId"] = self.fifo_message_group_id
            request["MessageDeduplicationId"] = message.id
        else:
            request["DelaySeconds"] = delay_seconds(message.run_after, now)
        return request

    def send(self, message: TaskMessage, *, now: datetime | None = None) -> str:
        """Send a task message and return the SQS message id."""
        queue_url = self.get_queue_url(message.queue_name)
        request = self._message_request(message, queue_url, now or timezone.now())
        return self.client.send_message(QueueUrl=queue_url, **request)["MessageId"]

    # ------------------------------------------------------- django.tasks

    def validate_task(self, task: Task[..., Any]) -> None:
        super().validate_task(task)
        explicit_url = self.queue_urls.get(task.queue_name)
        if task.run_after is not None and explicit_url and self.is_fifo(explicit_url):
            raise InvalidTask("FIFO queues do not support run_after (per-message delays).")

    def _prepare[**P, R](
        self, task: Task[P, R], args: Sequence[Any], kwargs: Mapping[str, Any], now: datetime
    ) -> tuple[TaskResult[P, R], TaskMessage]:
        """Validate a task call and build its result and message, without sending."""
        self.validate_task(task)
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
        return result, message

    def enqueue[**P, R](
        self, task: Task[P, R], args: list[Any], kwargs: dict[str, Any]
    ) -> TaskResult[P, R]:
        now = timezone.now()
        result, message = self._prepare(task, args, kwargs, now)
        self.send(message, now=now)
        task_enqueued.send(type(self), task_result=result)
        return result

    def enqueue_many(self, calls: Iterable[TaskCall]) -> list[TaskResult[..., Any]]:
        """Enqueue several tasks with as few ``SendMessageBatch`` requests as possible.

        Every call is validated before anything is sent. Results come back in the
        order of ``calls``. If SQS rejects some messages, the others are still sent
        and :class:`EnqueueBatchError` is raised listing both.
        """
        now = timezone.now()
        prepared: list[tuple[TaskResult[..., Any], TaskMessage]] = []
        for task, args, kwargs in calls:
            if task.backend != self.alias:
                raise InvalidTask(f"Task {task.module_path!r} does not use backend {self.alias!r}.")
            prepared.append(self._prepare(task, args, kwargs, now))

        by_queue: dict[str, list[tuple[TaskResult[..., Any], TaskMessage]]] = {}
        for result, message in prepared:
            by_queue.setdefault(message.queue_name, []).append((result, message))

        failed: dict[str, str] = {}
        for queue_name, items in by_queue.items():
            queue_url = self.get_queue_url(queue_name)
            requests = [
                (result, self._message_request(message, queue_url, now))
                for result, message in items
            ]
            for chunk in _batches(requests):
                entries: list[Any] = [{"Id": str(i), **req} for i, (_, req) in enumerate(chunk)]
                response = self.client.send_message_batch(QueueUrl=queue_url, Entries=entries)
                errors = {
                    int(f["Id"]): f"{f['Code']}: {f.get('Message', '')}"
                    for f in response.get("Failed", [])
                }
                for i, (result, _) in enumerate(chunk):
                    if i in errors:
                        failed[result.id] = errors[i]
                    else:
                        task_enqueued.send(type(self), task_result=result)

        results = [result for result, _ in prepared]
        if failed:
            raise EnqueueBatchError(
                [r for r in results if r.id not in failed],
                [(r, failed[r.id]) for r in results if r.id in failed],
            )
        return results

    async def aenqueue_many(self, calls: Iterable[TaskCall]) -> list[TaskResult[..., Any]]:
        return await sync_to_async(self.enqueue_many, thread_sensitive=True)(calls)

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


def _request_size(request: dict[str, Any]) -> int:
    """Bytes a batch entry counts against ``MAX_BATCH_BYTES`` (body plus attributes)."""
    size = len(request["MessageBody"].encode())
    for name, value in request["MessageAttributes"].items():
        size += len(name.encode()) + len(value["DataType"].encode())
        size += len(value["StringValue"].encode())
    return size


def _batches[T](
    requests: list[tuple[T, dict[str, Any]]],
) -> Iterator[list[tuple[T, dict[str, Any]]]]:
    """Split requests into chunks that fit in one ``SendMessageBatch`` call."""
    chunk: list[tuple[T, dict[str, Any]]] = []
    chunk_bytes = 0
    for item in requests:
        size = _request_size(item[1])
        if chunk and (len(chunk) == MAX_BATCH_ENTRIES or chunk_bytes + size > MAX_BATCH_BYTES):
            yield chunk
            chunk, chunk_bytes = [], 0
        chunk.append(item)
        chunk_bytes += size
    if chunk:
        yield chunk
