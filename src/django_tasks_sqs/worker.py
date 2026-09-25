"""The worker: long-polls SQS and runs the tasks it receives."""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from traceback import format_exception
from typing import TYPE_CHECKING, Any

from django.db import close_old_connections
from django.tasks import Task, TaskContext, TaskResult, TaskResultStatus, task_backends
from django.tasks.base import TaskError
from django.tasks.exceptions import InvalidTask, InvalidTaskBackend
from django.tasks.signals import task_finished, task_started
from django.utils import timezone
from django.utils.crypto import get_random_string
from django.utils.json import normalize_json
from django.utils.module_loading import import_string

from .backend import SQSBackend
from .health import WorkerStats
from .message import InvalidMessage, TaskMessage
from .signals import message_processed

if TYPE_CHECKING:
    from mypy_boto3_sqs.type_defs import MessageTypeDef

logger = logging.getLogger("django_tasks_sqs")

#: SQS caps a message's visibility timeout at 12 hours.
MAX_VISIBILITY_TIMEOUT = 43_200


class Outcome:
    """What happened to a received message (useful for tests and metrics)."""

    SUCCEEDED = "succeeded"
    FAILED = "failed"
    DEFERRED = "deferred"
    INVALID = "invalid"


@dataclass(frozen=True, slots=True)
class WorkerOptions:
    wait_time_seconds: int = 20
    """Long-polling wait. 20s (the maximum) keeps the number of empty receives low."""
    max_messages: int = 1
    """Messages fetched per receive. Keep it low for slow tasks, so messages
    don't sit invisible in one busy worker while others are idle."""
    visibility_timeout: int | None = None
    """Override the queue's visibility timeout for received messages."""
    heartbeat: bool = True
    """Keep extending the visibility timeout while a task runs, so long tasks
    are not redelivered to another worker halfway through."""
    retry_backoff: int | None = None
    """Base seconds for exponential backoff on failure (``base * 2**(attempt-1)``).
    ``None`` retries when the visibility timeout expires."""


def backoff_seconds(base: int, attempt: int) -> int:
    exponent = min(max(0, attempt - 1), 20)  # 2**20 already exceeds the cap
    return min(MAX_VISIBILITY_TIMEOUT, base * (1 << exponent))


class Worker:
    """Consume one or more queues of an :class:`SQSBackend`.

    Each queue gets ``concurrency`` polling threads; each thread runs one task at
    a time. Failed tasks are *not* deleted: SQS redelivers them after the
    visibility timeout, and a redrive policy on the queue moves them to a
    dead-letter queue after ``maxReceiveCount`` attempts.
    """

    def __init__(
        self,
        *,
        backend_alias: str = "default",
        queue_names: list[str] | None = None,
        concurrency: int = 1,
        options: WorkerOptions | None = None,
    ) -> None:
        backend = task_backends[backend_alias]
        if not isinstance(backend, SQSBackend):
            raise TypeError(f"Task backend {backend_alias!r} is not an SQSBackend")
        self.backend = backend
        self.queue_names = sorted(queue_names or backend.queues)
        unknown = set(self.queue_names) - backend.queues
        if unknown:
            raise ValueError(f"Queues not configured for backend {backend_alias!r}: {unknown}")
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        self.concurrency = concurrency
        self.options = options or WorkerOptions()
        self.worker_id = get_random_string(32)
        self._stop = threading.Event()
        self._visibility_timeouts: dict[str, int] = {}
        self.stats = WorkerStats(self.queue_names)

    # --------------------------------------------------------------- lifecycle

    def stop(self) -> None:
        """Ask every thread to finish its current task and exit."""
        logger.info("Worker %s stopping after current tasks", self.worker_id)
        self._stop.set()

    @property
    def stopping(self) -> bool:
        return self._stop.is_set()

    def run(self) -> None:
        """Block, polling every queue, until :meth:`stop` is called."""
        logger.info(
            "Worker %s consuming %s (concurrency=%d)",
            self.worker_id,
            ", ".join(self.queue_names),
            self.concurrency,
        )
        threads = [
            threading.Thread(
                target=self._poll_forever, args=(queue,), name=f"sqs-{queue}-{i}", daemon=True
            )
            for queue in self.queue_names
            for i in range(self.concurrency)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        logger.info("Worker %s stopped", self.worker_id)

    def _poll_forever(self, queue_name: str) -> None:
        self.stats.register_thread()
        while not self.stopping:
            try:
                self.run_once(queue_name)
            except Exception:
                self.stats.poll_failed(queue_name)
                logger.exception("Error polling queue %s; retrying in 5s", queue_name)
                self._stop.wait(5)

    # ---------------------------------------------------------------- polling

    def run_once(self, queue_name: str, *, wait_time_seconds: int | None = None) -> list[str]:
        """Receive one batch from ``queue_name`` and process it. Returns the outcomes."""
        request: dict[str, Any] = {
            "QueueUrl": self.backend.get_queue_url(queue_name),
            "MaxNumberOfMessages": self.options.max_messages,
            "WaitTimeSeconds": (
                self.options.wait_time_seconds if wait_time_seconds is None else wait_time_seconds
            ),
            "MessageSystemAttributeNames": ["ApproximateReceiveCount"],
        }
        if self.options.visibility_timeout is not None:
            request["VisibilityTimeout"] = self.options.visibility_timeout
        response = self.backend.client.receive_message(**request)
        self.stats.polled()
        messages = response.get("Messages", [])
        if not messages:
            return []
        with self.stats.busy():
            return [self.process(queue_name, m) for m in messages]

    def process(self, queue_name: str, sqs_message: MessageTypeDef) -> str:
        """Handle a single received SQS message. Returns an :class:`Outcome` value."""
        started = time.monotonic()
        outcome = self._process(queue_name, sqs_message)
        self.stats.processed(queue_name, outcome)
        message_processed.send(
            type(self),
            worker=self,
            queue_name=queue_name,
            outcome=outcome,
            duration=time.monotonic() - started,
        )
        return outcome

    def _process(self, queue_name: str, sqs_message: MessageTypeDef) -> str:
        queue_url = self.backend.get_queue_url(queue_name)
        receipt = sqs_message["ReceiptHandle"]
        attempt = int(sqs_message.get("Attributes", {}).get("ApproximateReceiveCount", "1"))

        try:
            message = TaskMessage.from_json(sqs_message["Body"])
            task = self._load_task(message)
        except InvalidMessage as exc:
            # Leave it: it will be retried and, with a redrive policy, dead-lettered.
            logger.error("Unprocessable message %s: %s", sqs_message.get("MessageId"), exc)
            return Outcome.INVALID

        now = timezone.now()
        if message.run_after is not None and message.run_after > now:
            # Deferred beyond SQS's 15-minute delay limit: send it again, closer to due.
            self.backend.send(message, now=now)
            self.backend.client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
            return Outcome.DEFERRED

        with self._heartbeat(queue_url, receipt):
            result = self._execute(task, message, attempt)

        if result.status == TaskResultStatus.SUCCESSFUL:
            self.backend.client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt)
            return Outcome.SUCCEEDED

        if self.options.retry_backoff is not None:
            self.backend.client.change_message_visibility(
                QueueUrl=queue_url,
                ReceiptHandle=receipt,
                VisibilityTimeout=backoff_seconds(self.options.retry_backoff, attempt),
            )
        return Outcome.FAILED

    # -------------------------------------------------------------- execution

    @staticmethod
    def _load_task(message: TaskMessage) -> Task[..., Any]:
        try:
            task: object = import_string(message.task_path)
        except ImportError as exc:
            raise InvalidMessage(f"cannot import task {message.task_path!r}") from exc
        if not isinstance(task, Task):
            raise InvalidMessage(f"{message.task_path!r} is not a django.tasks Task")
        try:
            return task.using(queue_name=message.queue_name, backend=message.backend)
        except (InvalidTask, InvalidTaskBackend) as exc:
            raise InvalidMessage(f"cannot run {message.task_path!r}: {exc}") from exc

    def _execute(
        self, task: Task[..., Any], message: TaskMessage, attempt: int
    ) -> TaskResult[..., Any]:
        started = timezone.now()
        result: TaskResult[..., Any] = TaskResult(
            task=task,
            id=message.id,
            status=TaskResultStatus.RUNNING,
            enqueued_at=message.enqueued_at,
            started_at=started,
            last_attempted_at=started,
            finished_at=None,
            args=message.args,
            kwargs=message.kwargs,
            backend=message.backend,
            errors=[],
            # SQS only tells us how many times the message was received, not by
            # whom; this keeps ``TaskResult.attempts`` / ``TaskContext.attempt`` right.
            worker_ids=[self.worker_id] * attempt,
        )
        close_old_connections()
        task_started.send(type(self.backend), task_result=result)
        try:
            if task.takes_context:
                value = task.call(TaskContext(task_result=result), *result.args, **result.kwargs)
            else:
                value = task.call(*result.args, **result.kwargs)
            object.__setattr__(result, "_return_value", normalize_json(value))
        except KeyboardInterrupt:
            raise
        except BaseException as exc:
            object.__setattr__(result, "finished_at", timezone.now())
            object.__setattr__(result, "status", TaskResultStatus.FAILED)
            exc_type = type(exc)
            result.errors.append(
                TaskError(
                    exception_class_path=f"{exc_type.__module__}.{exc_type.__qualname__}",
                    traceback="".join(format_exception(exc)),
                )
            )
            task_finished.send(type(self.backend), task_result=result)
        else:
            object.__setattr__(result, "finished_at", timezone.now())
            object.__setattr__(result, "status", TaskResultStatus.SUCCESSFUL)
            task_finished.send(type(self.backend), task_result=result)
        finally:
            close_old_connections()
        return result

    # -------------------------------------------------------------- heartbeat

    def _queue_visibility_timeout(self, queue_url: str) -> int:
        if self.options.visibility_timeout is not None:
            return self.options.visibility_timeout
        if queue_url not in self._visibility_timeouts:
            attributes = self.backend.client.get_queue_attributes(
                QueueUrl=queue_url, AttributeNames=["VisibilityTimeout"]
            )["Attributes"]
            self._visibility_timeouts[queue_url] = int(attributes["VisibilityTimeout"])
        return self._visibility_timeouts[queue_url]

    @contextmanager
    def _heartbeat(self, queue_url: str, receipt: str) -> Iterator[None]:
        """Extend the message's visibility every half timeout while the task runs."""
        if not self.options.heartbeat:
            yield
            return
        timeout = self._queue_visibility_timeout(queue_url)
        done = threading.Event()

        def beat() -> None:
            while not done.wait(max(1.0, timeout / 2)):
                try:
                    self.backend.client.change_message_visibility(
                        QueueUrl=queue_url, ReceiptHandle=receipt, VisibilityTimeout=timeout
                    )
                except Exception:
                    logger.exception("Could not extend visibility of a running task")

        thread = threading.Thread(target=beat, name="sqs-heartbeat", daemon=True)
        thread.start()
        try:
            yield
        finally:
            done.set()
            thread.join()
