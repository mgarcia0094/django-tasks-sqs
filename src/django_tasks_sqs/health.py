"""Worker liveness and counters, and an optional HTTP endpoint exposing them."""

from __future__ import annotations

import logging
import threading
from collections import Counter
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from time import monotonic
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .worker import Worker

logger = logging.getLogger("django_tasks_sqs")

OUTCOMES = ("succeeded", "failed", "deferred", "invalid")


class WorkerStats:
    """Thread-safe counters and liveness information of a :class:`Worker`.

    Liveness is tracked per polling thread: a thread is alive if it received from
    SQS successfully within ``max_age`` seconds, or if it is running a task (which
    may legitimately take a long time).
    """

    def __init__(self, queue_names: Iterable[str] = ()) -> None:
        queue_names = list(queue_names)
        self._lock = threading.Lock()
        self._messages: Counter[tuple[str, str]] = Counter(
            {(queue, outcome): 0 for queue in queue_names for outcome in OUTCOMES}
        )
        self._poll_errors: Counter[str] = Counter({queue: 0 for queue in queue_names})
        self._last_poll: dict[str, float] = {}
        self._busy: set[str] = set()

    # ---------------------------------------------------------------- record

    def register_thread(self) -> None:
        """Start tracking the current thread, giving it ``max_age`` to poll."""
        self.polled()

    def polled(self) -> None:
        with self._lock:
            self._last_poll[threading.current_thread().name] = monotonic()

    def poll_failed(self, queue_name: str) -> None:
        with self._lock:
            self._poll_errors[queue_name] += 1

    def processed(self, queue_name: str, outcome: str) -> None:
        with self._lock:
            self._messages[queue_name, outcome] += 1

    @contextmanager
    def busy(self) -> Iterator[None]:
        name = threading.current_thread().name
        with self._lock:
            self._busy.add(name)
        try:
            yield
        finally:
            with self._lock:
                self._busy.discard(name)

    # ------------------------------------------------------------------ read

    @property
    def messages(self) -> dict[tuple[str, str], int]:
        """Messages handled, keyed by ``(queue_name, outcome)``."""
        with self._lock:
            return dict(self._messages)

    @property
    def poll_errors(self) -> dict[str, int]:
        """Failed receives, by queue."""
        with self._lock:
            return dict(self._poll_errors)

    @property
    def busy_threads(self) -> int:
        with self._lock:
            return len(self._busy)

    def last_poll_age(self) -> float | None:
        """Seconds since the least recent successful poll of an idle thread."""
        now = monotonic()
        with self._lock:
            ages = [now - t for name, t in self._last_poll.items() if name not in self._busy]
        return max(ages, default=None)

    def is_healthy(self, max_age: float) -> bool:
        age = self.last_poll_age()
        return age is None or age <= max_age


# ------------------------------------------------------------------ HTTP


def _label(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def render_metrics(stats: WorkerStats, max_age: float) -> str:
    """The stats in the Prometheus text exposition format."""
    lines = [
        "# HELP django_tasks_sqs_messages_total Messages handled, by queue and outcome.",
        "# TYPE django_tasks_sqs_messages_total counter",
    ]
    for (queue, outcome), count in sorted(stats.messages.items()):
        lines.append(
            f'django_tasks_sqs_messages_total{{queue="{_label(queue)}",outcome="{outcome}"}} '
            f"{count}"
        )
    lines += [
        "# HELP django_tasks_sqs_poll_errors_total Failed receives from SQS, by queue.",
        "# TYPE django_tasks_sqs_poll_errors_total counter",
    ]
    for queue, count in sorted(stats.poll_errors.items()):
        lines.append(f'django_tasks_sqs_poll_errors_total{{queue="{_label(queue)}"}} {count}')
    age = stats.last_poll_age()
    lines += [
        "# HELP django_tasks_sqs_busy_threads Polling threads currently running a task.",
        "# TYPE django_tasks_sqs_busy_threads gauge",
        f"django_tasks_sqs_busy_threads {stats.busy_threads}",
        "# HELP django_tasks_sqs_last_poll_age_seconds Seconds since the stalest idle "
        "thread last polled SQS.",
        "# TYPE django_tasks_sqs_last_poll_age_seconds gauge",
        f"django_tasks_sqs_last_poll_age_seconds {0.0 if age is None else round(age, 3)}",
        "# HELP django_tasks_sqs_healthy 1 if every polling thread is polling or working.",
        "# TYPE django_tasks_sqs_healthy gauge",
        f"django_tasks_sqs_healthy {int(stats.is_healthy(max_age))}",
    ]
    return "\n".join(lines) + "\n"


class HealthServer:
    """Serve ``/healthz`` (200 or 503) and ``/metrics`` (Prometheus) for a worker.

    Runs in a daemon thread. ``port=0`` picks a free port (see :attr:`port`).
    """

    def __init__(
        self, worker: Worker, *, host: str = "0.0.0.0", port: int, max_age: float = 60
    ) -> None:
        stats = worker.stats

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path == "/healthz":
                    healthy = stats.is_healthy(max_age)
                    self._reply(200 if healthy else 503, "ok\n" if healthy else "unhealthy\n")
                elif self.path == "/metrics":
                    self._reply(200, render_metrics(stats, max_age), "text/plain; version=0.0.4")
                else:
                    self._reply(404, "not found\n")

            def _reply(self, status: int, body: str, content_type: str = "text/plain") -> None:
                data = body.encode()
                self.send_response(status)
                self.send_header("Content-Type", f"{content_type}; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def log_message(self, format: str, *args: Any) -> None:
                logger.debug("health server: " + format, *args)

        self._server = ThreadingHTTPServer((host, port), Handler)
        self._server.daemon_threads = True
        self._thread = threading.Thread(
            target=self._server.serve_forever, name="sqs-health", daemon=True
        )

    @property
    def port(self) -> int:
        return self._server.server_address[1]

    def start(self) -> None:
        self._thread.start()
        logger.info("Health server listening on port %d", self.port)

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()
