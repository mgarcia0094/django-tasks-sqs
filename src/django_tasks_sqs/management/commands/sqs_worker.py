"""``manage.py sqs_worker``: run tasks enqueued with the SQS backend."""

from __future__ import annotations

import signal
from types import FrameType
from typing import Any

from django.core.management.base import BaseCommand, CommandParser
from django.tasks import DEFAULT_TASK_BACKEND_ALIAS

from django_tasks_sqs.health import HealthServer
from django_tasks_sqs.worker import Worker, WorkerOptions


class Command(BaseCommand):
    help = "Run a worker that executes django.tasks enqueued on Amazon SQS."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--backend",
            default=DEFAULT_TASK_BACKEND_ALIAS,
            help="Task backend alias from settings.TASKS (default: %(default)s).",
        )
        parser.add_argument(
            "--queue",
            action="append",
            dest="queues",
            help="Queue to consume. Repeat for several. Default: all of the backend's QUEUES.",
        )
        parser.add_argument(
            "--concurrency",
            type=int,
            default=1,
            help="Polling threads per queue (default: %(default)s).",
        )
        parser.add_argument("--wait-time", type=int, default=20, help="Long-poll seconds.")
        parser.add_argument("--max-messages", type=int, default=1, help="Messages per receive.")
        parser.add_argument(
            "--visibility-timeout", type=int, default=None, help="Override queue visibility."
        )
        parser.add_argument(
            "--retry-backoff",
            type=int,
            default=None,
            help="Base seconds for exponential backoff between failed attempts.",
        )
        parser.add_argument(
            "--no-heartbeat",
            action="store_true",
            help="Don't extend the visibility timeout of running tasks.",
        )
        parser.add_argument(
            "--health-port",
            type=int,
            default=None,
            help="Serve /healthz and /metrics (Prometheus) on this port. Off by default.",
        )
        parser.add_argument("--health-host", default="0.0.0.0", help="Address for --health-port.")
        parser.add_argument(
            "--health-max-age",
            type=float,
            default=60,
            help="Seconds an idle thread may go without polling SQS before /healthz "
            "reports unhealthy (default: %(default)s).",
        )

    def handle(self, *args: Any, **options: Any) -> None:
        worker = Worker(
            backend_alias=options["backend"],
            queue_names=options["queues"],
            concurrency=options["concurrency"],
            options=WorkerOptions(
                wait_time_seconds=options["wait_time"],
                max_messages=options["max_messages"],
                visibility_timeout=options["visibility_timeout"],
                heartbeat=not options["no_heartbeat"],
                retry_backoff=options["retry_backoff"],
            ),
        )

        def shutdown(signum: int, frame: FrameType | None) -> None:
            worker.stop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, shutdown)

        health = None
        if options["health_port"] is not None:
            health = HealthServer(
                worker,
                host=options["health_host"],
                port=options["health_port"],
                max_age=options["health_max_age"],
            )
            health.start()
        try:
            worker.run()
        finally:
            if health is not None:
                health.stop()
