"""Module-level tasks used by the test-suite (the worker imports them by path)."""

from __future__ import annotations

import time
from typing import Any

from django.tasks import TaskContext, task

calls: list[tuple[str, object]] = []


@task
def add(a: int, b: int) -> int:
    calls.append(("add", (a, b)))
    return a + b


@task(queue_name="emails")
def send_email(to: str, *, subject: str = "") -> None:
    calls.append(("send_email", (to, subject)))


@task
def explode() -> None:
    raise RuntimeError("boom")


@task(takes_context=True)
def with_context(context: TaskContext[..., Any]) -> int:
    calls.append(("with_context", context.attempt))
    return context.attempt


@task
async def async_add(a: int, b: int) -> int:
    calls.append(("async_add", (a, b)))
    return a + b


@task(queue_name="orders.fifo")
def place_order(order_id: int) -> None:
    calls.append(("place_order", order_id))


NOT_A_TASK = 42


@task
def nap(seconds: float) -> None:
    time.sleep(seconds)
    calls.append(("nap", seconds))
