"""The JSON envelope a task travels in through SQS."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

FORMAT_VERSION = 1


class InvalidMessage(ValueError):
    """The SQS message body is not a task envelope this library can read."""


@dataclass(frozen=True, slots=True, kw_only=True)
class TaskMessage:
    """Everything the worker needs to rebuild and run a task.

    Only the task's *path* travels, never code: the worker imports it. Arguments
    must be JSON-serialisable, which ``django.tasks`` already enforces.
    """

    id: str
    task_path: str
    args: list[Any]
    kwargs: dict[str, Any]
    queue_name: str
    backend: str
    enqueued_at: datetime
    run_after: datetime | None = None

    def to_json(self) -> str:
        return json.dumps(
            {
                "v": FORMAT_VERSION,
                "id": self.id,
                "task": self.task_path,
                "args": self.args,
                "kwargs": self.kwargs,
                "queue_name": self.queue_name,
                "backend": self.backend,
                "enqueued_at": self.enqueued_at.isoformat(),
                "run_after": self.run_after.isoformat() if self.run_after else None,
            },
            separators=(",", ":"),
        )

    @classmethod
    def from_json(cls, body: str) -> TaskMessage:
        try:
            data = json.loads(body)
            if data.get("v") != FORMAT_VERSION:
                raise InvalidMessage(f"unsupported message version: {data.get('v')!r}")
            return cls(
                id=str(data["id"]),
                task_path=str(data["task"]),
                args=list(data["args"]),
                kwargs=dict(data["kwargs"]),
                queue_name=str(data["queue_name"]),
                backend=str(data["backend"]),
                enqueued_at=datetime.fromisoformat(data["enqueued_at"]),
                run_after=(
                    datetime.fromisoformat(data["run_after"]) if data["run_after"] else None
                ),
            )
        except InvalidMessage:
            raise
        except (ValueError, KeyError, TypeError, AttributeError) as exc:
            raise InvalidMessage(f"malformed task message: {exc}") from exc
