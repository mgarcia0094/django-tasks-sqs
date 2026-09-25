from __future__ import annotations

import json
from datetime import UTC, datetime

import pytest

from django_tasks_sqs.message import InvalidMessage, TaskMessage

NOW = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)


def make(**overrides: object) -> TaskMessage:
    fields: dict[str, object] = {
        "id": "abc",
        "task_path": "tests.tasks.add",
        "args": [1, 2],
        "kwargs": {"x": "y"},
        "queue_name": "default",
        "backend": "default",
        "enqueued_at": NOW,
        "run_after": None,
    }
    fields.update(overrides)
    return TaskMessage(**fields)  # type: ignore[arg-type]


@pytest.mark.parametrize("run_after", [None, datetime(2026, 1, 2, tzinfo=UTC)])
def test_roundtrip(run_after: datetime | None) -> None:
    message = make(run_after=run_after)
    assert TaskMessage.from_json(message.to_json()) == message


def test_body_is_compact_json() -> None:
    body = json.loads(make().to_json())
    assert body["v"] == 1
    assert body["task"] == "tests.tasks.add"


@pytest.mark.parametrize(
    "body",
    [
        "not json",
        "[]",
        json.dumps({"v": 1}),
        json.dumps(
            {
                "v": 1,
                "id": "x",
                "task": "t",
                "args": [],
                "kwargs": {},
                "queue_name": "q",
                "backend": "b",
                "enqueued_at": "nope",
                "run_after": None,
            }
        ),
    ],
)
def test_malformed_bodies(body: str) -> None:
    with pytest.raises(InvalidMessage, match="malformed"):
        TaskMessage.from_json(body)


def test_unknown_version() -> None:
    body = json.loads(make().to_json())
    body["v"] = 99
    with pytest.raises(InvalidMessage, match="version"):
        TaskMessage.from_json(json.dumps(body))
