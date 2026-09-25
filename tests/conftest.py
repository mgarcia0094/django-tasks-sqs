from __future__ import annotations

import os
from collections.abc import Iterator

import boto3
import pytest
from moto import mock_aws
from mypy_boto3_sqs import SQSClient

from django_tasks_sqs import SQSBackend
from tests import tasks


def sqs_backend(alias: str = "default") -> SQSBackend:
    from django.tasks import task_backends

    backend = task_backends[alias]
    assert isinstance(backend, SQSBackend)
    return backend


@pytest.fixture(autouse=True)
def _aws_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"):
        monkeypatch.setenv(key, "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "eu-west-1")
    assert os.environ["AWS_ACCESS_KEY_ID"] == "testing"


@pytest.fixture(autouse=True)
def _reset_calls() -> Iterator[None]:
    tasks.calls.clear()
    yield
    tasks.calls.clear()


@pytest.fixture
def sqs() -> Iterator[SQSClient]:
    """A mocked SQS with the queues from tests/settings.py."""
    from django.tasks import task_backends

    with mock_aws():
        client = boto3.client("sqs", region_name="eu-west-1")
        client.create_queue(QueueName="test-default", Attributes={"VisibilityTimeout": "30"})
        client.create_queue(QueueName="test-emails")
        client.create_queue(
            QueueName="test-orders.fifo",
            Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "false"},
        )
        # Fresh backend instances so no client/URL cache leaks between tests.
        task_backends._connections = type(task_backends._connections)()  # type: ignore[attr-defined]
        yield client
