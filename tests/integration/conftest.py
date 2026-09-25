"""Run the worker against a real SQS API (LocalStack).

Skipped unless ``LOCALSTACK_ENDPOINT`` is set, e.g.::

    docker run --rm -d -p 4566:4566 localstack/localstack
    LOCALSTACK_ENDPOINT=http://localhost:4566 uv run pytest -m integration --no-cov
"""

from __future__ import annotations

import os
from collections.abc import Iterator
from typing import Any

import boto3
import pytest
from django.utils.crypto import get_random_string
from mypy_boto3_sqs import SQSClient

ENDPOINT = os.environ.get("LOCALSTACK_ENDPOINT")
REGION = "eu-west-1"


def pytest_collection_modifyitems(items: list[pytest.Item]) -> None:
    here = os.path.dirname(__file__)
    for item in items:
        if str(item.path).startswith(here):
            item.add_marker(pytest.mark.integration)
            if not ENDPOINT:
                item.add_marker(pytest.mark.skip(reason="LOCALSTACK_ENDPOINT is not set"))


@pytest.fixture
def sqs() -> SQSClient:
    assert ENDPOINT
    return boto3.client("sqs", region_name=REGION, endpoint_url=ENDPOINT)


@pytest.fixture
def prefix(sqs: SQSClient, settings: Any) -> Iterator[str]:
    """Fresh queues for each test, wired into ``settings.TASKS``."""
    prefix = f"it-{get_random_string(8).lower()}-"
    urls = [
        sqs.create_queue(QueueName=f"{prefix}default", Attributes={"VisibilityTimeout": "30"})[
            "QueueUrl"
        ],
        sqs.create_queue(QueueName=f"{prefix}emails")["QueueUrl"],
        sqs.create_queue(
            QueueName=f"{prefix}orders.fifo",
            Attributes={"FifoQueue": "true", "ContentBasedDeduplication": "false"},
        )["QueueUrl"],
    ]
    settings.TASKS = {
        "default": {
            "BACKEND": "django_tasks_sqs.SQSBackend",
            "QUEUES": ["default", "emails", "orders.fifo"],
            "OPTIONS": {
                "queue_name_prefix": prefix,
                "region_name": REGION,
                "endpoint_url": ENDPOINT,
            },
        }
    }
    yield prefix
    for url in urls:
        sqs.delete_queue(QueueUrl=url)
