# django-tasks-sqs

[![CI](https://github.com/mgarcia0094/django-tasks-sqs/actions/workflows/ci.yml/badge.svg)](https://github.com/mgarcia0094/django-tasks-sqs/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/django-tasks-sqs)](https://pypi.org/project/django-tasks-sqs/)
[![Python](https://img.shields.io/pypi/pyversions/django-tasks-sqs)](https://pypi.org/project/django-tasks-sqs/)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

An **Amazon SQS backend and worker** for Django's built-in
[tasks framework](https://docs.djangoproject.com/en/stable/topics/tasks/) (`django.tasks`, Django 6.0+).

Django defines how you declare and enqueue background tasks, but ships no production
backend and no worker. This package provides both, on top of SQS. Your code only uses
the standard `django.tasks` API, so you can switch backends later without touching it.

```python
from django.tasks import task

@task
def send_welcome_email(user_id: int) -> None:
    ...

send_welcome_email.enqueue(user_id=42)   # returns immediately; a worker runs it
```

## Install

```bash
pip install django-tasks-sqs
```

```python
# settings.py
INSTALLED_APPS = [..., "django_tasks_sqs"]

TASKS = {
    "default": {
        "BACKEND": "django_tasks_sqs.SQSBackend",
        "QUEUES": ["default", "emails"],
        "OPTIONS": {
            "region_name": "eu-west-1",
            # SQS queue = prefix + django queue name ("myapp-default", "myapp-emails")...
            "queue_name_prefix": "myapp-",
            # ...or map queue names to URLs explicitly:
            # "queue_urls": {"default": "https://sqs.eu-west-1.amazonaws.com/123456789012/app"},
        },
    }
}
```

Then run one or more workers:

```bash
python manage.py sqs_worker                       # all queues of the "default" backend
python manage.py sqs_worker --queue emails --concurrency 4
```

The queues must already exist. Create them with your usual infrastructure tooling
(Terraform, CDK, CloudFormation…).

## Features

- **Standard `django.tasks` API:** `@task`, `.enqueue()`, `.aenqueue()`, `.using()`,
  `takes_context`, async tasks, and the `task_enqueued` / `task_started` /
  `task_finished` signals.
- **Deferred tasks:** `task.using(run_after=...)`. Delays up to 15 minutes use SQS
  `DelaySeconds`. Longer delays are re-queued by the worker until they are due.
- **Retries and dead-letter queues:** a failed task is not deleted, so SQS delivers it
  again. Add a redrive policy to the queue to move it to a DLQ after `maxReceiveCount`
  attempts. `--retry-backoff N` adds exponential backoff between attempts.
- **Long-running tasks:** a heartbeat extends the message's visibility timeout while
  the task runs, so another worker doesn't pick it up halfway through.
- **Graceful shutdown:** on `SIGTERM`/`SIGINT` the worker finishes its current tasks and
  exits. Plays well with ECS, Kubernetes and systemd.
- **FIFO queues** (queue names ending in `.fifo`).
- **Typed, with 100% test coverage.** Tested against Django 6.0 and 6.1 on Python 3.12–3.14.

## How it works

```
 web process                        SQS queue                     sqs_worker
 ───────────                        ─────────                     ──────────
 task.enqueue(args)  ──JSON msg──▶  [ ... ]  ──long poll──▶  import task by path
                                                                   │
                                            delete ◀── success ────┤
                        visibility timeout expires ◀── failure ────┘ (retry / DLQ)
```

The message carries the task's import path, its JSON arguments and some metadata. No
code or pickles travel through the queue.

## Worker options

| Flag | Default | |
|---|---|---|
| `--backend` | `default` | Alias in `settings.TASKS` |
| `--queue` | all `QUEUES` | Repeat to consume several |
| `--concurrency` | 1 | Polling threads per queue, each running one task at a time |
| `--wait-time` | 20 | Long-poll seconds (max 20) |
| `--max-messages` | 1 | Messages per receive (1–10). Keep it low for slow tasks |
| `--visibility-timeout` | queue's | Override for received messages |
| `--retry-backoff` | off | Base seconds for exponential backoff: `base * 2**(attempt-1)` |
| `--no-heartbeat` | | Don't extend visibility while tasks run |

You can also run a worker from code with `django_tasks_sqs.Worker`.

## Things to know

- **Delivery is at least once.** That is how SQS works: a task can run more than once,
  for example if a worker dies after running it but before deleting the message. Make
  tasks idempotent.
- **No result storage (yet).** `supports_get_result = False`, so `task.get_result(id)`
  raises `NotImplementedError`. Store results yourself if you need them.
- **No priorities.** SQS has none. Use separate queues and give the important ones more
  workers.
- **FIFO queues don't support `run_after`,** because SQS has no per-message delay on
  FIFO queues.
- **Messages are limited to 256 KB.** Pass IDs, not big payloads.
- **`TaskContext.attempt`** comes from SQS's `ApproximateReceiveCount`.
- **IAM permissions:** the web process needs `sqs:SendMessage` and `sqs:GetQueueUrl`.
  Workers also need `sqs:ReceiveMessage`, `sqs:DeleteMessage`,
  `sqs:ChangeMessageVisibility` and `sqs:GetQueueAttributes`.

## Local development

Point `endpoint_url` at [LocalStack](https://www.localstack.cloud/) or
[moto](https://docs.getmoto.org/) in server mode:

```python
"OPTIONS": {"endpoint_url": "http://localhost:4566", "region_name": "us-east-1", ...}
```

For unit tests, use Django's `ImmediateBackend` instead, which runs tasks inline.

## Contributing

```bash
uv sync              # install with dev dependencies
uv run pytest        # tests (SQS is mocked with moto)
uv run ruff check .  # lint
uv run mypy          # strict type checking
```

## License

[MIT](LICENSE)
