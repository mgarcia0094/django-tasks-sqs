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
- **Batch enqueue:** `backend.enqueue_many(...)` sends many tasks with
  `SendMessageBatch` (see below).
- **Health checks and metrics:** `--health-port` serves `/healthz` and Prometheus
  `/metrics`; the `message_processed` signal feeds any other metrics system.
- **FIFO queues** (queue names ending in `.fifo`).
- **Typed, with 100% test coverage.** Tested against Django 6.0 and 6.1 on Python 3.12–3.14.

### Enqueueing many tasks at once

`django.tasks` has no bulk API, so the backend adds one. `enqueue_many` takes
`(task, args, kwargs)` tuples and sends them with as few `SendMessageBatch` requests as
possible (up to 10 messages and 256 KiB per request, one queue per request):

```python
backend = send_welcome_email.get_backend()
results = backend.enqueue_many(
    (send_welcome_email, [], {"user_id": user.pk}) for user in new_users
)
```

Every call is validated before anything is sent, and the `TaskResult`s come back in
the same order. `task_enqueued` is sent for each task. If SQS rejects some messages, the
rest are still sent and `django_tasks_sqs.EnqueueBatchError` is raised: its
`enqueued` lists the tasks that went through, and `failed` pairs each rejected task
with SQS's error. `aenqueue_many` is the async version.

## How it works

```
 web process                        SQS queue                     sqs_worker
 ───────────                        ─────────                     ──────────
 task.enqueue(args)  ──JSON msg──▶  [ ... ]  ──long poll──▶  import task by path
                                                                   │
                                            delete ◀── success ────┤
                        visibility timeout expires ◀── failure ────┘ (retry / DLQ)
```

### 1. Enqueueing

`task.enqueue(*args, **kwargs)` runs in your web process. `SQSBackend`:

1. validates the task, as `django.tasks` requires: module-level function, JSON-serialisable
   arguments, a queue listed in `QUEUES`…;
2. resolves the SQS queue URL, either from `queue_urls` or by calling `GetQueueUrl` with
   `queue_name_prefix + queue_name` (the result is cached);
3. sends one message and returns a `TaskResult` with status `READY`.

The message body is a small, versioned JSON envelope. Only the task's **import path**
travels, never code or pickles:

```json
{"v": 1, "id": "…", "task": "myapp.tasks.send_welcome_email",
 "args": [], "kwargs": {"user_id": 42}, "queue_name": "default",
 "backend": "default", "enqueued_at": "2026-09-25T10:00:00+00:00", "run_after": null}
```

The task path is also sent as a message attribute (`task`), which is handy for
filtering and debugging in the AWS console.

### 2. Deferring (`run_after`)

SQS can delay a message for at most 15 minutes (`DelaySeconds`). For longer delays the
backend sends the message with the maximum delay. When a worker receives it before
`run_after`, it sends a new copy delayed again and deletes the original. This repeats
until the task is due, so a task can be deferred for any length of time.

### 3. Consuming

`manage.py sqs_worker` starts `--concurrency` threads per queue. Each thread loops:

1. **Long-polls** SQS (`ReceiveMessage` with `WaitTimeSeconds=20`), which is cheap
   when the queue is idle.
2. **Imports the task** by path. If the message is malformed or the task can't be
   imported, it is logged and left alone, so the redrive policy eventually moves it
   to the DLQ.
3. **Runs it** exactly like Django's `ImmediateBackend` does: it builds a `TaskResult`,
   sends `task_started`, calls the function (sync or async, with `TaskContext` if
   `takes_context=True`), sends `task_finished`, and closes stale DB connections
   before and after.
4. **Acknowledges or retries.** On success the message is **deleted**. On failure it is
   **kept**: SQS delivers it again when the visibility timeout expires, or after
   `--retry-backoff` seconds (doubling each attempt) if you set it.

While a task runs, a **heartbeat** thread calls `ChangeMessageVisibility` every half
timeout, so a slow task is never handed to a second worker.

### 4. Shutting down

`SIGTERM`/`SIGINT` sets a stop flag. Threads finish the task they are running, stop
polling and exit. A message that was received but not finished simply becomes visible
again, and another worker picks it up.

### Code map

| Module | What lives there |
|---|---|
| [`backend.py`](src/django_tasks_sqs/backend.py) | `SQSBackend`: settings, boto3 client, queue URL resolution, `enqueue`, system checks |
| [`message.py`](src/django_tasks_sqs/message.py) | `TaskMessage`: the JSON envelope and its validation |
| [`worker.py`](src/django_tasks_sqs/worker.py) | `Worker`: polling threads, execution, retries, heartbeat, deferral |
| [`health.py`](src/django_tasks_sqs/health.py) | `WorkerStats` (counters, liveness) and the `/healthz` + `/metrics` server |
| [`signals.py`](src/django_tasks_sqs/signals.py) | `message_processed` |
| [`management/commands/sqs_worker.py`](src/django_tasks_sqs/management/commands/sqs_worker.py) | CLI flags and signal handling |

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
| `--health-port` | off | Serve `/healthz` and `/metrics` on this port |
| `--health-host` | `0.0.0.0` | Address for `--health-port` |
| `--health-max-age` | 60 | Seconds an idle thread may go without polling before `/healthz` fails |

You can also run a worker from code with `django_tasks_sqs.Worker`.

## Health checks and metrics

```bash
python manage.py sqs_worker --health-port 8000
```

- **`GET /healthz`** returns `200 ok` or `503 unhealthy`. The worker is healthy when
  every polling thread either received from SQS successfully in the last
  `--health-max-age` seconds or is busy running a task. So a long task never fails the
  check, but a worker stuck retrying (expired credentials, no network) does. Point a
  Kubernetes liveness probe or an ECS health check (`curl -f`) at it.
- **`GET /metrics`** uses the Prometheus text format:

  | Metric | Type | |
  |---|---|---|
  | `django_tasks_sqs_messages_total{queue,outcome}` | counter | `succeeded`, `failed`, `deferred`, `invalid` |
  | `django_tasks_sqs_poll_errors_total{queue}` | counter | Failed `ReceiveMessage` calls |
  | `django_tasks_sqs_busy_threads` | gauge | Threads running a task |
  | `django_tasks_sqs_last_poll_age_seconds` | gauge | Seconds since the stalest idle thread polled |
  | `django_tasks_sqs_healthy` | gauge | 1 or 0, as `/healthz` |

For CloudWatch, StatsD or anything else, connect to the `message_processed` signal. It
is sent after every message with `worker`, `queue_name`, `outcome` and `duration`
(seconds):

```python
from django.dispatch import receiver
from django_tasks_sqs.signals import message_processed

@receiver(message_processed)
def record(sender, queue_name, outcome, duration, **kwargs):
    statsd.timing(f"tasks.{queue_name}.{outcome}", duration * 1000)
```

When running a `Worker` from code, the same data is in `worker.stats`, and
`django_tasks_sqs.health.HealthServer(worker, port=...)` serves the endpoints.

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

## Roadmap

Ideas where help is very welcome. Open an issue to discuss before starting something big:

- [ ] Optional result storage (e.g. in the Django database), so `get_result()` works
- [x] Batch sends (`SendMessageBatch`) for enqueueing many tasks at once
- [ ] Priorities emulated with several queues and weighted polling
- [x] Health check and metrics hooks for the worker (Prometheus / CloudWatch)
- [ ] Payloads over 256 KB stored in S3 (extended client pattern)
- [x] Integration tests against LocalStack in CI

## Contributing

Contributions of any size are welcome: bug reports, docs, tests, features. Start with
[CONTRIBUTING.md](CONTRIBUTING.md). In short:

```bash
git clone https://github.com/mgarcia0094/django-tasks-sqs && cd django-tasks-sqs
uv sync              # install with dev dependencies
uv run pytest        # tests (SQS is mocked with moto, no AWS account needed)
uv run ruff check .  # lint
uv run mypy          # strict type checking
```

Please follow the [Code of Conduct](CODE_OF_CONDUCT.md). To report a security issue,
see [SECURITY.md](SECURITY.md).

## License

[MIT](LICENSE)
