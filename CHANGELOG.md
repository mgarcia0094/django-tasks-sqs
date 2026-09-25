# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/) and the project uses
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- `SQSBackend.enqueue_many()` / `aenqueue_many()` enqueue many tasks with
  `SendMessageBatch`, raising `EnqueueBatchError` on partial failures (#2).
- Priorities (#3): the `priority_levels` option maps priority ranges to SQS queues
  (`emails-high`, `emails`, `emails-low`…), and the worker polls them with weighted
  random order. Messages now carry `priority`; messages from 0.1.0 still parse.
- Worker health checks and metrics (#4): `sqs_worker --health-port` serves `/healthz`
  and Prometheus `/metrics`; `Worker.stats` exposes counters and liveness; the new
  `message_processed` signal carries queue, outcome and duration for other backends.

## [0.1.0] - 2026-09-25

First release.

- `SQSBackend` for `django.tasks`: enqueue on standard and FIFO queues, with queue URLs
  given explicitly or resolved from a name prefix.
- Deferred tasks with `run_after`, including delays beyond SQS's 15-minute limit.
- `sqs_worker` management command and `Worker` class. Features: long polling,
  per-queue concurrency, retries through SQS redelivery, optional exponential
  backoff, visibility heartbeat for long tasks, graceful shutdown on SIGTERM/SIGINT.
- Support for async tasks, `takes_context` and the `django.tasks` signals.
- System checks for unknown options and mismatched queues.
