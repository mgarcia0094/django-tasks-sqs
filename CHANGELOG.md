# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/) and the project uses
[Semantic Versioning](https://semver.org/).

## [0.1.0] - Unreleased

First release.

- `SQSBackend` for `django.tasks`: enqueue on standard and FIFO queues, with queue URLs
  given explicitly or resolved from a name prefix.
- Deferred tasks with `run_after`, including delays beyond SQS's 15-minute limit.
- `sqs_worker` management command and `Worker` class. Features: long polling,
  per-queue concurrency, retries through SQS redelivery, optional exponential
  backoff, visibility heartbeat for long tasks, graceful shutdown on SIGTERM/SIGINT.
- Support for async tasks, `takes_context` and the `django.tasks` signals.
- System checks for unknown options and mismatched queues.
