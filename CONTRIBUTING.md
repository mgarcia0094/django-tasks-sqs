# Contributing to django-tasks-sqs

Thanks for taking the time to contribute! Bug reports, documentation fixes, tests and
features are all welcome, and you don't need an AWS account: the test-suite mocks SQS
with [moto](https://docs.getmoto.org/).

## Before you start

- **Small fixes** (typos, docs, obvious bugs): open a pull request directly.
- **Features or behaviour changes:** open an issue first, so we can agree on the
  approach before you spend time on it. The [roadmap](README.md#roadmap) lists ideas
  that are already welcome.
- Look for issues labelled
  [`good first issue`](https://github.com/mgarcia0094/django-tasks-sqs/labels/good%20first%20issue)
  if you're new to the project.

## Development setup

You need [uv](https://docs.astral.sh/uv/). Everything else is installed for you.

```bash
git clone https://github.com/<your-user>/django-tasks-sqs
cd django-tasks-sqs
uv sync
```

Run the same checks as CI before you push:

```bash
uv run pytest            # tests, with a 95% coverage floor (we're at 100%)
uv run ruff check .      # lint
uv run ruff format .     # format
uv run mypy              # strict type checking
```

Test another Django version with `uv run --with "django~=6.0.0" pytest`.

Optionally, install the git hooks so formatting happens on commit:

```bash
uvx pre-commit install
```

## Guidelines

- **Tests:** every change in behaviour needs a test. Tasks used by tests live in
  `tests/tasks.py`, because the worker imports tasks by module path.
- **Types:** the package ships `py.typed` and is checked with `mypy --strict`. Keep it
  that way.
- **Public API:** everything exported from `django_tasks_sqs` (`SQSBackend`, `Worker`,
  `WorkerOptions`), the `OPTIONS` keys, the `sqs_worker` flags and the message format
  are public. Changes there need a note in [CHANGELOG.md](CHANGELOG.md), and breaking
  ones need a discussion first.
- **Message format:** if you change the JSON envelope in `message.py`, bump
  `FORMAT_VERSION` and keep reading the previous version. Workers and web processes
  are often deployed at different times.
- **Docs:** update the README when you add options or change behaviour.

## Pull requests

1. Fork the repo and create a branch from `main`.
2. Make your change with tests and docs.
3. Add a line under "Unreleased" in [CHANGELOG.md](CHANGELOG.md).
4. Open the PR and fill in the template. CI must be green.

Maintainers aim to reply within a week. Don't worry about making it perfect: we can
iterate in review.

## Releasing (maintainers)

1. Update the version in `pyproject.toml` and move the "Unreleased" changelog entries
   under the new version.
2. Merge to `main`, then create a GitHub release tagged `vX.Y.Z`. The release workflow
   builds the package and publishes it to PyPI through trusted publishing.

By contributing, you agree that your contributions are licensed under the
[MIT License](LICENSE).
