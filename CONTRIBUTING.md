# Contributing

Thanks for contributing to this repository.

## Development setup

From the repository root:

```bash
make setup
```

This creates `dagster-gx-pipeline/.venv/` and installs the project in editable mode with its
development dependencies. Enable the repository's Git hooks once per clone:

```bash
dagster-gx-pipeline/.venv/bin/pre-commit install
```

## Typical local workflow

```bash
make lint
make format-check
make typecheck
make test
make definitions
make demo
make demo-failure
```

## Commit quality gates

Pre-commit hooks run Ruff and basic formatting checks on commit. You can run all hooks manually with:

```bash
pre-commit run --all-files
```

Run the complete local CI suite with `make check`.

## Pull request expectations

- Keep PRs small and focused.
- Update docs/changelog when behavior changes.
- Include validation steps and relevant output.
- Use the PR template checklist before requesting review.
