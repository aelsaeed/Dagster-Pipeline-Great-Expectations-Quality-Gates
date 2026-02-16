# Contributing

Thanks for contributing to this repository.

## Development setup

From the repository root:

```bash
make setup
```

This installs the project in editable mode with dev dependencies and enables pre-commit hooks.

## Typical local workflow

```bash
make lint
make typecheck
make test
make demo
```

## Commit quality gates

Pre-commit hooks run Ruff and basic formatting checks on commit. You can run all hooks manually with:

```bash
pre-commit run --all-files
```

## Pull request expectations

- Keep PRs small and focused.
- Update docs/changelog when behavior changes.
- Include validation steps and relevant output.
- Use the PR template checklist before requesting review.
