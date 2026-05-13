# Contributing

Thanks for your interest in `holocron-python`. This SDK is pre-alpha scaffolding; expect rapid changes and a moving wire protocol while the [broker](https://github.com/jedi-knights/holocron) is still pre-alpha.

## Ground rules

- **One PR, one concern.** Use [Conventional Commits](https://www.conventionalcommits.org/) for the title and body. Mixed concerns must be split into separate branches.
- **Tests with the change.** Every behavioural change needs a test. Use `pytest`.
- **Lint, type-check, test before pushing.** CI runs all three; locally:
  ```bash
  uv run ruff check .
  uv run ruff format --check .
  uv run mypy
  uv run pytest
  ```
- **Update documentation.** Stale docs are worse than missing docs. If you change behaviour, update the README (and any module docstrings) in the same PR.

## Local setup

Requires Python 3.11+ and [`uv`](https://docs.astral.sh/uv/).

```bash
git clone https://github.com/jedi-knights/holocron-python.git
cd holocron-python
uv sync --all-groups
uv run pytest
```

## Wire protocol coupling

This SDK tracks the wire protocol defined in the broker repository. Protocol-breaking changes must land in lockstep with a broker release; reference the corresponding broker PR or commit in your description.

## Reporting issues

Open an issue at <https://github.com/jedi-knights/holocron-python/issues>. Include the broker version you are running against, the Python version, and a minimal reproduction.
