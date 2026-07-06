# Contributing to hermes-reachy

Thanks for helping embody Hermes agents in Reachy Mini robots.

## Development

```bash
pip install -e ".[dev]"
# The plugin imports Hermes gateway base classes, so run tests with hermes-agent importable:
PYTHONPATH="$HOME/.hermes/hermes-agent" python -m pytest -q
python -m ruff check --isolated src tests
```

## Rules

- Keep it a **standalone plugin**: import Hermes base classes, never patch the core. If you need a
  capability the framework doesn't expose, raise it upstream as a new generic `ctx`/hook — don't
  special-case this plugin in core.
- No hardcoded hosts, ports, robot ids, credentials, or personal data — everything via env.
- Body actions stay advisory: the robot app owns the allowlist and bounded execution. Don't add
  raw motor control here.
- Add or update tests for behavior changes.

## Pull requests

Small and reviewable; conventional commit subjects (`feat:`, `fix:`, `docs:`). Open issues and PRs
on the GitHub repository.
