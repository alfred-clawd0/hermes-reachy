"""The suite must never read the developer's real Hermes home (see tests/conftest.py)."""
from __future__ import annotations

import os
from pathlib import Path

import pytest


def _inside(path, root) -> bool:
    return Path(path).expanduser().resolve().is_relative_to(root)


def test_home_and_hermes_home_point_into_the_sandbox(hermes_sandbox):
    root = hermes_sandbox["root"]
    assert _inside(os.environ["HOME"], root)
    assert _inside(os.environ["HERMES_HOME"], root)
    assert _inside(Path.home(), root)
    assert _inside("~/.hermes", root)
    assert not _inside(hermes_sandbox["real_home"], root)  # the real home really is elsewhere


def test_hermes_loaded_its_env_and_config_from_the_sandbox(hermes_sandbox):
    constants = pytest.importorskip("hermes_constants")
    run = pytest.importorskip("gateway.run")
    root = hermes_sandbox["root"]
    assert _inside(constants.get_hermes_home(), root)
    # gateway.run resolves this at import and load_hermes_dotenv() reads <home>/.env (and the
    # config.yaml-driven secret sources) from it: it must be the empty sandbox, not ~/.hermes.
    assert _inside(run._hermes_home, root)
    assert not (Path(run._hermes_home) / ".env").exists()
    assert not (Path(run._hermes_home) / "config.yaml").exists()
