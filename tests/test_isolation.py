"""The suite must never read the developer's real Hermes home (see tests/conftest.py)."""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import textwrap
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


_FAILING_TEST = '''
import os
import pathlib


def test_deliberately_fails():
    pathlib.Path(__file__).with_name("seen_home.txt").write_text(os.environ["HERMES_HOME"])
    assert "hermes-reachy-tests-" in os.environ["HERMES_HOME"]
    assert "HERMES_SENTINEL" not in os.environ and "XDG_CONFIG_HOME" not in os.environ
    raise AssertionError("deliberate failure")
'''

_DRIVER = '''
import os
import sys

import pytest

before = dict(os.environ)
rc = pytest.main([sys.argv[1], "-q", "-p", "no:cacheprovider"])
changed = sorted(k for k in before.keys() | os.environ.keys() if before.get(k) != os.environ.get(k))
print("RESULT", int(rc), changed)
'''


def test_env_and_home_are_restored_after_a_failing_session(tmp_path):
    """Run a session whose only test fails, in a subprocess: the sandbox must have been active
    during the test, and HOME / HERMES_* / XDG_* must be exactly restored afterwards."""
    project = tmp_path / "project"
    project.mkdir()
    shutil.copy(Path(__file__).with_name("conftest.py"), project / "conftest.py")
    (project / "test_failing.py").write_text(textwrap.dedent(_FAILING_TEST))
    real_home = tmp_path / "real-home"
    # PYTEST_* is this outer run's bookkeeping (the child's pytest sets and clears it itself).
    env = {k: v for k, v in os.environ.items() if not k.startswith(("HERMES_", "XDG_", "PYTEST_"))}
    env.update(
        HOME=str(real_home),
        HERMES_HOME=str(real_home / ".hermes"),
        HERMES_SENTINEL="kept",
        XDG_CONFIG_HOME=str(real_home / ".config"),
    )
    out = subprocess.run(
        [sys.executable, "-c", _DRIVER, str(project)],
        env=env, capture_output=True, text=True, timeout=120, check=False,
    )
    report = out.stdout + out.stderr
    assert "deliberate failure" in report, report
    assert "RESULT 1 []" in report, report  # the test failed; no variable left changed
    seen = Path((project / "seen_home.txt").read_text())
    assert not seen.exists()  # the throwaway home was removed
    assert not (real_home / ".hermes").exists()  # and the "real" one never touched
