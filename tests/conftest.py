"""Require adapter test collection in CI while keeping Hermes optional locally."""

import importlib
import os

import pytest


def pytest_sessionstart(session):
    """Check imports before collection, regardless of test filenames or skip style."""
    if os.environ.get("HERMES_REACHY_REQUIRE_GATEWAY") != "1":
        return
    for module in ("gateway", "hermes_reachy.adapter"):
        try:
            importlib.import_module(module)
        except Exception as exc:
            pytest.exit(
                f"HERMES_REACHY_REQUIRE_GATEWAY=1 requires {module} to import. "
                "Install the pinned Hermes checkout and gateway dependencies described "
                f"in CONTRIBUTING.md. {type(exc).__name__}: {exc}",
                returncode=2,
            )


@pytest.hookimpl(wrapper=True)
def pytest_make_collect_report(collector):
    report = yield
    if (
        os.environ.get("HERMES_REACHY_REQUIRE_GATEWAY") == "1"
        and collector.path.name == "test_adapter.py"
        and report.skipped
    ):
        reason = report.longrepr[2] if isinstance(report.longrepr, tuple) else report.longrepr
        report.outcome = "failed"
        report.longrepr = (
            "HERMES_REACHY_REQUIRE_GATEWAY=1 requires adapter tests to collect. "
            "Install the pinned Hermes checkout and gateway dependencies described "
            f"in CONTRIBUTING.md. Original collection skip: {reason}"
        )
    return report


# ── Hermetic Hermes home ───────────────────────────────────────────────────
# Hermes resolves its home — and loads <home>/.env, config.yaml, secret sources and
# profiles from it — at import time (gateway.run calls load_hermes_dotenv() on import).
# So the sandbox is installed in pytest_configure, which runs before pytest_sessionstart
# above and before collection imports any test module. Tests must never see the
# developer's real ~/.hermes: results would differ from CI and could pick up real
# allowlists or secrets. Imports stay local so the block above remains verbatim.
_SANDBOX = {}
_SANDBOXED_NAMES = frozenset(
    {"HOME", "TERMINAL_HOME_MODE", "_HERMES_GATEWAY", "XDG_CONFIG_HOME", "XDG_DATA_HOME", "XDG_STATE_HOME"}
)


def _sandboxed_var(name):
    """HOME/XDG dirs and every inherited HERMES_* setting, except this repo's HERMES_REACHY_* flags."""
    return name in _SANDBOXED_NAMES or (name.startswith("HERMES_") and not name.startswith("HERMES_REACHY_"))


def pytest_configure(config):
    import tempfile
    from pathlib import Path

    real_home = Path.home().resolve()
    root = Path(tempfile.mkdtemp(prefix="hermes-reachy-tests-")).resolve()
    home = root / "home"
    hermes_home = home / ".hermes"
    hermes_home.mkdir(parents=True)
    saved = {name: value for name, value in os.environ.items() if _sandboxed_var(name)}
    for name in saved:
        del os.environ[name]
    os.environ["HOME"] = str(home)
    os.environ["HERMES_HOME"] = str(hermes_home)
    _SANDBOX.update(root=root, home=home, hermes_home=hermes_home, real_home=real_home, saved=saved)


def pytest_unconfigure(config):
    import shutil

    if not _SANDBOX:
        return
    for name in [name for name in os.environ if _sandboxed_var(name)]:
        del os.environ[name]
    os.environ.update(_SANDBOX["saved"])
    shutil.rmtree(_SANDBOX["root"], ignore_errors=True)
    _SANDBOX.clear()


@pytest.fixture
def hermes_sandbox():
    """Paths of the session's throwaway home (see pytest_configure)."""
    return dict(_SANDBOX)


# ── Per-test Reachy / gateway env ──────────────────────────────────────────
_ENV = (
    "REACHY_WS_PORT",
    "REACHY_WS_HOST",
    "REACHY_WS_API_KEY",
    "REACHY_WS_API_KEY_FILE",
    "REACHY_ALLOWED_ROBOTS",
    "REACHY_ALLOW_ALL_ROBOTS",
    "REACHY_HOME_CHANNEL",
    "GATEWAY_ALLOWED_USERS",
    "GATEWAY_ALLOW_ALL_USERS",
)


@pytest.fixture(autouse=True)
def _clean_reachy_env(monkeypatch):
    for name in _ENV:
        monkeypatch.delenv(name, raising=False)


class _RegisteringCtx:
    """Stand-in PluginContext: builds the real PlatformEntry and registers it under the current
    profile scope, as ``PluginContext.register_platform`` does."""

    def __init__(self, registry_module):
        self._mod = registry_module
        self.scope = registry_module.platform_registry.current_scope_key()

    def register_platform(self, **kwargs):
        entry = self._mod.PlatformEntry(**kwargs)  # unknown kwargs would raise TypeError here
        self._mod.platform_registry.register(entry, scope=self.scope)


@pytest.fixture
def reachy_platform():
    """Register this plugin's platform with the real Hermes registry (so ``Platform("reachy")``
    resolves and the gateway's authz reads our allowlist env names). Needs hermes-agent."""
    registry = pytest.importorskip("gateway.platform_registry")
    from hermes_reachy.adapter import register_platform

    ctx = _RegisteringCtx(registry)
    register_platform(ctx)
    yield registry.platform_registry.get("reachy")
    registry.platform_registry.unregister("reachy", scope=ctx.scope)
