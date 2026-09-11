"""Shared fixtures: keep the tests independent of the developer's Reachy / gateway env."""
from __future__ import annotations

import pytest

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
