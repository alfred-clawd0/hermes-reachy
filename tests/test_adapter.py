"""Tests for the Reachy platform adapter.

The adapter subclasses Hermes gateway base classes, so these tests are skipped when
hermes-agent is not importable (e.g. a bare CI without it). Run with:

    PYTHONPATH="$HOME/.hermes/hermes-agent" python -m pytest -q
"""
from __future__ import annotations

import pytest

adapter = pytest.importorskip(
    "hermes_reachy.adapter",
    reason="hermes-agent (gateway.*) not importable in this environment",
)


class _FakeCtx:
    def __init__(self):
        self.platforms = []

    def register_platform(self, **kwargs):
        self.platforms.append(kwargs)


def test_robot_id_from_path():
    f = adapter.ReachyAdapter._robot_id_from_path
    assert f("/robot/reachy") == "reachy"
    assert f("/robot/kitchen-bot?token=x") == "kitchen-bot"
    assert f("") == adapter.DEFAULT_ROBOT_ID
    assert f("/") == adapter.DEFAULT_ROBOT_ID


def test_tag_turn_origin():
    tagged = adapter.ReachyAdapter._tag_turn({"type": "say"}, "t1")
    assert tagged["turn_id"] == "t1" and tagged["origin"] == "turn"
    proactive = adapter.ReachyAdapter._tag_turn({"type": "say"}, None)
    assert proactive["turn_id"] is None and proactive["origin"] == "proactive"


def test_check_requirements_env_gate(monkeypatch):
    monkeypatch.delenv("REACHY_WS_PORT", raising=False)
    # websockets is a declared dep; the gate then hinges on the port being set.
    assert adapter.check_requirements() is False
    monkeypatch.setenv("REACHY_WS_PORT", "8770")
    assert adapter.check_requirements() is True


def test_env_enablement_seed(monkeypatch):
    monkeypatch.delenv("REACHY_WS_PORT", raising=False)
    assert adapter._env_enablement() is None
    monkeypatch.setenv("REACHY_WS_PORT", "8770")
    monkeypatch.setenv("REACHY_HOME_CHANNEL", "reachy")
    seed = adapter._env_enablement()
    assert seed["port"] == 8770
    assert seed["home_channel"]["chat_id"] == "reachy"


def test_register_platform_registers_reachy():
    ctx = _FakeCtx()
    adapter.register_platform(ctx)
    assert len(ctx.platforms) == 1
    p = ctx.platforms[0]
    assert p["name"] == "reachy"
    assert "REACHY_WS_PORT" in p["required_env"]
    assert callable(p["adapter_factory"])
    assert callable(p["check_fn"])


def test_no_private_markers_in_source():
    import pathlib

    src = pathlib.Path(adapter.__file__).read_text().lower()
    for marker in ("tars", "manfred", "192.168", "spiner", "gap-map"):
        assert marker not in src
