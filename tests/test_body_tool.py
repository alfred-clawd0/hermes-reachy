"""Tests for the reachy_body tool — no hermes-agent / gateway required."""
from __future__ import annotations

import json

import pytest

from hermes_reachy.body_tool import (
    REACHY_BODY_SCHEMA,
    handle_reachy_body,
    register_body_tool,
)


class _FakeCtx:
    def __init__(self):
        self.tools = []
        self.platforms = []

    def register_tool(self, **kwargs):
        self.tools.append(kwargs)

    def register_platform(self, **kwargs):
        self.platforms.append(kwargs)


def test_schema_shape():
    assert REACHY_BODY_SCHEMA["name"] == "reachy_body"
    params = REACHY_BODY_SCHEMA["parameters"]
    assert params["required"] == ["action"]
    assert set(params["properties"]["action"]["enum"]) == {
        "emote",
        "dance",
        "look",
        "stop",
        "head_tracking",
        "chirp",
    }
    # public: no persona / private markers leaked into agent-facing text
    blob = json.dumps(REACHY_BODY_SCHEMA).lower()
    for marker in ("tars", "manfred", "192.168", "spiner"):
        assert marker not in blob


def test_register_body_tool_registers_reachy_body():
    ctx = _FakeCtx()
    register_body_tool(ctx)
    assert len(ctx.tools) == 1
    tool = ctx.tools[0]
    assert tool["name"] == "reachy_body"
    assert tool["toolset"] == "reachy"
    assert tool["is_async"] is True
    assert callable(tool["handler"])


@pytest.mark.asyncio
async def test_handle_reachy_body_without_adapter_returns_error():
    # No platform running (and hermes-agent may be absent) -> graceful JSON error, no raise.
    out = await handle_reachy_body({"action": "emote", "emotion": "happy"})
    payload = json.loads(out)
    assert "error" in payload


@pytest.mark.asyncio
async def test_handle_reachy_body_missing_action():
    class _Adapter:
        _loop = None
        _robots = {"reachy": object()}

        async def call_robot_tool(self, robot_id, action, params):  # pragma: no cover - not reached
            return {"ok": True}

    import hermes_reachy.body_tool as bt

    # Inject a fake active adapter so we exercise the arg-validation path, not the resolver.
    orig = bt._resolve_active_adapter
    bt._resolve_active_adapter = lambda: _Adapter()
    try:
        out = await handle_reachy_body({"emotion": "happy"})  # no 'action'
    finally:
        bt._resolve_active_adapter = orig
    payload = json.loads(out)
    assert payload.get("error") == "missing 'action'"
