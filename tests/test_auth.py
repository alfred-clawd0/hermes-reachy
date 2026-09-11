"""Real-WebSocket tests for the authenticated hello handshake, and for how the Hermes
gateway authorizes the message sources it produces.

Skipped when hermes-agent (gateway.*) is not importable — see tests/test_adapter.py.
"""
from __future__ import annotations

import asyncio
import json
import logging
import socket
from types import SimpleNamespace

import pytest
from websockets.asyncio.client import connect as ws_connect

adapter = pytest.importorskip(
    "hermes_reachy.adapter",
    reason="hermes-agent (gateway.*) not importable in this environment",
)
pytestmark = pytest.mark.usefixtures("reachy_platform")

KEY = "correct-horse-battery-staple"
LOGGER = "hermes_reachy.adapter"


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture
async def serve():
    """Start real ReachyAdapters on ephemeral loopback ports; handle_message records events."""
    started = []

    async def _start(api_key: str = KEY, **extra):
        port = _free_port()
        cfg = adapter.PlatformConfig(
            enabled=True, extra={"host": "127.0.0.1", "port": port, "api_key": api_key, **extra}
        )
        reachy = adapter.ReachyAdapter(cfg)
        events = []

        async def _record(event):
            events.append(event)

        reachy.handle_message = _record
        assert await reachy.connect()
        started.append(reachy)
        return reachy, f"ws://127.0.0.1:{port}", events

    yield _start
    for reachy in started:
        await reachy.disconnect()


async def _hello(ws, **fields):
    """Send a hello; a field set to None is omitted from the frame."""
    frame = {"type": "hello", "robot_id": "reachy", "api_key": KEY, **fields}
    await ws.send(json.dumps({k: v for k, v in frame.items() if v is not None}))


async def _stt(ws, text, **fields):
    await ws.send(json.dumps({"type": "stt", "text": text, **fields}))


async def _until(predicate, timeout=5.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(0.01)


async def _close_of(ws):
    await asyncio.wait_for(ws.wait_closed(), timeout=5.0)
    return ws.close_code, ws.close_reason


# ── handshake ──────────────────────────────────────────────────────────────
async def test_valid_hello_dispatches_messages(serve):
    reachy, url, events = await serve()
    async with ws_connect(url) as ws:
        await _hello(ws)
        await _stt(ws, "hello robot", turn_id="t1")
        await _until(lambda: events)
    event = events[0]
    assert event.text == "hello robot"
    assert event.source.user_id == "reachy" and event.source.chat_id == "reachy"
    assert event.metadata["reachy_turn_id"] == "t1"
    await _until(lambda: not reachy._robots)


REJECTIONS = {
    "wrong-key": ({"type": "hello", "robot_id": "reachy", "api_key": "not-the-key"}, "authentication failed"),
    "non-ascii-wrong-key": ({"type": "hello", "robot_id": "reachy", "api_key": "ключ-🔑"}, "authentication failed"),
    "legacy-hello-without-key": ({"type": "hello", "robot_id": "reachy"}, "api_key required"),
    "empty-key": ({"type": "hello", "robot_id": "reachy", "api_key": ""}, "api_key required"),
    "numeric-key": ({"type": "hello", "robot_id": "reachy", "api_key": 12345}, "api_key required"),
    "list-key": ({"type": "hello", "robot_id": "reachy", "api_key": [KEY]}, "api_key required"),
    "stt-before-hello": ({"type": "stt", "text": "hi", "robot_id": "reachy", "api_key": KEY}, "hello required"),
    "non-json-first-frame": ("hello?", "hello required"),
    "non-string-robot-id": ({"type": "hello", "robot_id": 7, "api_key": KEY}, "invalid robot_id"),
}


@pytest.mark.parametrize("case", sorted(REJECTIONS))
async def test_bad_first_frame_closes_1008(case, serve, caplog):
    caplog.set_level(logging.INFO, logger=LOGGER)
    reachy, url, events = await serve()
    frame, reason = REJECTIONS[case]
    async with ws_connect(url) as ws:
        await ws.send(frame if isinstance(frame, str) else json.dumps(frame))
        assert await _close_of(ws) == (1008, reason)
    assert events == []
    assert reachy._robots == {}
    assert "auth rejected" in caplog.text
    # Never authenticated -> never "connected"/"disconnected"; and no key material in logs.
    assert "robot connected" not in caplog.text
    assert "robot disconnected" not in caplog.text
    assert KEY not in caplog.text and "not-the-key" not in caplog.text and "ключ" not in caplog.text


async def test_hello_timeout_closes_1008(serve, monkeypatch, caplog):
    monkeypatch.setattr(adapter, "HELLO_TIMEOUT_S", 0.2)
    caplog.set_level(logging.INFO, logger=LOGGER)
    reachy, url, events = await serve()
    async with ws_connect(url) as ws:
        assert await _close_of(ws) == (1008, "hello timeout")
    assert events == [] and reachy._robots == {}
    assert "auth rejected" in caplog.text
    assert "robot disconnected" not in caplog.text


async def test_non_ascii_key(serve):
    key = "clé-🔑-ключ"
    _, url, events = await serve(api_key=key)
    async with ws_connect(url) as ws:
        await _hello(ws, api_key=key)
        await _stt(ws, "bonjour")
        await _until(lambda: events)
    assert events[0].text == "bonjour"
    for wrong in ("cle-🔑-ключ", "ascii-only"):
        async with ws_connect(url) as ws:
            await _hello(ws, api_key=wrong)
            assert await _close_of(ws) == (1008, "authentication failed")


# ── robot allowlist ────────────────────────────────────────────────────────
async def test_allowlist_rejects_unlisted_robot(serve, monkeypatch, caplog):
    monkeypatch.setenv("REACHY_ALLOWED_ROBOTS", "kitchen")
    caplog.set_level(logging.INFO, logger=LOGGER)
    _, url, events = await serve()
    async with ws_connect(url) as ws:
        await _hello(ws)  # robot_id "reachy" is not on the list
        assert await _close_of(ws) == (1008, "robot not allowed")
    assert "not allowed" in caplog.text
    async with ws_connect(url + "/robot/kitchen") as ws:
        await _hello(ws, robot_id=None)  # no robot_id: taken from the URL path
        await _stt(ws, "hi")
        await _until(lambda: events)
    assert events[0].source.user_id == "kitchen"


async def test_allow_all_accepts_any_robot_but_still_needs_key(serve, monkeypatch):
    monkeypatch.setenv("REACHY_ALLOW_ALL_ROBOTS", "true")
    _, url, events = await serve()
    async with ws_connect(url) as ws:
        await _hello(ws, robot_id="dev-bot-7")
        await _stt(ws, "hi")
        await _until(lambda: events)
    assert events[0].source.user_id == "dev-bot-7"
    async with ws_connect(url) as ws:
        await _hello(ws, robot_id="dev-bot-7", api_key="nope")
        assert await _close_of(ws) == (1008, "authentication failed")


@pytest.mark.parametrize("raw", ["", "   ", " , "])
async def test_blank_allowlist_means_default_robot(raw, serve, monkeypatch):
    monkeypatch.setenv("REACHY_ALLOWED_ROBOTS", raw)
    _, url, events = await serve()
    async with ws_connect(url) as ws:
        await _hello(ws, robot_id="kitchen")
        assert await _close_of(ws) == (1008, "robot not allowed")
    async with ws_connect(url) as ws:
        await _hello(ws)
        await _stt(ws, "hi")
        await _until(lambda: events)
    assert events[0].source.user_id == "reachy"


# ── session identity ───────────────────────────────────────────────────────
async def test_robot_id_is_fixed_per_connection(serve):
    reachy, url, events = await serve(allowed_robots=["reachy", "kitchen"])
    async with ws_connect(url) as ws:
        await _hello(ws)
        await _stt(ws, "spoofed", robot_id="kitchen")  # dropped
        await _hello(ws, robot_id="kitchen")  # a second hello cannot rebind
        await _stt(ws, "genuine", robot_id="reachy")
        await _stt(ws, "no id")
        await _until(lambda: len(events) == 2)
        assert set(reachy._robots) == {"reachy"}
    assert [e.text for e in events] == ["genuine", "no id"]
    assert {e.source.user_id for e in events} == {"reachy"}


async def test_duplicate_robot_id_closes_previous_socket(serve):
    reachy, url, events = await serve()
    async with ws_connect(url) as old:
        await _hello(old)
        await _until(lambda: "reachy" in reachy._robots)
        old_server_side = reachy._robots["reachy"]
        async with ws_connect(url) as new:
            await _hello(new)
            assert await _close_of(old) == (1000, "superseded by a newer connection")
            assert reachy._robots["reachy"] is not old_server_side
            await _stt(new, "still here")
            await _until(lambda: events)
            result = await reachy.send("reachy", "outbound")
            assert result.success
            frame = json.loads(await asyncio.wait_for(new.recv(), timeout=5.0))
            assert frame["type"] == "say" and frame["content"] == "outbound"
            # The old connection's cleanup must not evict the new one.
            assert set(reachy._robots) == {"reachy"}
            assert reachy._robots["reachy"] is not old_server_side
    assert [e.text for e in events] == ["still here"]
    await _until(lambda: not reachy._robots)


# ── gateway authorization (second layer) ───────────────────────────────────
@pytest.fixture
def gateway_authorized(reachy_platform):
    """``authorized(reachy, source)`` -> the real gateway ``_is_user_authorized`` verdict, with
    this plugin registered in the real platform registry (conftest ``reachy_platform``)."""
    authz = pytest.importorskip("gateway.authz_mixin")
    assert reachy_platform.allowed_users_env == "REACHY_ALLOWED_ROBOTS"

    class _Runner(authz.GatewayAuthorizationMixin):
        def __init__(self, reachy, platform, pairing_store):
            self.adapters = {platform: reachy}
            self.config = None
            self.pairing_store = pairing_store

    def authorized(reachy, source, *, pairing_store=None, allow_adapter_delegation=True):
        runner = _Runner(reachy, source.platform, pairing_store)
        return runner._is_user_authorized(source, allow_adapter_delegation=allow_adapter_delegation)

    return authorized


def _source(reachy, robot_id):
    return reachy.build_source(
        chat_id=robot_id, chat_name=f"Reachy {robot_id}", chat_type="dm",
        user_id=robot_id, user_name=f"Reachy {robot_id}",
    )


async def test_gateway_authorizes_default_robot_end_to_end(serve, gateway_authorized):
    # REACHY_ALLOWED_ROBOTS / REACHY_ALLOW_ALL_ROBOTS / GATEWAY_* all unset (conftest).
    reachy, url, events = await serve()
    async with ws_connect(url) as ws:
        await _hello(ws)
        await _stt(ws, "hi")
        await _until(lambda: events)
    source = events[0].source
    assert gateway_authorized(reachy, source) is True
    # ...via the adapter's allowlist policy, not a blanket upstream bypass:
    assert gateway_authorized(reachy, source, allow_adapter_delegation=False) is False


def test_gateway_env_allowlist_allow_all_and_pairing_still_apply(monkeypatch, gateway_authorized):
    monkeypatch.setenv("REACHY_ALLOWED_ROBOTS", "kitchen")
    reachy = adapter.ReachyAdapter(adapter.PlatformConfig(enabled=True, extra={"port": 8770, "api_key": KEY}))
    assert gateway_authorized(reachy, _source(reachy, "kitchen")) is True
    assert gateway_authorized(reachy, _source(reachy, "reachy")) is False
    paired = SimpleNamespace(is_approved=lambda platform, user: (platform, user) == ("reachy", "paired-bot"))
    assert gateway_authorized(reachy, _source(reachy, "paired-bot"), pairing_store=paired) is True
    monkeypatch.setenv("REACHY_ALLOW_ALL_ROBOTS", "true")
    assert gateway_authorized(reachy, _source(reachy, "dev-bot")) is True


async def test_gateway_allowlist_wins_and_mismatch_is_logged(serve, monkeypatch, caplog, gateway_authorized):
    monkeypatch.setenv("GATEWAY_ALLOWED_USERS", "some-telegram-user")
    caplog.set_level(logging.WARNING, logger=LOGGER)
    reachy, _, _ = await serve()
    assert "GATEWAY_ALLOWED_USERS is set but REACHY_ALLOWED_ROBOTS is not" in caplog.text
    assert gateway_authorized(reachy, _source(reachy, "reachy")) is False
    monkeypatch.setenv("REACHY_ALLOWED_ROBOTS", "reachy")
    assert gateway_authorized(reachy, _source(reachy, "reachy")) is True
