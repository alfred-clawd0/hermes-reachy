"""Real-WebSocket tests for the authenticated hello handshake, and for how the Hermes
gateway authorizes the message sources it produces.

Skipped when hermes-agent (gateway.*) is not importable — see tests/test_adapter.py.
"""
from __future__ import annotations

import asyncio
import json
import logging
import logging.config
import os
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
# The test client's own websockets logger would dump the frames it sends (keys included) at
# DEBUG; hold it at WARNING so log assertions only see the adapter side.
_CLIENT_LOGGER = logging.getLogger("tests.reachy.client")
_CLIENT_LOGGER.setLevel(logging.WARNING)


def _client(url, **kwargs):
    return ws_connect(url, logger=_CLIENT_LOGGER, **kwargs)


@pytest.fixture
async def serve():
    """Start real ReachyAdapters on ephemeral loopback ports; handle_message records events."""
    started = []

    async def _start(api_key: str = KEY, **extra):
        cfg = adapter.PlatformConfig(
            enabled=True, extra={"host": "127.0.0.1", "port": 8770, "api_key": api_key, **extra}
        )
        reachy = adapter.ReachyAdapter(cfg)
        reachy._port = 0  # let the OS pick while binding; read the real port back (no race)
        events = []

        async def _record(event):
            events.append(event)

        reachy.handle_message = _record
        assert await reachy.connect()
        started.append(reachy)
        port = next(iter(reachy._server.sockets)).getsockname()[1]
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
    """(code, reason) of the close frame the server sent. Read off the protocol, since
    ClientConnection.close_code / close_reason only exist from websockets 14."""
    await asyncio.wait_for(ws.wait_closed(), timeout=5.0)
    rcvd = ws.protocol.close_rcvd
    return (rcvd.code, rcvd.reason) if rcvd is not None else (None, None)


# ── handshake ──────────────────────────────────────────────────────────────
async def test_valid_hello_dispatches_messages(serve):
    reachy, url, events = await serve()
    async with _client(url) as ws:
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
    "non-object-first-frame": ('["hello"]', "hello required"),
    "non-string-robot-id": ({"type": "hello", "robot_id": 7, "api_key": KEY}, "invalid robot_id"),
    "malformed-robot-id": ({"type": "hello", "robot_id": "robot one", "api_key": KEY}, "invalid robot_id"),
}


@pytest.mark.parametrize("case", sorted(REJECTIONS))
async def test_bad_first_frame_closes_1008(case, serve, caplog):
    caplog.set_level(logging.INFO, logger=LOGGER)
    reachy, url, events = await serve()
    frame, reason = REJECTIONS[case]
    async with _client(url) as ws:
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
    async with _client(url) as ws:
        assert await _close_of(ws) == (1008, "hello timeout")
    assert events == [] and reachy._robots == {}
    assert "auth rejected" in caplog.text
    assert "robot disconnected" not in caplog.text


@pytest.mark.parametrize("compression", ["deflate", None])
async def test_oversized_hello_closes_1008(compression, serve, caplog):
    caplog.set_level(logging.INFO, logger=LOGGER)
    reachy, url, events = await serve()
    padding = os.urandom(adapter.HELLO_MAX_BYTES).hex()  # 2x the pre-auth budget
    async with _client(url, compression=compression) as ws:
        await _hello(ws, pad=padding)
        assert await _close_of(ws) == (1008, "frame too large")
    # The protocol closes the socket from inside its parser; the handler notices (and logs)
    # right after, so wait for it rather than racing the client's view of the close.
    await _until(lambda: reachy._pending == 0 and "frame too large" in caplog.text)
    assert events == [] and reachy._robots == {}
    assert "auth rejected" in caplog.text
    assert "robot connected" not in caplog.text and "robot disconnected" not in caplog.text


async def test_frames_after_the_hello_may_exceed_the_hello_budget(serve):
    reachy, url, events = await serve()
    long_text = "a" * (adapter.HELLO_MAX_BYTES * 4)
    async with _client(url, compression=None) as ws:
        await _hello(ws)
        await _until(lambda: "reachy" in reachy._robots)  # authenticated -> limit raised
        await _stt(ws, long_text)
        await _until(lambda: events)
    assert events[0].text == long_text


async def test_non_ascii_key(serve):
    key = "clé-🔑-ключ"
    _, url, events = await serve(api_key=key)
    async with _client(url) as ws:
        await _hello(ws, api_key=key)
        await _stt(ws, "bonjour")
        await _until(lambda: events)
    assert events[0].text == "bonjour"
    for wrong in ("cle-🔑-ключ", "ascii-only"):
        async with _client(url) as ws:
            await _hello(ws, api_key=wrong)
            assert await _close_of(ws) == (1008, "authentication failed")


async def test_pending_connection_cap(serve, monkeypatch, caplog):
    monkeypatch.setattr(adapter, "MAX_PENDING_CONNECTIONS", 2)
    caplog.set_level(logging.INFO, logger=LOGGER)
    reachy, url, events = await serve(allowed_robots=["reachy", "kitchen"])
    async with _client(url) as first, _client(url) as _second:
        await _until(lambda: reachy._pending == 2)  # both sit in the hello phase
        for _ in range(3):
            async with _client(url) as extra:
                assert await _close_of(extra) == (1008, "too many pending connections")
        overflow_logs = [r for r in caplog.records if "too many pending connections" in r.getMessage()]
        assert len(overflow_logs) == 1  # rate-limited: one warning for three refusals
        await _hello(first)
        await _until(lambda: reachy._pending == 1)
        async with _client(url) as late:  # a slot freed up
            await _hello(late, robot_id="kitchen")
            await _stt(late, "hi")
            await _until(lambda: events)
    assert events[0].source.user_id == "kitchen"
    await _until(lambda: reachy._pending == 0)


# ── robot allowlist ────────────────────────────────────────────────────────
async def test_allowlist_rejects_unlisted_robot(serve, monkeypatch, caplog):
    monkeypatch.setenv("REACHY_ALLOWED_ROBOTS", "kitchen")
    caplog.set_level(logging.INFO, logger=LOGGER)
    _, url, events = await serve()
    async with _client(url) as ws:
        await _hello(ws)  # robot_id "reachy" is not on the list
        assert await _close_of(ws) == (1008, "robot not allowed")
    assert "robot_id not allowlisted" in caplog.text
    async with _client(url + "/robot/kitchen") as ws:
        await _hello(ws, robot_id=None)  # no robot_id: taken from the URL path
        await _stt(ws, "hi")
        await _until(lambda: events)
    assert events[0].source.user_id == "kitchen"


async def test_allow_all_accepts_any_robot_but_still_needs_key(serve, monkeypatch):
    monkeypatch.setenv("REACHY_ALLOW_ALL_ROBOTS", "true")
    _, url, events = await serve()
    async with _client(url) as ws:
        await _hello(ws, robot_id="dev-bot-7")
        await _stt(ws, "hi")
        await _until(lambda: events)
    assert events[0].source.user_id == "dev-bot-7"
    async with _client(url) as ws:
        await _hello(ws, robot_id="dev-bot-7", api_key="nope")
        assert await _close_of(ws) == (1008, "authentication failed")


@pytest.mark.parametrize("raw", ["", "   ", " , "])
async def test_blank_allowlist_means_default_robot(raw, serve, monkeypatch):
    monkeypatch.setenv("REACHY_ALLOWED_ROBOTS", raw)
    _, url, events = await serve()
    async with _client(url) as ws:
        await _hello(ws, robot_id="kitchen")
        assert await _close_of(ws) == (1008, "robot not allowed")
    async with _client(url) as ws:
        await _hello(ws)
        await _stt(ws, "hi")
        await _until(lambda: events)
    assert events[0].source.user_id == "reachy"


# ── session identity ───────────────────────────────────────────────────────
async def test_robot_id_is_fixed_per_connection(serve):
    reachy, url, events = await serve(allowed_robots=["reachy", "kitchen"])
    async with _client(url) as ws:
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
    async with _client(url) as old:
        await _hello(old)
        await _until(lambda: "reachy" in reachy._robots)
        old_server_side = reachy._robots["reachy"]
        async with _client(url) as new:
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


async def test_tool_result_from_another_robot_is_ignored(serve, caplog):
    caplog.set_level(logging.INFO, logger=LOGGER)
    reachy, url, _ = await serve(allowed_robots=["reachy", "kitchen"])
    async with _client(url) as owner, _client(url) as other:
        await _hello(owner)
        await _hello(other, robot_id="kitchen")
        await _until(lambda: set(reachy._robots) == {"reachy", "kitchen"})
        call = asyncio.create_task(reachy.call_robot_tool("reachy", "emote", {"emotion": "happy"}, timeout_s=5))
        request = json.loads(await asyncio.wait_for(owner.recv(), timeout=5.0))
        assert request["type"] == "tool_call"

        def result(payload):
            return json.dumps({"type": "tool_result", "tool_call_id": request["tool_call_id"], "result": payload})

        await other.send(result({"ok": "spoofed"}))
        await _until(lambda: "ignored tool_result from kitchen for a call sent to reachy" in caplog.text)
        assert not call.done()
        await owner.send(result({"ok": True}))
        assert await asyncio.wait_for(call, timeout=5.0) == {"ok": True}


# ── logging hygiene ────────────────────────────────────────────────────────
SECRET = "review-secret-5f3a9c"


async def test_frame_contents_and_keys_are_never_logged(serve, caplog):
    caplog.set_level(logging.DEBUG)  # root: every logger at DEBUG
    caplog.set_level(logging.DEBUG, logger=LOGGER)
    reachy, url, events = await serve(api_key=SECRET)
    pre_auth_probes = [
        '{"type":"hello","robot_id":"reachy","api_key":"' + SECRET,  # truncated JSON
        json.dumps([SECRET]),  # not an object
        json.dumps({"type": SECRET, "api_key": SECRET}),  # not a hello
        json.dumps({"type": "hello", "robot_id": "reachy", "api_key": SECRET + "-wrong"}),
        json.dumps({"type": "hello", "robot_id": SECRET + " x", "api_key": SECRET}),  # malformed id
        json.dumps({"type": "hello", "robot_id": SECRET, "api_key": SECRET}),  # authenticated, id == key
        json.dumps({"type": "hello", "robot_id": "not-listed", "api_key": SECRET}),  # not allowlisted
        json.dumps({"type": "hello", "api_key": SECRET, "pad": SECRET * 400}),  # oversized
    ]
    for probe in pre_auth_probes:
        async with _client(f"{url}/robot/reachy?api_key={SECRET}") as ws:
            await ws.send(probe)
            code, _ = await _close_of(ws)
            assert code == 1008
    await _until(lambda: sum("auth rejected" in r.getMessage() for r in caplog.records) == len(pre_auth_probes))
    async with _client(f"{url}/robot/reachy?token={SECRET}") as ws:
        await _hello(ws, api_key=SECRET)
        for frame in (
            '{"type":"stt","text":"' + SECRET,  # truncated JSON
            json.dumps([SECRET]),
            json.dumps({"type": SECRET}),
            json.dumps({"type": "stt", "text": "hi", "robot_id": SECRET}),  # claims another id
            json.dumps({"type": "tool_result", "tool_call_id": SECRET, "result": {"k": SECRET}}),
            json.dumps({"type": "stt", "text": SECRET}),
        ):
            await ws.send(frame)
        await _until(lambda: events)  # the last frame was dispatched, so all were processed
        await ws.close(reason=SECRET)
    await _until(lambda: not reachy._robots)
    messages = [r.getMessage() for r in caplog.records]
    assert sum("auth rejected" in m for m in messages) == len(pre_auth_probes)
    assert any("robot connected" in m for m in messages)
    for record in caplog.records:
        rendered = record.getMessage()
        if record.exc_info:
            rendered += logging.Formatter().formatException(record.exc_info)
        assert SECRET not in rendered, (record.name, record.levelname, rendered[:200])
    assert SECRET not in caplog.text


# ── gateway authorization (second layer) ───────────────────────────────────
@pytest.fixture
def gateway_runner(reachy_platform):
    """A minimal gateway runner: the real ``GatewayAuthorizationMixin`` over this adapter, with
    this plugin registered in the real platform registry (conftest ``reachy_platform``)."""
    authz = pytest.importorskip("gateway.authz_mixin")
    assert reachy_platform.allowed_users_env == "REACHY_ALLOWED_ROBOTS"

    class _Runner(authz.GatewayAuthorizationMixin):
        def __init__(self, reachy, platform, pairing_store=None):
            self.adapters = {platform: reachy}
            self.config = None
            self.pairing_store = pairing_store

    return _Runner


@pytest.fixture
def gateway_authorized(gateway_runner):
    """``authorized(reachy, source)`` -> the real gateway ``_is_user_authorized`` verdict."""

    def authorized(reachy, source, *, pairing_store=None, allow_adapter_delegation=True):
        runner = gateway_runner(reachy, source.platform, pairing_store)
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
    async with _client(url) as ws:
        await _hello(ws)
        await _stt(ws, "hi")
        await _until(lambda: events)
    source = events[0].source
    assert gateway_authorized(reachy, source) is True
    # ...via the adapter's allowlist policy, not a blanket upstream bypass:
    assert gateway_authorized(reachy, source, allow_adapter_delegation=False) is False


@pytest.mark.parametrize("allowed", [None, "reachy"])
def test_unauthorized_robot_dms_are_ignored_not_answered(allowed, monkeypatch, gateway_runner):
    # The gateway would otherwise answer an unauthorized DM with a pairing code — which a robot
    # would speak aloud.
    if allowed:
        monkeypatch.setenv("REACHY_ALLOWED_ROBOTS", allowed)
    reachy = adapter.ReachyAdapter(adapter.PlatformConfig(enabled=True, extra={"port": 8770, "api_key": KEY}))
    platform = _source(reachy, "reachy").platform
    assert gateway_runner(reachy, platform)._get_unauthorized_dm_behavior(platform) == "ignore"


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



async def test_robot_ids_containing_the_key_are_rejected_unlogged(serve, monkeypatch, caplog):
    monkeypatch.setenv("REACHY_ALLOW_ALL_ROBOTS", "true")  # robot ids are client-chosen here
    caplog.set_level(logging.DEBUG)
    caplog.set_level(logging.DEBUG, logger=LOGGER)
    reachy, url, events = await serve(api_key=SECRET)
    for robot_id in (SECRET, f"bot-{SECRET}"):
        async with _client(url) as ws:
            await _hello(ws, robot_id=robot_id, api_key=SECRET)
            assert await _close_of(ws) == (1008, "invalid robot_id")
    assert events == [] and reachy._robots == {}
    assert "robot_id contains the API key" in caplog.text
    assert SECRET not in caplog.text


# ── logging reconfigured after start-up ────────────────────────────────────
@pytest.fixture
def restore_logging():
    """Snapshot, and afterwards restore, the global logging configuration a test rewires."""
    root = logging.getLogger()
    saved_root = (root.level, list(root.handlers), list(root.filters))
    saved = {
        name: (lg.level, list(lg.handlers), lg.propagate, lg.disabled, list(lg.filters))
        for name, lg in list(logging.Logger.manager.loggerDict.items())
        if isinstance(lg, logging.Logger)
    }
    yield
    root.setLevel(saved_root[0])
    root.handlers[:] = saved_root[1]
    root.filters[:] = saved_root[2]
    for name, lg in list(logging.Logger.manager.loggerDict.items()):
        if isinstance(lg, logging.Logger) and name in saved:
            level, handlers, propagate, disabled, filters = saved[name]
            lg.setLevel(level)
            lg.handlers[:] = handlers
            lg.propagate = propagate
            lg.disabled = disabled
            lg.filters[:] = filters


class _Collect(logging.Handler):
    def __init__(self):
        super().__init__(logging.DEBUG)
        self.records = []

    def emit(self, record):
        self.records.append(record)


def _rendered(record):
    text = record.getMessage()
    if record.exc_info:
        text += logging.Formatter().formatException(record.exc_info)
    return text


async def test_secret_not_logged_after_logging_is_reconfigured(serve, restore_logging):
    reachy, url, events = await serve(api_key=SECRET)  # the server exists BEFORE reconfiguration
    logging.config.dictConfig({
        "version": 1,
        "disable_existing_loggers": False,
        "loggers": {LOGGER: {"level": "DEBUG"}},
        "root": {"level": "DEBUG"},
    })
    logging.basicConfig(level=logging.DEBUG, force=True)
    # That reset the adapter's websockets logger to NOTSET, i.e. effectively DEBUG.
    assert logging.getLogger(LOGGER + ".ws").getEffectiveLevel() == logging.DEBUG
    collector = _Collect()
    logging.getLogger().addHandler(collector)
    async with _client(url) as ws:  # a NEW connection, created after the reconfiguration
        await _hello(ws, api_key=SECRET)  # the authenticating hello carries the key
        await ws.send('{"type":"stt","text":"' + SECRET)  # the adapter's DEBUG "invalid JSON"
        await _stt(ws, "hi")
        await _until(lambda: events)
    async with _client(url) as ws:
        await _hello(ws, api_key=SECRET + "-wrong")
        assert (await _close_of(ws))[0] == 1008
    await _until(lambda: not reachy._robots)
    records = collector.records
    assert any(r.name == LOGGER and r.levelno == logging.DEBUG for r in records)  # DEBUG capture was live
    assert [r for r in records if r.name.startswith(LOGGER + ".ws") and r.levelno <= logging.DEBUG] == []
    assert [(r.name, r.levelname) for r in records if SECRET in _rendered(r)] == []


# ── pending tool calls and closed connections ──────────────────────────────
@pytest.mark.parametrize("how", ["superseded", "dropped"])
async def test_pending_tool_call_fails_fast_when_its_connection_closes(how, serve):
    reachy, url, _ = await serve()
    loop = asyncio.get_running_loop()
    async with _client(url) as old:
        await _hello(old)
        await _until(lambda: "reachy" in reachy._robots)
        call = asyncio.create_task(reachy.call_robot_tool("reachy", "emote", {"emotion": "happy"}, timeout_s=10))
        request = json.loads(await asyncio.wait_for(old.recv(), timeout=5.0))
        assert request["type"] == "tool_call"
        started = loop.time()
        if how == "superseded":
            async with _client(url) as new:
                await _hello(new)
                result = await asyncio.wait_for(call, timeout=5.0)
        else:
            await old.close()
            result = await asyncio.wait_for(call, timeout=5.0)
    assert result == {"ok": False, "error": "robot disconnected"}
    assert loop.time() - started < 5.0  # not the 10 s call timeout
    assert reachy._tool_futures == {}


async def test_replacement_connection_may_still_answer_a_pending_call():
    reachy = adapter.ReachyAdapter(adapter.PlatformConfig(enabled=True, extra={"port": 8770, "api_key": KEY}))
    old_socket, fut = object(), asyncio.get_running_loop().create_future()
    reachy._tool_futures["t1"] = ("reachy", old_socket, fut)
    # The replacement connection (same robot) answers before the old socket finishes closing...
    reachy._resolve_tool_result("reachy", {"tool_call_id": "t1", "result": {"ok": True}})
    reachy._fail_pending_calls(old_socket)
    assert fut.result() == {"ok": True}  # ...and its answer stands


async def test_oversize_mapping_fails_safe_when_websockets_changes(serve, monkeypatch, caplog):
    import websockets.protocol

    real_fail = websockets.protocol.Protocol.fail

    def changed_fail(self, code, reason="", **kwargs):
        # A release whose fail() no longer accepts the call the adapter's hook makes.
        if int(code) == 1008 and reason == "frame too large":
            raise TypeError("fail() got an unexpected keyword argument")
        return real_fail(self, code, reason)

    monkeypatch.setattr(websockets.protocol.Protocol, "fail", changed_fail)
    monkeypatch.setattr(adapter, "_hook_fallback_logged", False)
    caplog.set_level(logging.INFO, logger=LOGGER)
    reachy, url, _ = await serve()
    for _ in range(2):
        async with _client(url, compression=None) as ws:
            await _hello(ws, pad="x" * (2 * adapter.HELLO_MAX_BYTES))
            assert (await _close_of(ws))[0] == 1009  # the library's own handling
    await _until(lambda: reachy._pending == 0 and caplog.text.count("frame too large") >= 2)
    assert caplog.text.count("cannot map oversized pre-auth frames") == 1  # logged once
    assert "auth rejected" in caplog.text
    await asyncio.wait_for(reachy.disconnect(), timeout=5.0)  # shutdown isn't stalled



# ── tool results carrying the key ──────────────────────────────────────────
async def test_tool_results_are_redacted_before_they_reach_hermes(serve, caplog):
    from hermes_reachy.body_tool import handle_reachy_body

    caplog.set_level(logging.DEBUG)  # root: every logger at DEBUG
    caplog.set_level(logging.DEBUG, logger=LOGGER)
    reachy, url, _ = await serve(api_key=SECRET)
    async with _client(url) as ws:
        await _hello(ws, api_key=SECRET)
        await _until(lambda: "reachy" in reachy._robots)
        await ws.send("not json")  # an adapter DEBUG record, to prove DEBUG capture is live
        handler = asyncio.create_task(handle_reachy_body({"action": "emote", "emotion": "happy"}))
        request = json.loads(await asyncio.wait_for(ws.recv(), timeout=5.0))
        assert request["type"] == "tool_call"
        result = {
            "ok": False,
            "error": f"credential rejected: {SECRET}",
            "detail": {"echo": SECRET, "attempts": [SECRET, 1, None, {"k": f"x{SECRET}x"}]},
            SECRET: "the key as a dict key",
            "raw": json.dumps({"api_key": SECRET}),  # a JSON document nested in a string
            "count": 3,
        }
        await ws.send(json.dumps({"type": "tool_result", "tool_call_id": request["tool_call_id"], "result": result}))
        returned = await asyncio.wait_for(handler, timeout=5.0)  # what the tool handler hands Hermes
    assert SECRET not in returned
    assert json.loads(returned) == {
        "ok": False,
        "error": "credential rejected: [redacted]",
        "detail": {"echo": "[redacted]", "attempts": ["[redacted]", 1, None, {"k": "x[redacted]x"}]},
        "[redacted]": "the key as a dict key",
        "raw": json.dumps({"api_key": "[redacted]"}),
        "count": 3,
    }
    # Hermes 0.20.5 logs a preview of a tool error at WARNING (agent/tool_executor.py):
    #   logger.warning("Tool %s returned error (%.2fs): %s", function_name, tool_duration, result_preview)
    # with result_preview = function_result[:200]. Reproduced here with the handler's real output.
    logging.getLogger("agent.tool_executor").warning(
        "Tool %s returned error (%.2fs): %s", "reachy_body", 0.01, returned[:200]
    )
    assert any(r.name == "agent.tool_executor" and r.levelno == logging.WARNING for r in caplog.records)
    assert any(r.name == LOGGER and r.levelno == logging.DEBUG for r in caplog.records)
    assert [r.name for r in caplog.records if SECRET in r.getMessage()] == []
    assert SECRET not in caplog.text
