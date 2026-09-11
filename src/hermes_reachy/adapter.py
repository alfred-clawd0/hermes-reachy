"""Reachy robot platform adapter (Hermes gateway plugin).

Transport model
---------------
The adapter runs a **WebSocket server**; the robot's voice app connects to it as a
**client** and stays connected. The app keeps ownership of the real-time audio path
(mic, STT, VAD, TTS synthesis, PCM streaming, half-duplex, barge onset). This adapter
only carries *text*:

  inbound  (app -> adapter):  {"type":"hello","robot_id":"reachy","api_key":"..."}  (first frame)
                              {"type":"stt","text":"...","robot_id":"reachy"}
                              {"type":"interrupt","text":"...","robot_id":"reachy"}
  outbound (adapter -> app):  {"type":"say","message_id":"m1","content":"...","final":false}
                              {"type":"typing","robot_id":"reachy"}

Authentication
--------------
The first frame of every connection must be a ``hello`` carrying the shared API key
(``REACHY_WS_API_KEY`` or ``REACHY_WS_API_KEY_FILE``), sent within ``HELLO_TIMEOUT_S`` and no
larger than ``HELLO_MAX_BYTES``. Its ``robot_id`` must be allowlisted (``REACHY_ALLOWED_ROBOTS``,
default ``reachy``; ``REACHY_ALLOW_ALL_ROBOTS`` accepts any id but still needs the key) and stays
fixed for the life of the connection. Any failure closes the socket with 1008 (policy
violation), as does arriving while ``MAX_PENDING_CONNECTIONS`` sockets already await their hello.
Frame contents, keys, rejected identities and configuration values are never logged. The
gateway's own allowlist / allow-all / pairing checks still run on every message — see
``ReachyAdapter.enforces_own_access_policy``.

An inbound "stt"/"interrupt" frame becomes a MessageEvent dispatched via
``handle_message``. Because the gateway's default busy-input mode is ``interrupt``,
a frame that arrives while a turn is generating cancels that turn mid-flight — that is
how barge-in maps onto the platform model (the app's semantic gate decides *whether*
to forward).

Outbound text is streamed: the gateway's stream consumer calls ``edit_message`` with
the growing accumulated ``content`` (SUPPORTS_MESSAGE_EDITING = True). Each edit is
pushed verbatim as a "say" frame; the app diffs against what it has already spoken and
extracts new clauses for TTS. Because the WebSocket persists, supports_async_delivery
stays True (the base default) so background/cron/send_message delivery reaches Reachy.

Zero core changes: ``Platform("reachy")`` resolves via the enum's ``_missing_`` hook.
"""

from __future__ import annotations

import asyncio
import contextvars
import hmac
import ipaddress
import json
import logging
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

try:  # optional dep; check_requirements gates instantiation
    from websockets.asyncio.server import ServerConnection
    from websockets.asyncio.server import serve as ws_serve
    from websockets.exceptions import ConnectionClosed

    WEBSOCKETS_AVAILABLE = True
except Exception:  # pragma: no cover - import guard
    ws_serve = None  # type: ignore
    ServerConnection = object  # type: ignore  # _ReachyConnection is never built without websockets
    ConnectionClosed = OSError  # type: ignore  # never raised: connect() bails out first
    WEBSOCKETS_AVAILABLE = False

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import (
    BasePlatformAdapter,
    MessageEvent,
    MessageType,
    SendResult,
)

logger = logging.getLogger(__name__)

MAX_MESSAGE_LENGTH = 4000
DEFAULT_ROBOT_ID = "reachy"
# Loopback: the voice app normally runs on the same machine as the gateway.
DEFAULT_HOST = "127.0.0.1"
# Seconds a new connection has to send its authenticating hello frame.
HELLO_TIMEOUT_S = 10.0
# Largest frame accepted before the hello is authenticated (a hello is ~100 bytes).
HELLO_MAX_BYTES = 4096
# Largest frame accepted from an authenticated robot (websockets' default).
MAX_MESSAGE_BYTES = 2**20
# Connections allowed to sit in the hello phase at once; the rest are closed immediately.
MAX_PENDING_CONNECTIONS = 16
# At most one "too many pending connections" warning per this many seconds.
PENDING_REJECT_LOG_INTERVAL_S = 10.0
# RFC 6455 close codes.
CLOSE_NORMAL = 1000
CLOSE_POLICY_VIOLATION = 1008
CLOSE_MESSAGE_TOO_BIG = 1009
# Same spelling the gateway accepts for {PLATFORM}_ALLOW_ALL_USERS-style flags.
_TRUTHY = frozenset({"true", "1", "yes"})
# Robot ids are identities that end up in logs, session keys and allowlists: keep them plain.
_ROBOT_ID_RE = re.compile(r"[A-Za-z0-9_.:@-]{1,64}")
_ROBOT_ID_RULE = "robot ids are 1-64 characters from A-Z a-z 0-9 _ . : @ -"

# Correlates outbound frames to the turn that produced them. Set around handle_message()
# in _dispatch_text; asyncio.create_task copies the current context, so the turn's detached
# background task (and its edit_message/send/on_processing_complete calls) inherit this
# client-supplied turn_id. A proactive send (async-delegation watcher / cron / send_message)
# runs in a SEPARATE task that never entered _dispatch_text, so the var is unset (None) and
# the frame is tagged origin="proactive" — this is how the client tells an interactive reply
# apart from an unsolicited delivery arriving mid-turn.
_CURRENT_TURN_ID: contextvars.ContextVar[Optional[str]] = contextvars.ContextVar(
    "reachy_current_turn_id", default=None
)

# The running adapter instance (set in connect/disconnect). The body-tool handler uses
# this to reach the robot from a tool handler — same process, no extra transport
# (body-tool surface).
_ACTIVE_ADAPTER: Optional["Any"] = None


def get_active_adapter() -> Optional["Any"]:
    return _ACTIVE_ADAPTER


class _HandshakeNoiseFilter(logging.Filter):
    """Drop websockets' noisy tracebacks when a non-ws client (e.g. a bare-TCP
    health check) opens and closes the port without a handshake."""

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: D401
        try:
            msg = record.getMessage()
        except Exception:
            return True
        return "opening handshake failed" not in msg and "did not receive a valid HTTP request" not in msg


class _NoDebugLogger(logging.LoggerAdapter):
    """The logger handed to websockets: it never enables or emits DEBUG, whatever the logging
    configuration says. websockets' DEBUG output dumps frames, and frames carry the API key.

    A level on the underlying logger is not enough: e.g. ``logging.config.dictConfig`` resets
    it to NOTSET when it configures a parent. websockets decides once per connection with
    ``isEnabledFor(DEBUG)`` and wraps this object in its own ``LoggerAdapter``, whose calls
    delegate here — so both the decision and every record pass through this class.
    """

    def isEnabledFor(self, level: int) -> bool:
        return level > logging.DEBUG and self.logger.isEnabledFor(level)

    def log(self, level: int, msg: object, *args: Any, **kwargs: Any) -> None:
        if level > logging.DEBUG:
            super().log(level, msg, *args, **kwargs)

    def process(self, msg: Any, kwargs: Any) -> tuple[Any, Any]:
        return msg, kwargs  # keep websockets' own ``extra`` (the base class would replace it)


_WS_LOGGER = _NoDebugLogger(logging.getLogger(__name__ + ".ws"))

# Set once the size hook below has had to fall back, so the warning is logged only once.
_hook_fallback_logged = False


def _warn_hook_fallback(why: str) -> None:
    global _hook_fallback_logged
    if not _hook_fallback_logged:
        _hook_fallback_logged = True
        logger.warning(
            "[reachy] cannot map oversized pre-auth frames to 1008 on this websockets release "
            "(%s); they close with the library's 1009 instead",
            why,
        )


def _install_hello_size_hook(conn: Any) -> None:
    """Make an oversized pre-auth frame close with 1008 "frame too large", not 1009.

    websockets rejects an oversized message inside its frame parser by calling
    ``protocol.fail(1009, ...)``; there is no public hook to choose the code. This wraps that
    connection's ``protocol.fail``: while ``conn.authenticated`` is False it re-issues the call
    with 1008 / "frame too large" (keeping any other arguments) and sets
    ``conn.hello_too_large``. ``protocol.fail`` is a websockets internal — stable in 13-17,
    hence the ``<18`` pin — so the wrapper fails safe: if the attribute is missing or the
    re-issued call is rejected, the library's own handling (1009) runs, with one warning.
    """
    protocol = getattr(conn, "protocol", None)
    original = getattr(protocol, "fail", None)
    if not callable(original):
        _warn_hook_fallback("protocol.fail is missing")
        return

    def fail(*args: Any, **kwargs: Any) -> Any:
        code = kwargs["code"] if "code" in kwargs else (args[0] if args else None)
        if code != CLOSE_MESSAGE_TOO_BIG or getattr(conn, "authenticated", True):
            return original(*args, **kwargs)
        mapped_args, mapped_kwargs = list(args), dict(kwargs)
        if "code" in mapped_kwargs:
            mapped_kwargs["code"] = CLOSE_POLICY_VIOLATION
        else:
            mapped_args[0] = CLOSE_POLICY_VIOLATION
        if "reason" in mapped_kwargs:
            mapped_kwargs["reason"] = "frame too large"
        elif len(mapped_args) > 1:
            mapped_args[1] = "frame too large"
        else:
            mapped_kwargs["reason"] = "frame too large"
        try:
            result = original(*mapped_args, **mapped_kwargs)
        except (TypeError, AttributeError):
            _warn_hook_fallback("unexpected protocol.fail() signature")
            return original(*args, **kwargs)
        conn.hello_too_large = True
        return result

    try:
        protocol.fail = fail
    except AttributeError:  # e.g. a release that makes the protocol slotted
        _warn_hook_fallback("protocol.fail is read-only")


class _ReachyConnection(ServerConnection):
    """Server connection that keeps the hello contract for oversized frames (see
    ``_install_hello_size_hook``). The hook is installed when the connection is built, so it
    also covers frames that arrive before the handler runs. ``_handle_conn`` marks the
    connection authenticated and raises its limit to ``MAX_MESSAGE_BYTES`` after the hello;
    from then on an oversized frame closes with the library's usual 1009."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.authenticated = False
        self.hello_too_large = False
        _install_hello_size_hook(self)


def _set_message_limit(websocket: Any, limit: int) -> None:
    """Change one connection's incoming message limit."""
    protocol = websocket.protocol
    if hasattr(protocol, "max_message_size"):  # websockets >= 16 splits message / fragment limits
        protocol.max_message_size = limit
    else:  # websockets 13-15
        protocol.max_size = limit


# ── configuration ──────────────────────────────────────────────────────────
class ReachyConfigError(ValueError):
    """The Reachy platform configuration is unusable. The message names the setting and the
    problem, never a configured value (a value may be, or contain, the key)."""


@dataclass(frozen=True)
class _Settings:
    host: str
    port: int
    api_key: bytes = field(repr=False)
    allowed_robots: frozenset[str]
    allow_all_robots: bool


def _setting(extra: dict[str, Any], key: str, env: str) -> Any:
    """``PlatformConfig.extra[key]`` when set, else the ``env`` variable ("" if neither)."""
    value = extra.get(key)
    if value is None or (isinstance(value, str) and not value.strip()):
        return os.getenv(env, "")
    return value


def _key_bytes(key: str) -> bytes:
    # Compare bytes, not str: hmac.compare_digest rejects non-ASCII str. surrogatepass keeps
    # env values decoded with surrogateescape (undecodable bytes) encodable too.
    return key.encode("utf-8", "surrogatepass")


_REDACTED = "[redacted]"


def _key_needles(api_key: bytes) -> tuple[str, ...]:
    """The key as it can appear in text: raw, and JSON-escaped (ASCII-only and UTF-8) for a key
    inside a JSON document nested in a string. Longest first, so no needle hides another."""
    key = api_key.decode("utf-8", "surrogatepass")
    forms = {key, json.dumps(key)[1:-1], json.dumps(key, ensure_ascii=False)[1:-1]}
    return tuple(sorted((form for form in forms if form), key=len, reverse=True))


def _redact(value: Any, needles: tuple[str, ...]) -> Any:
    """``value`` with every needle in every string (dict keys included, at any depth) replaced
    by ``[redacted]``. Containers are rebuilt only along paths that changed: the input is never
    mutated, and a value without the key is returned as the very same object."""
    if isinstance(value, str):
        for needle in needles:
            if needle in value:
                value = value.replace(needle, _REDACTED)
        return value
    if isinstance(value, dict):
        pairs = [(_redact(k, needles), _redact(v, needles)) for k, v in value.items()]
        if all(nk is k and nv is v for (nk, nv), (k, v) in zip(pairs, value.items(), strict=True)):
            return value
        return dict(pairs)
    if isinstance(value, (list, tuple)):
        items = [_redact(item, needles) for item in value]
        if all(new is old for new, old in zip(items, value, strict=True)):
            return value
        return type(value)(items)
    return value


def _os_error_text(exc: BaseException) -> str:
    return (exc.strerror if isinstance(exc, OSError) else None) or type(exc).__name__


def _parse_port(raw: Any) -> int:
    text = str(raw).strip()
    if not text:
        raise ReachyConfigError("REACHY_WS_PORT is not set")
    try:
        port = int(text)
    except ValueError:
        raise ReachyConfigError("REACHY_WS_PORT is not an integer") from None
    if not 1 <= port <= 65535:
        raise ReachyConfigError("REACHY_WS_PORT is outside 1-65535")
    return port


def _load_api_key(extra: dict[str, Any]) -> bytes:
    """Inline key (``REACHY_WS_API_KEY``) wins; otherwise read ``REACHY_WS_API_KEY_FILE``."""
    inline = _setting(extra, "api_key", "REACHY_WS_API_KEY")
    if not isinstance(inline, str):
        raise ReachyConfigError("api_key must be a string")
    if inline.strip():
        return _key_bytes(inline.strip())
    key_file = str(_setting(extra, "api_key_file", "REACHY_WS_API_KEY_FILE")).strip()
    if not key_file:
        raise ReachyConfigError("no API key configured: set REACHY_WS_API_KEY or REACHY_WS_API_KEY_FILE")
    try:
        key = Path(key_file).expanduser().read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise ReachyConfigError(f"REACHY_WS_API_KEY_FILE is unreadable: {_os_error_text(exc)}") from None
    except UnicodeDecodeError:
        raise ReachyConfigError("REACHY_WS_API_KEY_FILE is not valid UTF-8") from None
    if not key:
        raise ReachyConfigError("REACHY_WS_API_KEY_FILE is empty")
    return _key_bytes(key)


def _split_robot_ids(raw: Any) -> list[str]:
    items = raw.split(",") if isinstance(raw, str) else [str(item) for item in raw or ()]
    return [item.strip() for item in items if item.strip()]


def _load_settings(config: Any) -> _Settings:
    """Single source of truth for validate_config and the adapter; raises ReachyConfigError."""
    extra = getattr(config, "extra", None) or {}
    port = _parse_port(_setting(extra, "port", "REACHY_WS_PORT"))
    host = str(_setting(extra, "host", "REACHY_WS_HOST")).strip() or DEFAULT_HOST
    api_key = _load_api_key(extra)
    # Env first, like the gateway's own allowlist check, so both layers see the same list.
    source, raw_robots = "REACHY_ALLOWED_ROBOTS", os.getenv("REACHY_ALLOWED_ROBOTS", "")
    if not raw_robots.strip():
        source, raw_robots = "allowed_robots", extra.get("allowed_robots") or ""
    robots = _split_robot_ids(raw_robots)
    for position, robot in enumerate(robots, 1):
        # Name the entry by position only: the value itself may be the key pasted by mistake.
        if not _ROBOT_ID_RE.fullmatch(robot):
            raise ReachyConfigError(f"{source} entry #{position} is malformed: {_ROBOT_ID_RULE}")
        if api_key in _key_bytes(robot):
            raise ReachyConfigError(f"{source} entry #{position} contains the API key")
    return _Settings(
        host=host,
        port=port,
        api_key=api_key,
        # Unset, empty or all-blank means the default robot — never an empty allowlist.
        allowed_robots=frozenset(robots) or frozenset({DEFAULT_ROBOT_ID}),
        # Env only: it is the exact flag the gateway's allow-all check reads.
        allow_all_robots=os.getenv("REACHY_ALLOW_ALL_ROBOTS", "").strip().lower() in _TRUTHY,
    )


def _is_loopback(host: str) -> bool:
    if host.lower() == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _format_sockaddr(addr: Any) -> str:
    host, port = str(addr[0]), addr[1]
    return f"[{host}]:{port}" if ":" in host else f"{host}:{port}"


def _peer(websocket: Any) -> str:
    addr = getattr(websocket, "remote_address", None)
    if isinstance(addr, tuple) and len(addr) >= 2:
        return _format_sockaddr(addr)
    return "unknown peer"


def check_requirements() -> bool:
    """Return whether the WebSocket dependency is available."""
    return WEBSOCKETS_AVAILABLE


def is_configured(config: PlatformConfig) -> bool:
    """Gateway ``is_connected`` hook: has the operator opted in (a port is set)?

    Deliberately shallow and silent — it gates auto-enablement and status displays. An
    enabled platform with an incomplete setup is then refused by ``validate_config``,
    which logs the reason.
    """
    extra = getattr(config, "extra", None) or {}
    return bool(str(_setting(extra, "port", "REACHY_WS_PORT")).strip())


def validate_config(config: PlatformConfig) -> bool:
    """Require a valid port, a usable API key (inline, or from a readable non-empty file) and
    a well-formed robot allowlist."""
    try:
        _load_settings(config)
    except ReachyConfigError as exc:
        logger.error("[reachy] platform not started: %s", exc)
        return False
    return True


class ReachyAdapter(BasePlatformAdapter):
    """WebSocket-server adapter for the Reachy robot voice app."""

    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH
    # Enable the gateway's streaming path (progressive edit_message deltas).
    SUPPORTS_MESSAGE_EDITING = True
    # Persistent outbound channel → background/cron/send_message can reach Reachy.
    supports_async_delivery = True

    @property
    def enforces_own_access_policy(self) -> bool:
        """The transport admits only allowlisted robot ids (see ``_is_dm_allowed``).

        When no env allowlist is configured the gateway would otherwise default-deny even
        the default ``reachy`` robot; with this flag and an ``allowlist`` ``_dm_policy`` it
        re-checks the sender through ``_is_dm_allowed`` instead. Unlike
        ``authorization_is_upstream`` (meant for the trusted relay), this keeps the
        gateway's allowlist, allow-all flag and pairing store in force as a second layer.
        """
        return True

    def __init__(self, config: PlatformConfig):
        super().__init__(config=config, platform=Platform("reachy"))
        # Same loader as validate_config: a config it rejects raises here, loudly.
        settings = _load_settings(config)
        self._host: str = settings.host
        self._port: int = settings.port
        self._api_key: bytes = settings.api_key
        self._key_needles = _key_needles(settings.api_key)
        self._allowed_robots: frozenset[str] = settings.allowed_robots
        self._allow_all_robots: bool = settings.allow_all_robots
        # Read by the gateway's authorization when no env allowlist is set. Under allow-all
        # the gateway's own REACHY_ALLOW_ALL_ROBOTS check admits the robot before this.
        self._dm_policy = "open" if self._allow_all_robots else "allowlist"
        self._server: Optional[Any] = None
        # robot_id -> active websocket connection
        self._robots: Dict[str, Any] = {}
        # robot_id -> turn_id of the most recently dispatched client turn (see _current_turn_id)
        self._turn_ids: Dict[str, str] = {}
        # tool_call_id -> (robot_id it was sent to, the socket it went out on, Future for the result)
        self._tool_futures: dict[str, tuple[str, Any, asyncio.Future]] = {}
        # strong refs to in-flight closes of superseded connections
        self._closing: set[asyncio.Task] = set()
        # sockets still in the hello phase, and rate-limit state for the overflow warning
        self._pending = 0
        self._pending_refused = 0
        self._last_pending_log = float("-inf")

    def _robot_allowed(self, robot_id: str) -> bool:
        return self._allow_all_robots or robot_id in self._allowed_robots

    def _is_dm_allowed(self, sender_id: str) -> bool:
        """Gateway hook for ``dm_policy: allowlist``: would the transport admit this robot?"""
        return self._robot_allowed(str(sender_id))

    def _warn_if_gateway_denies_robots(self) -> None:
        """With GATEWAY_ALLOWED_USERS set and REACHY_ALLOWED_ROBOTS unset, the gateway only
        admits robot ids listed in GATEWAY_ALLOWED_USERS — say so instead of failing silently."""
        if self._allow_all_robots or os.getenv("REACHY_ALLOWED_ROBOTS", "").strip():
            return
        gateway_ids = frozenset(_split_robot_ids(os.getenv("GATEWAY_ALLOWED_USERS", "")))
        if not gateway_ids or "*" in gateway_ids:
            return
        denied = len(self._allowed_robots - gateway_ids)
        if denied:
            logger.warning(
                "[reachy] GATEWAY_ALLOWED_USERS is set but REACHY_ALLOWED_ROBOTS is not: the "
                "gateway will deny %d allowlisted robot id(s) missing from GATEWAY_ALLOWED_USERS "
                "— set REACHY_ALLOWED_ROBOTS explicitly",
                denied,
            )

    # ── lifecycle ──────────────────────────────────────────────────────────
    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not WEBSOCKETS_AVAILABLE:
            logger.warning("[reachy] websockets not installed (pip install 'websockets>=13,<18')")
            return False
        # keep the gateway journal clean of non-ws probe tracebacks
        ws_logger = _WS_LOGGER.logger
        if not any(isinstance(f, _HandshakeNoiseFilter) for f in ws_logger.filters):
            ws_logger.addFilter(_HandshakeNoiseFilter())
        try:
            self._server = await ws_serve(
                self._handle_conn,
                self._host,
                self._port,
                max_size=HELLO_MAX_BYTES,  # raised per connection once the hello is accepted
                logger=_WS_LOGGER,
                create_connection=_ReachyConnection,
            )
        except Exception as e:  # pragma: no cover - bind failure path
            logger.error(
                "[reachy] failed to start the ws server on REACHY_WS_HOST / REACHY_WS_PORT: %s",
                _os_error_text(e),
            )
            return False
        self._mark_connected()
        global _ACTIVE_ADAPTER
        _ACTIVE_ADAPTER = self
        # The adapter's home loop: tool handlers may run in a DIFFERENT loop (the registry's
        # async bridge) — call_robot_tool must execute here or its future never wakes.
        self._loop = asyncio.get_running_loop()
        # Log the addresses actually bound (OS-reported), not the configured host string.
        bound = [sock.getsockname() for sock in self._server.sockets]
        logger.info("[reachy] ws server listening on %s", ", ".join(_format_sockaddr(a) for a in bound))
        if any(not _is_loopback(str(addr[0])) for addr in bound):
            logger.warning(
                "[reachy] listening on a non-loopback address: ws:// carries the API key in plaintext — "
                "prefer an SSH tunnel or a TLS proxy"
            )
        self._warn_if_gateway_denies_robots()
        return True

    async def disconnect(self) -> None:
        self._mark_disconnected()
        for rid, ws in list(self._robots.items()):
            try:
                await ws.close()
            except Exception:
                pass
            self._robots.pop(rid, None)
        if self._server is not None:
            try:
                self._server.close()
                await self._server.wait_closed()
            except Exception:
                pass
            self._server = None
        global _ACTIVE_ADAPTER
        if _ACTIVE_ADAPTER is self:
            _ACTIVE_ADAPTER = None
        logger.info("[reachy] disconnected")

    # ── inbound transport ──────────────────────────────────────────────────
    async def _handle_conn(self, websocket: Any) -> None:
        """One persistent robot connection. websockets>=13 passes a single arg;
        the request path is on ``websocket.request.path``."""
        path = ""
        try:
            path = getattr(getattr(websocket, "request", None), "path", "") or ""
        except Exception:
            path = ""
        if self._pending >= MAX_PENDING_CONNECTIONS:
            self._log_pending_overflow(websocket)
            await websocket.close(CLOSE_POLICY_VIOLATION, "too many pending connections")
            return
        self._pending += 1
        try:
            robot_id = await self._authenticate(websocket, path)
        finally:
            self._pending -= 1
        if robot_id is None:
            return
        # Authenticated: size violations are ordinary 1009s again, under the normal limit.
        websocket.authenticated = True
        _set_message_limit(websocket, MAX_MESSAGE_BYTES)
        previous = self._robots.get(robot_id)
        self._robots[robot_id] = websocket
        if previous is not None and previous is not websocket:
            # Newest connection wins; close the old socket rather than leaving it orphaned.
            logger.warning("[reachy] robot %s re-authenticated; closing its previous connection", robot_id)
            task = asyncio.create_task(previous.close(CLOSE_NORMAL, "superseded by a newer connection"))
            self._closing.add(task)
            task.add_done_callback(self._closing.discard)
        logger.info("[reachy] robot connected: %s", robot_id)
        try:
            async for raw in websocket:
                await self._on_inbound(robot_id, raw)
        except asyncio.CancelledError:  # pragma: no cover
            raise
        except Exception as e:
            # type only: exception text (e.g. a close reason) is client-controlled
            logger.debug("[reachy] connection loop ended for %s: %s", robot_id, type(e).__name__)
        finally:
            # Tool calls that went out on this socket can no longer be answered through it.
            self._fail_pending_calls(websocket)
            # only drop the mapping if it still points at this socket
            if self._robots.get(robot_id) is websocket:
                self._robots.pop(robot_id, None)
                logger.info("[reachy] robot disconnected: %s", robot_id)
            else:
                logger.info("[reachy] closed a superseded connection for %s", robot_id)

    def _log_pending_overflow(self, websocket: Any) -> None:
        """Rate-limited warning for connections refused because too many await their hello."""
        self._pending_refused += 1
        now = time.monotonic()
        if now - self._last_pending_log < PENDING_REJECT_LOG_INTERVAL_S:
            return
        logger.warning(
            "[reachy] auth rejected from %s: too many pending connections "
            "(limit %d; %d refused since the last report)",
            _peer(websocket),
            MAX_PENDING_CONNECTIONS,
            self._pending_refused,
        )
        self._pending_refused = 0
        self._last_pending_log = now

    async def _authenticate(self, websocket: Any, path: str) -> str | None:
        """Run the hello handshake: return the admitted robot id, or None once closed.
        Rejections log a category and the peer, never a value taken from the frame."""
        try:
            raw = await asyncio.wait_for(websocket.recv(), timeout=HELLO_TIMEOUT_S)
        except TimeoutError:
            return await self._reject(websocket, f"no hello within {HELLO_TIMEOUT_S:g}s", "hello timeout")
        except ConnectionClosed:
            close_sent = getattr(getattr(websocket, "protocol", None), "close_sent", None)
            if getattr(websocket, "hello_too_large", False) or getattr(close_sent, "code", None) == CLOSE_MESSAGE_TOO_BIG:
                # Closed by the protocol itself: 1008 via the size hook, or 1009 if it fell back.
                logger.warning(
                    "[reachy] auth rejected from %s: first frame over %d bytes (frame too large)",
                    _peer(websocket),
                    HELLO_MAX_BYTES,
                )
            else:
                logger.debug("[reachy] %s closed before sending hello", _peer(websocket))
            return None
        hello, problem = self._decode_frame(raw)
        if problem:
            return await self._reject(websocket, f"first frame: {problem}", "hello required")
        if str(hello.get("type") or "").lower() != "hello":
            return await self._reject(websocket, "first frame was not a hello", "hello required")
        supplied = hello.get("api_key")
        if not isinstance(supplied, str) or not supplied:
            return await self._reject(websocket, "hello without a string api_key", "api_key required")
        if not hmac.compare_digest(_key_bytes(supplied), self._api_key):
            return await self._reject(websocket, "wrong api_key", "authentication failed")
        claimed = hello.get("robot_id")
        if claimed is not None and not isinstance(claimed, str):
            return await self._reject(websocket, "non-string robot_id", "invalid robot_id")
        robot_id = (claimed or "").strip() or self._robot_id_from_path(path)
        if not _ROBOT_ID_RE.fullmatch(robot_id):
            return await self._reject(websocket, "malformed robot_id", "invalid robot_id")
        # Admitted ids are logged; one carrying the key (possible under allow-all) never is.
        if self._api_key in _key_bytes(robot_id):
            return await self._reject(websocket, "robot_id contains the API key", "invalid robot_id")
        if not self._robot_allowed(robot_id):
            return await self._reject(websocket, "robot_id not allowlisted", "robot not allowed")
        return robot_id

    @staticmethod
    async def _reject(websocket: Any, why: str, reason: str) -> None:
        """Close an unauthenticated connection with 1008; log why — a category, never content."""
        logger.warning("[reachy] auth rejected from %s: %s", _peer(websocket), why)
        await websocket.close(CLOSE_POLICY_VIOLATION, reason)

    @staticmethod
    def _robot_id_from_path(path: str) -> str:
        seg = (path or "").strip("/").split("/")[-1].split("?")[0].strip()
        return seg or DEFAULT_ROBOT_ID

    async def _on_inbound(self, robot_id: str, raw: Any) -> None:
        """Handle one post-handshake frame; the connection's robot_id never changes."""
        frame, problem = self._decode_frame(raw)
        if problem:
            logger.debug("[reachy] dropped a frame from %s: %s", robot_id, problem)
            return

        ftype = str(frame.get("type") or "").lower()
        if ftype == "hello":
            return
        new_id = str(frame.get("robot_id") or "").strip()
        if new_id and new_id != robot_id:
            logger.warning("[reachy] dropped a frame claiming a different robot id on %s's connection", robot_id)
            return

        if ftype in ("stt", "interrupt", "text"):
            text = str(frame.get("text") or "").strip()
            if text:
                turn_id = str(frame.get("turn_id") or "").strip() or None
                await self._dispatch_text(robot_id, text, turn_id=turn_id)
        elif ftype == "tool_result":
            self._resolve_tool_result(robot_id, frame)
        else:
            logger.debug("[reachy] ignored a frame of unhandled type from %s", robot_id)

    def _resolve_tool_result(self, robot_id: str, frame: dict[str, Any]) -> None:
        """Complete a pending tool call — only for the robot it was sent to (on any of its
        connections, so a replacement socket may still answer)."""
        pending = self._tool_futures.get(str(frame.get("tool_call_id") or "").strip())
        if pending is None:
            logger.debug("[reachy] tool_result from %s matches no pending call", robot_id)
            return
        owner, _socket, fut = pending
        if owner != robot_id:
            logger.warning("[reachy] ignored tool_result from %s for a call sent to %s", robot_id, owner)
            return
        if not fut.done():
            result = frame.get("result")
            # The robot app may echo the key (e.g. in an error string), and the result travels on
            # to Hermes, which logs tool-error previews: strip the key before it leaves.
            try:
                fut.set_result(_redact(result, self._key_needles) if isinstance(result, dict) else {})
            except RecursionError:
                fut.set_result({"ok": False, "error": "tool result nested too deeply"})

    def _fail_pending_calls(self, websocket: Any) -> None:
        """Fail, at once, the still-open tool calls that were sent on a socket that has closed
        (superseded or dropped), instead of letting them run into their timeout."""
        for _owner, socket, fut in list(self._tool_futures.values()):
            if socket is websocket and not fut.done():
                fut.set_result({"ok": False, "error": "robot disconnected"})

    @staticmethod
    def _decode_frame(raw: Any) -> tuple[dict[str, Any], str]:
        """Parse one JSON-object frame: ``(frame, "")``, or ``({}, problem)`` where the problem
        is a category that is safe to log. The frame content never is: it can carry the key."""
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="replace")
        raw = (raw or "").strip()
        if not raw:
            return {}, "empty frame"
        try:
            frame = json.loads(raw)
        except (ValueError, RecursionError):
            return {}, "invalid JSON"
        if not isinstance(frame, dict):
            return {}, "not a JSON object"
        return frame, ""

    async def _dispatch_text(self, robot_id: str, text: str, *, turn_id: Optional[str] = None) -> None:
        source = self.build_source(
            chat_id=robot_id,
            chat_name=f"Reachy {robot_id}",
            chat_type="dm",
            user_id=robot_id,
            user_name=f"Reachy {robot_id}",
        )
        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            source=source,
            message_id=f"stt_{robot_id}_{int(time.time() * 1000)}",
            timestamp=datetime.now(),
        )
        # Stash on the event for on_processing_complete (which receives the event) and set the
        # contextvar so the turn's background task stamps every say/edit frame with this id.
        if turn_id:
            event.metadata["reachy_turn_id"] = turn_id
            self._turn_ids[robot_id] = turn_id
        tok = _CURRENT_TURN_ID.set(turn_id)
        try:
            await self.handle_message(event)
        finally:
            # Reset only THIS task's view; the turn's detached task already captured its own copy.
            _CURRENT_TURN_ID.reset(tok)

    # ── outbound transport ─────────────────────────────────────────────────
    def _current_turn_id(self, robot_id: str) -> Optional[str]:
        """turn_id to stamp on an outbound frame for this robot.

        None (no turn context) -> proactive. Inside a turn context, use the robot's LATEST
        dispatched turn id rather than the inherited ContextVar value: the busy-mode pending
        drain task is created from the OLD turn task's context, so its ContextVar still carries
        the superseded id — frames stamped with it were dropped client-side and the interrupt
        turn's answer went silent."""
        ctx = _CURRENT_TURN_ID.get()
        if ctx is None:
            return None
        return self._turn_ids.get(robot_id, ctx)

    @staticmethod
    def _tag_turn(obj: Dict[str, Any], turn_id: Optional[str]) -> Dict[str, Any]:
        """Stamp an outbound frame so the client can route it: an interactive reply carries the
        client's ``turn_id`` (origin=turn); anything emitted outside a turn is a proactive
        delivery (turn_id=None, origin=proactive)."""
        obj["turn_id"] = turn_id
        obj["origin"] = "turn" if turn_id else "proactive"
        return obj

    async def call_robot_tool(
        self, robot_id: str, action: str, params: Optional[Dict[str, Any]] = None, *, timeout_s: float = 12.0
    ) -> Dict[str, Any]:
        """Body-tool surface: push a tool_call frame to the robot app and
        await its tool_result. The app executes through its MovementManager/tool registry with
        a client-side allowlist; this side only correlates request and response. A call whose
        connection closes before the answer fails at once with ``{"ok": False, "error":
        "robot disconnected"}``."""
        ws = self._robots.get(robot_id)
        if ws is None:
            return {"error": f"robot {robot_id} not connected"}
        tcid = uuid.uuid4().hex
        fut: "asyncio.Future" = asyncio.get_running_loop().create_future()
        self._tool_futures[tcid] = (robot_id, ws, fut)
        try:
            ok = await self._push(
                robot_id,
                {"type": "tool_call", "tool_call_id": tcid, "action": action, "params": params or {}},
            )
            if not ok:
                return {"error": f"robot {robot_id} not reachable"}
            return await asyncio.wait_for(fut, timeout=timeout_s)
        except asyncio.TimeoutError:
            return {"error": f"robot tool timed out after {timeout_s:.0f}s"}
        finally:
            self._tool_futures.pop(tcid, None)

    async def _push(self, robot_id: str, obj: Dict[str, Any]) -> bool:
        ws = self._robots.get(robot_id)
        if ws is None:
            return False
        try:
            await ws.send(json.dumps(obj, ensure_ascii=False))
            return True
        except Exception as e:
            # type only: exception text (e.g. a close reason) is client-controlled
            logger.warning("[reachy] push to %s failed: %s", robot_id, type(e).__name__)
            # Only evict OUR socket: the client may have reconnected already; popping
            # unconditionally removed the healthy new connection.
            if self._robots.get(robot_id) is ws:
                self._robots.pop(robot_id, None)
            return False

    async def send(
        self,
        chat_id: str,
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> SendResult:
        robot_id = (metadata or {}).get("robot_id") or chat_id
        message_id = f"say_{robot_id}_{uuid.uuid4().hex[:10]}"
        ok = await self._push(
            robot_id,
            self._tag_turn(
                {
                    "type": "say",
                    "kind": "message",  # standalone (notice / tool-status / short reply / proactive)
                    "message_id": message_id,
                    "content": content,
                    "final": True,
                },
                self._current_turn_id(robot_id),
            ),
        )
        if not ok:
            return SendResult(success=False, error=f"robot {robot_id} not connected")
        return SendResult(success=True, message_id=message_id)

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ) -> SendResult:
        ok = await self._push(
            chat_id,
            self._tag_turn(
                {
                    "type": "say",
                    "kind": "stream",  # progressive edit of a streamed reply
                    "message_id": message_id,
                    "content": content,
                    "final": bool(finalize),
                },
                self._current_turn_id(chat_id),
            ),
        )
        if not ok:
            return SendResult(success=False, error=f"robot {chat_id} not connected")
        return SendResult(success=True, message_id=message_id)

    async def send_typing(self, chat_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        # Tagged like every outbound frame: untagged typing fell back to the client's temporal
        # rule and reset the ACTIVE turn's per-frame timeout even when it belonged to another
        # (e.g. proactive) turn. Tagged, it routes/drops cleanly
        # and doubles as the liveness signal for the client's stall watchdog.
        await self._push(
            chat_id,
            self._tag_turn({"type": "typing", "robot_id": chat_id}, self._current_turn_id(chat_id)),
        )

    async def on_processing_complete(self, event: MessageEvent, outcome: Any) -> None:
        """Emit an explicit turn boundary so the client knows a turn is fully
        done (the gateway sends several 'say' messages per turn — notices,
        tool-status, and the streamed answer — with no other end marker)."""
        try:
            chat_id = event.source.chat_id
        except Exception:
            return
        # Prefer the id stashed on the event (most reliable here); fall back to the contextvar.
        turn_id = None
        try:
            turn_id = (event.metadata or {}).get("reachy_turn_id")
        except Exception:
            turn_id = None
        turn_id = turn_id or self._current_turn_id(chat_id)
        await self._push(
            chat_id,
            self._tag_turn(
                {"type": "turn_end", "robot_id": chat_id, "outcome": getattr(outcome, "value", str(outcome))},
                turn_id,
            ),
        )

    async def get_chat_info(self, chat_id: str) -> Dict[str, Any]:
        return {
            "name": f"Reachy {chat_id}",
            "type": "dm",
            "chat_id": chat_id,
            "connected": chat_id in self._robots,
        }


# ── plugin registration ────────────────────────────────────────────────────
def _env_enablement() -> Optional[dict]:
    """Seed PlatformConfig.extra from env so an env-only setup surfaces in status."""
    port = os.getenv("REACHY_WS_PORT", "").strip()
    if not port:
        return None
    # A malformed port is reported by validate_config; never raise from the enablement sweep.
    seed: dict[str, Any] = {"port": int(port) if port.isdigit() else port}
    host = os.getenv("REACHY_WS_HOST", "").strip()
    if host:
        seed["host"] = host
    home = os.getenv("REACHY_HOME_CHANNEL", "").strip()
    if home:
        seed["home_channel"] = {"chat_id": home, "name": f"Reachy {home}"}
    key_file = os.getenv("REACHY_WS_API_KEY_FILE", "").strip()
    if key_file:
        seed["api_key_file"] = key_file
    return seed


async def _standalone_send(pconfig, chat_id: str, message: str, **kwargs) -> Dict[str, Any]:
    """Out-of-process cron delivery is not possible: Reachy needs the live
    WebSocket held by the running gateway adapter."""
    return {
        "error": (
            "Reachy delivery requires the running gateway adapter (persistent "
            "WebSocket to the robot); no standalone send."
        )
    }


def register_platform(ctx) -> None:
    """Register the Reachy platform adapter with the Hermes plugin context."""
    ctx.register_platform(
        name="reachy",
        label="Reachy",
        adapter_factory=lambda cfg: ReachyAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        is_connected=is_configured,
        # Hermes checks required_env names literally, so neither key alternative is listed: the
        # key (REACHY_WS_API_KEY or REACHY_WS_API_KEY_FILE) is enforced by validate_config.
        required_env=["REACHY_WS_PORT"],
        install_hint="pip install 'websockets>=13,<18'",
        env_enablement_fn=_env_enablement,
        cron_deliver_env_var="REACHY_HOME_CHANNEL",
        standalone_sender_fn=_standalone_send,
        allowed_users_env="REACHY_ALLOWED_ROBOTS",
        allow_all_env="REACHY_ALLOW_ALL_ROBOTS",
        max_message_length=MAX_MESSAGE_LENGTH,
        emoji="🤖",
        allow_update_command=True,
        platform_hint=(
            "You are speaking through a Reachy robot: the user talks and hears you "
            "aloud (speech in, speech out). Keep replies concise and natural for TTS; "
            "avoid markdown, code blocks, URLs, and long lists unless asked."
        ),
    )
