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
Frame contents and keys are never logged. The gateway's own allowlist / allow-all / pairing
checks still run on every message — see ``ReachyAdapter.enforces_own_access_policy``.

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
# websockets' own logger, held at INFO in connect(): its DEBUG output dumps frames, and frames
# carry the API key.
_WS_LOGGER = logging.getLogger(__name__ + ".ws")

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
_LOGGABLE_PATH_RE = re.compile(r"[A-Za-z0-9_./:@-]{0,128}")

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


class _ReachyConnection(ServerConnection):
    """Server connection that keeps the hello contract for oversized frames.

    Until the robot authenticates, the protocol's message limit is ``HELLO_MAX_BYTES``
    (``serve(max_size=...)``). websockets rejects an oversized message inside its frame parser
    by calling ``protocol.fail(1009)``; while unauthenticated this wrapper turns that into
    1008 "frame too large" and flags it so the handler logs an auth rejection. It is installed
    when the connection is built, so it also covers frames that arrive before the handler runs.
    ``_handle_conn`` raises the limit to ``MAX_MESSAGE_BYTES`` after the hello; from then on an
    oversized frame closes with the library's usual 1009.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.authenticated = False
        self.hello_too_large = False
        protocol_fail = self.protocol.fail

        def fail(code: int, reason: str = "") -> None:
            if code == CLOSE_MESSAGE_TOO_BIG and not self.authenticated:
                self.hello_too_large = True
                code, reason = CLOSE_POLICY_VIOLATION, "frame too large"
            protocol_fail(code, reason)

        self.protocol.fail = fail


def _set_message_limit(websocket: Any, limit: int) -> None:
    """Change one connection's incoming message limit."""
    protocol = websocket.protocol
    if hasattr(protocol, "max_message_size"):  # websockets >= 16 splits message / fragment limits
        protocol.max_message_size = limit
    else:  # websockets 13-15
        protocol.max_size = limit


# ── configuration ──────────────────────────────────────────────────────────
class ReachyConfigError(ValueError):
    """The Reachy platform configuration is unusable; the message says why (never the key)."""


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


def _parse_port(raw: Any) -> int:
    text = str(raw).strip()
    if not text:
        raise ReachyConfigError("REACHY_WS_PORT is not set")
    try:
        port = int(text)
    except ValueError:
        raise ReachyConfigError(f"REACHY_WS_PORT={text!r} is not an integer") from None
    if not 1 <= port <= 65535:
        raise ReachyConfigError(f"REACHY_WS_PORT={port} is outside 1-65535")
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
    path = Path(key_file).expanduser()
    try:
        key = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise ReachyConfigError(f"API key file {path} is unreadable: {exc.strerror or exc}") from None
    except UnicodeDecodeError:
        raise ReachyConfigError(f"API key file {path} is not valid UTF-8") from None
    if not key:
        raise ReachyConfigError(f"API key file {path} is empty")
    return _key_bytes(key)


def _parse_robot_ids(raw: Any) -> frozenset[str]:
    items = raw.split(",") if isinstance(raw, str) else [str(item) for item in raw or ()]
    return frozenset(item.strip() for item in items if item.strip())


def _load_settings(config: Any) -> _Settings:
    """Single source of truth for validate_config and the adapter; raises ReachyConfigError."""
    extra = getattr(config, "extra", None) or {}
    port = _parse_port(_setting(extra, "port", "REACHY_WS_PORT"))
    host = str(_setting(extra, "host", "REACHY_WS_HOST")).strip() or DEFAULT_HOST
    api_key = _load_api_key(extra)
    # Env first, like the gateway's own allowlist check, so both layers see the same list.
    raw_robots = os.getenv("REACHY_ALLOWED_ROBOTS", "")
    if not raw_robots.strip():
        raw_robots = extra.get("allowed_robots") or ""
    robots = _parse_robot_ids(raw_robots)
    invalid = sorted(robot for robot in robots if not _ROBOT_ID_RE.fullmatch(robot))
    if invalid:
        raise ReachyConfigError(
            f"REACHY_ALLOWED_ROBOTS has invalid robot id(s) {', '.join(map(repr, invalid))}: "
            "use 1-64 characters from A-Z a-z 0-9 _ . : @ -"
        )
    return _Settings(
        host=host,
        port=port,
        api_key=api_key,
        # Unset, empty or all-blank means the default robot — never an empty allowlist.
        allowed_robots=robots or frozenset({DEFAULT_ROBOT_ID}),
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


def _peer(websocket: Any) -> str:
    addr = getattr(websocket, "remote_address", None)
    if isinstance(addr, tuple) and len(addr) >= 2:
        return f"{addr[0]}:{addr[1]}"
    return "unknown peer"


def _path_for_log(path: str) -> str:
    """The request path without its query string (clients may put tokens there)."""
    bare = (path or "").split("?", 1)[0]
    return bare if _LOGGABLE_PATH_RE.fullmatch(bare) else "<redacted>"


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
        # tool_call_id -> (robot_id it was sent to, Future awaiting that robot's tool_result)
        self._tool_futures: dict[str, tuple[str, asyncio.Future]] = {}
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
        gateway_ids = _parse_robot_ids(os.getenv("GATEWAY_ALLOWED_USERS", ""))
        if not gateway_ids or "*" in gateway_ids:
            return
        denied = sorted(self._allowed_robots - gateway_ids)
        if denied:
            logger.warning(
                "[reachy] GATEWAY_ALLOWED_USERS is set but REACHY_ALLOWED_ROBOTS is not: the "
                "gateway will deny robot id(s) %s — set REACHY_ALLOWED_ROBOTS explicitly",
                ", ".join(denied),
            )

    # ── lifecycle ──────────────────────────────────────────────────────────
    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not WEBSOCKETS_AVAILABLE:
            logger.warning("[reachy] websockets not installed (pip install websockets)")
            return False
        _WS_LOGGER.setLevel(logging.INFO)  # never dump frames (they carry the key)
        # keep the gateway journal clean of non-ws probe tracebacks
        if not any(isinstance(f, _HandshakeNoiseFilter) for f in _WS_LOGGER.filters):
            _WS_LOGGER.addFilter(_HandshakeNoiseFilter())
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
            logger.error("[reachy] failed to start ws server on %s:%s: %s", self._host, self._port, e)
            return False
        self._mark_connected()
        global _ACTIVE_ADAPTER
        _ACTIVE_ADAPTER = self
        # The adapter's home loop: tool handlers may run in a DIFFERENT loop (the registry's
        # async bridge) — call_robot_tool must execute here or its future never wakes.
        self._loop = asyncio.get_running_loop()
        logger.info("[reachy] ws server listening on %s:%s", self._host, self._port)
        if not _is_loopback(self._host):
            logger.warning(
                "[reachy] listening on non-loopback %s: ws:// carries the API key in plaintext — "
                "prefer an SSH tunnel or a TLS proxy",
                self._host,
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
        logger.info("[reachy] robot connected: %s (path=%s)", robot_id, _path_for_log(path))
        try:
            async for raw in websocket:
                await self._on_inbound(robot_id, raw)
        except asyncio.CancelledError:  # pragma: no cover
            raise
        except Exception as e:
            # type only: exception text (e.g. a close reason) is client-controlled
            logger.debug("[reachy] connection loop ended for %s: %s", robot_id, type(e).__name__)
        finally:
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
        """Run the hello handshake: return the admitted robot id, or None once closed."""
        try:
            raw = await asyncio.wait_for(websocket.recv(), timeout=HELLO_TIMEOUT_S)
        except TimeoutError:
            return await self._reject(websocket, f"no hello within {HELLO_TIMEOUT_S:g}s", "hello timeout")
        except ConnectionClosed:
            if getattr(websocket, "hello_too_large", False):
                # _ReachyConnection already closed it with 1008 "frame too large".
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
        if not self._robot_allowed(robot_id):
            return await self._reject(websocket, f"robot id {robot_id!r} not allowed", "robot not allowed")
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
        """Complete a pending tool call — only for the robot it was sent to."""
        pending = self._tool_futures.get(str(frame.get("tool_call_id") or "").strip())
        if pending is None:
            logger.debug("[reachy] tool_result from %s matches no pending call", robot_id)
            return
        owner, fut = pending
        if owner != robot_id:
            logger.warning("[reachy] ignored tool_result from %s for a call sent to %s", robot_id, owner)
            return
        if not fut.done():
            result = frame.get("result")
            fut.set_result(result if isinstance(result, dict) else {})

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
        a client-side allowlist; this side only correlates request and response."""
        if robot_id not in self._robots:
            return {"error": f"robot {robot_id} not connected"}
        tcid = uuid.uuid4().hex
        fut: "asyncio.Future" = asyncio.get_running_loop().create_future()
        self._tool_futures[tcid] = (robot_id, fut)
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
        install_hint="pip install websockets",
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
