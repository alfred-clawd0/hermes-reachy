"""Reachy robot platform adapter (Hermes gateway plugin).

Transport model
---------------
The adapter runs a **WebSocket server**; the robot's voice app connects to it as a
**client** and stays connected. The app keeps ownership of the real-time audio path
(mic, STT, VAD, TTS synthesis, PCM streaming, half-duplex, barge onset). This adapter
only carries *text*:

  inbound  (app -> adapter):  {"type":"hello","robot_id":"reachy"}
                              {"type":"stt","text":"...","robot_id":"reachy"}
                              {"type":"interrupt","text":"...","robot_id":"reachy"}
  outbound (adapter -> app):  {"type":"say","message_id":"m1","content":"...","final":false}
                              {"type":"typing","robot_id":"reachy"}

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
import json
import logging
import os
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

try:  # optional dep; check_requirements gates instantiation
    from websockets.asyncio.server import serve as ws_serve

    WEBSOCKETS_AVAILABLE = True
except Exception:  # pragma: no cover - import guard
    ws_serve = None  # type: ignore
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


def check_requirements() -> bool:
    """Return whether the WebSocket dependency is available."""
    return WEBSOCKETS_AVAILABLE


def validate_config(config: PlatformConfig) -> bool:
    """Require a port and either an inline key or readable key file."""
    extra = getattr(config, "extra", None) or {}
    port = extra.get("port") or os.getenv("REACHY_WS_PORT", "")
    api_key = str(extra.get("api_key") or os.getenv("REACHY_WS_API_KEY", "")).strip()
    key_file = str(extra.get("api_key_file") or os.getenv("REACHY_WS_API_KEY_FILE", "")).strip()
    return bool(port and (api_key or (key_file and Path(key_file).expanduser().is_file())))


class ReachyAdapter(BasePlatformAdapter):
    """WebSocket-server adapter for the Reachy robot voice app."""

    MAX_MESSAGE_LENGTH = MAX_MESSAGE_LENGTH
    # Enable the gateway's streaming path (progressive edit_message deltas).
    SUPPORTS_MESSAGE_EDITING = True
    # Persistent outbound channel → background/cron/send_message can reach Reachy.
    supports_async_delivery = True

    @property
    def authorization_is_upstream(self) -> bool:
        """Trust identities admitted by the authenticated robot transport."""
        return True

    def __init__(self, config: PlatformConfig):
        super().__init__(config=config, platform=Platform("reachy"))
        extra = getattr(config, "extra", None) or {}
        self._host: str = str(
            extra.get("host") or os.getenv("REACHY_WS_HOST", "127.0.0.1") or "127.0.0.1"
        )
        self._port: int = int(extra.get("port") or os.getenv("REACHY_WS_PORT", "8770"))
        self._api_key = str(extra.get("api_key") or os.getenv("REACHY_WS_API_KEY", "")).strip()
        key_file = str(extra.get("api_key_file") or os.getenv("REACHY_WS_API_KEY_FILE", "")).strip()
        if not self._api_key and key_file:
            try:
                self._api_key = Path(key_file).expanduser().read_text(encoding="utf-8").strip()
            except OSError as exc:
                logger.error("[reachy] could not read API key file %s: %s", key_file, exc)
        configured_robots = extra.get("allowed_robots") or os.getenv("REACHY_ALLOWED_ROBOTS", DEFAULT_ROBOT_ID)
        if isinstance(configured_robots, str):
            self._allowed_robots = {item.strip() for item in configured_robots.split(",") if item.strip()}
        else:
            self._allowed_robots = {str(item).strip() for item in configured_robots if str(item).strip()}
        self._server: Optional[Any] = None
        # robot_id -> active websocket connection
        self._robots: Dict[str, Any] = {}
        # robot_id -> turn_id of the most recently dispatched client turn (see _current_turn_id)
        self._turn_ids: Dict[str, str] = {}
        # tool_call_id -> Future awaiting the robot's tool_result (body-tool surface)
        self._tool_futures: Dict[str, "asyncio.Future"] = {}

    # ── lifecycle ──────────────────────────────────────────────────────────
    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not WEBSOCKETS_AVAILABLE:
            logger.warning("[reachy] websockets not installed (pip install websockets)")
            return False
        # keep the gateway journal clean of non-ws probe tracebacks
        _ws_logger = logging.getLogger("websockets.server")
        if not any(isinstance(f, _HandshakeNoiseFilter) for f in _ws_logger.filters):
            _ws_logger.addFilter(_HandshakeNoiseFilter())
        try:
            self._server = await ws_serve(self._handle_conn, self._host, self._port)
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
        robot_id = self._robot_id_from_path(path)
        try:
            raw_hello = await asyncio.wait_for(websocket.recv(), timeout=10.0)
            hello = self._parse_frame(raw_hello)
            supplied_key = str(hello.get("api_key") or "")
            if hello.get("type") != "hello" or not self._api_key or not hmac.compare_digest(
                supplied_key, self._api_key
            ):
                logger.warning("[reachy] rejected unauthenticated robot connection")
                await websocket.close(code=1008, reason="authentication required")
                return
            robot_id = str(hello.get("robot_id") or robot_id).strip() or robot_id
            if robot_id not in self._allowed_robots:
                logger.warning("[reachy] rejected robot id %s", robot_id)
                await websocket.close(code=1008, reason="robot not allowed")
                return
            self._robots[robot_id] = websocket
            logger.info("[reachy] robot connected: %s (path=%r)", robot_id, path)
            async for raw in websocket:
                robot_id = await self._on_inbound(robot_id, raw, websocket)
        except asyncio.CancelledError:  # pragma: no cover
            raise
        except Exception as e:
            logger.debug("[reachy] connection loop ended for %s: %s", robot_id, e)
        finally:
            # only drop the mapping if it still points at this socket
            if self._robots.get(robot_id) is websocket:
                self._robots.pop(robot_id, None)
            logger.info("[reachy] robot disconnected: %s", robot_id)

    @staticmethod
    def _robot_id_from_path(path: str) -> str:
        seg = (path or "").strip("/").split("/")[-1].split("?")[0].strip()
        return seg or DEFAULT_ROBOT_ID

    async def _on_inbound(self, robot_id: str, raw: Any, websocket: Any) -> str:
        """Parse one inbound frame; returns the (possibly updated) robot_id."""
        frame = self._parse_frame(raw)
        if not frame:
            return robot_id

        ftype = str(frame.get("type") or "").lower()
        if ftype == "hello":
            return robot_id
        new_id = str(frame.get("robot_id") or "").strip()
        if new_id and new_id != robot_id:
            logger.warning("[reachy] ignored robot id change from %s to %s", robot_id, new_id)
            return robot_id

        if ftype in ("stt", "interrupt", "text"):
            text = str(frame.get("text") or "").strip()
            if text:
                turn_id = str(frame.get("turn_id") or "").strip() or None
                await self._dispatch_text(robot_id, text, turn_id=turn_id)
        elif ftype == "tool_result":
            tcid = str(frame.get("tool_call_id") or "").strip()
            fut = self._tool_futures.get(tcid)
            if fut is not None and not fut.done():
                fut.set_result(frame.get("result") if isinstance(frame.get("result"), dict) else {})
        else:
            logger.debug("[reachy] unhandled frame type %r from %s", ftype, robot_id)
        return robot_id

    @staticmethod
    def _parse_frame(raw: Any) -> Dict[str, Any]:
        """Parse one JSON object frame."""
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="replace")
        raw = (raw or "").strip()
        if not raw:
            return {}
        try:
            frame = json.loads(raw)
        except Exception:
            logger.debug("[reachy] non-JSON frame: %r", raw[:80])
            return {}
        if not isinstance(frame, dict):
            return {}
        return frame

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
        self._tool_futures[tcid] = fut
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
            logger.warning("[reachy] push to %s failed: %s", robot_id, e)
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
    seed: Dict[str, Any] = {"port": int(port)}
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
