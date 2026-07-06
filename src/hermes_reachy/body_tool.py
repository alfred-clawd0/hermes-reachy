"""``reachy_body`` — the agent's body-tool surface for the Reachy Mini.

One tool lets the agent drive its own body over the existing Reachy platform WebSocket:
the gateway adapter pushes a ``tool_call`` frame, the on-robot app executes it through its
MovementManager / tool registry (client-side allowlist, bounded moves) and answers with a
``tool_result``. This module only correlates the two — no raw motor access, no unbounded motion.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

logger = logging.getLogger(__name__)

REACHY_BODY_PARAMETERS = {
    "type": "object",
    "properties": {
        "action": {
            "type": "string",
            "enum": ["emote", "dance", "look", "stop", "head_tracking", "chirp"],
            "description": (
                "emote: play one emotion from the curated library (field 'emotion'); "
                "dance: play a short dance; "
                "look: look in a direction (field 'direction'); "
                "stop: immediately stop any running movement/emotion; "
                "head_tracking: toggle face-following on/off (field 'enabled'); "
                "chirp: play an astromech-style status sound (field 'name')."
            ),
        },
        "emotion": {
            "type": "string",
            "description": (
                "Emotion intent for action=emote, e.g. happy, excited, loving, grateful, success, "
                "thinking, attentive, confused, uncertain, sad, angry, scared, surprised, amazed, "
                "calming, tired, sleepy, yes, no, welcoming, greeting, goodbye, helpful, random."
            ),
        },
        "direction": {
            "type": "string",
            "enum": ["left", "right", "up", "down", "front"],
            "description": "Look direction for action=look (front = re-center).",
        },
        "enabled": {"type": "boolean", "description": "For action=head_tracking."},
        "name": {
            "type": "string",
            "description": (
                "Chirp name for action=chirp: acknowledge, working, ready, done, error, affirm, "
                "negative, curious, notify, wake, sleep."
            ),
        },
    },
    "required": ["action"],
}

REACHY_BODY_SCHEMA = {
    "name": "reachy_body",
    "description": (
        "Control your Reachy Mini body: play emotions, dance, look in a direction, stop "
        "movement, toggle face-tracking, or play a status chirp."
    ),
    "parameters": REACHY_BODY_PARAMETERS,
}


def _resolve_active_adapter():
    """Find the running Reachy platform adapter instance (same process, no extra transport).

    The adapter and this tool ship in one package, so the live instance lives in
    ``hermes_reachy.adapter``. A sys.modules fallback keeps working if the platform was
    loaded under a legacy directory-plugin namespace.
    """
    import sys

    try:
        from hermes_reachy.adapter import get_active_adapter

        adapter = get_active_adapter()
        if adapter is not None:
            return adapter
    except Exception:
        pass
    for name in ("hermes_plugins.reachy_platform.adapter", "hermes_plugins.reachy.adapter"):
        mod = sys.modules.get(name)
        if mod is not None:
            try:
                return mod.get_active_adapter()
            except Exception:
                continue
    return None


async def handle_reachy_body(args: dict, **kwargs: Any) -> str:
    """Forward one body action to the robot app and return its result."""
    adapter = _resolve_active_adapter()
    if adapter is None:
        return json.dumps({"error": "reachy platform not running"})
    if isinstance(args, str):  # defensive: some paths hand the raw JSON string
        try:
            args = json.loads(args)
        except Exception:
            args = {}
    if not isinstance(args, dict):
        args = {}
    logger.info("reachy_body call: args=%r", args)
    action = str(args.get("action") or "").strip().lower()
    if not action:
        return json.dumps({
            "error": "missing 'action'",
            "hint": "call again with e.g. {\"action\": \"emote\", \"emotion\": \"happy\"}",
        })
    params = {k: v for k, v in args.items() if k != "action" and v is not None}

    # Pick the robot to drive: the configured home robot, else the single connected one.
    robot_id = _target_robot_id(adapter)

    home_loop = getattr(adapter, "_loop", None)
    try:
        running = asyncio.get_running_loop()
    except RuntimeError:
        running = None
    if home_loop is not None and home_loop is not running:
        # The registry's async bridge runs handlers in its own loop; the adapter's ws + futures
        # live in the gateway loop — submit there and await across loops.
        cfut = asyncio.run_coroutine_threadsafe(
            adapter.call_robot_tool(robot_id, action, params), home_loop
        )
        result = await asyncio.wrap_future(cfut)
    else:
        result = await adapter.call_robot_tool(robot_id, action, params)
    return json.dumps(result, ensure_ascii=False)


def _target_robot_id(adapter: Any) -> str:
    """Which connected robot the body action targets. Prefer REACHY_HOME_CHANNEL, then the
    only connected robot, then the adapter default."""
    import os

    home = os.getenv("REACHY_HOME_CHANNEL", "").strip()
    robots = list(getattr(adapter, "_robots", {}) or {})
    if home:
        return home
    if len(robots) == 1:
        return robots[0]
    from hermes_reachy.adapter import DEFAULT_ROBOT_ID

    return DEFAULT_ROBOT_ID


def register_body_tool(ctx) -> None:
    """Register the ``reachy_body`` tool with the Hermes plugin context."""
    ctx.register_tool(
        name="reachy_body",
        toolset="reachy",
        schema=REACHY_BODY_SCHEMA,
        handler=handle_reachy_body,
        is_async=True,
        description=(
            "Control your Reachy Mini body: play emotions, dance, look in a direction, stop "
            "movement, toggle face-tracking, play a status chirp. Use it sparingly and fittingly "
            "for the conversation — you ARE this body."
        ),
        emoji="\U0001f916",
    )
