"""hermes-reachy — embody a Hermes agent in the Reachy Mini robot.

Registers two things with the Hermes plugin system:

- a **platform adapter** (``reachy``): a WebSocket server the robot's voice app connects to,
  carrying inbound STT text and outbound streamed replies (speech in / speech out);
- a **body tool** (``reachy_body``): lets the agent play emotions, dance, look, stop, toggle
  face-tracking, and chirp — over the same connection, with client-side bounded execution.

Install as a pip plugin (entry point ``hermes_agent.plugins``) or drop this package into
``~/.hermes/plugins/reachy/``.

The Reachy platform *adapter* subclasses Hermes gateway base classes, so it is imported lazily
inside :func:`register` (where the gateway is present). Importing this package on its own — e.g.
to use the ``reachy_body`` schema/handler — does not require hermes-agent to be installed.
"""
from __future__ import annotations

from .body_tool import REACHY_BODY_SCHEMA, handle_reachy_body, register_body_tool

__all__ = [
    "register",
    "register_body_tool",
    "handle_reachy_body",
    "REACHY_BODY_SCHEMA",
]

__version__ = "0.2.0"


def register(ctx) -> None:
    """Plugin entry point — called by the Hermes plugin system at startup.

    Registers the Reachy platform adapter and the ``reachy_body`` tool together. The adapter
    module (which imports Hermes gateway base classes) is imported here, not at package load.
    """
    from .adapter import register_platform

    register_platform(ctx)
    register_body_tool(ctx)
