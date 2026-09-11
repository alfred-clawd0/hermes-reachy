# hermes-reachy

Give your [Hermes Agent](https://github.com/NousResearch/hermes-agent) a physical
[Reachy Mini](https://www.pollen-robotics.com/reachy-mini/) body — speech in, speech out, and a
bounded set of expressive body actions.

This is a standalone Hermes **plugin** (per the framework's "ship third-party/hardware
integrations as a plugin, not in core" rule). It registers two things:

- **A platform adapter (`reachy`)** — a WebSocket server the robot's voice app connects to as a
  client. Inbound STT text becomes agent messages (the app owns mic / STT / VAD); the agent's
  streamed reply is pushed back as `say` frames the app speaks through its own TTS. Because the
  connection persists, background / cron / proactive `send_message` delivery reaches the robot
  natively.
- **A body tool (`reachy_body`)** — lets the agent `emote` (curated emotion library), `dance`,
  `look` (bounded gotos), `stop`, toggle `head_tracking`, and `chirp` (astromech status sounds).
  The robot app executes with a **client-side allowlist** and bounded motion; the gateway only
  correlates request and response. No raw motor access.

It pairs with the [`reachy-hermes-agent`](https://github.com/ai-ag2026/reachy-hermes-agent)
brain-side runtime and the conversation-app fork, but the plugin only needs the robot voice app
to speak its small WebSocket protocol (see below).

## Install

As a pip plugin:

```bash
pip install hermes-reachy          # once published
# or from a clone:
pip install -e .
```

The Hermes plugin loader discovers it through the `hermes_agent.plugins` entry point. Alternatively
drop `src/hermes_reachy/` into `~/.hermes/plugins/reachy/` (directory plugin).

## Configure

Everything is environment-driven — no hosts, ports, or ids are baked in:

```dotenv
REACHY_WS_PORT=8770            # the robot voice app connects here (required)
REACHY_WS_API_KEY=<random>      # shared secret sent in the initial hello frame
# REACHY_WS_API_KEY_FILE=/path  # alternative: read the shared secret from a file
# REACHY_WS_HOST=127.0.0.1     # keep loopback when the body app runs on this Mac
# REACHY_ALLOWED_ROBOTS=      # comma-separated allowlist of robot ids
# REACHY_ALLOW_ALL_ROBOTS=    # dev only: accept any robot id
# REACHY_HOME_CHANNEL=        # default robot id for proactive / cron delivery + body tool
```

## WebSocket protocol

The robot voice app is the client. Frames are JSON.

```
inbound  (app → adapter):  {"type":"hello","robot_id":"reachy","api_key":"..."}
                           {"type":"stt","text":"...","turn_id":"t1","robot_id":"reachy"}
                           {"type":"interrupt","text":"...","robot_id":"reachy"}
                           {"type":"tool_result","tool_call_id":"...","result":{...}}
outbound (adapter → app):  {"type":"say","kind":"stream","message_id":"m1","content":"...","final":false,"turn_id":"t1"}
                           {"type":"typing","robot_id":"reachy"}
                           {"type":"turn_end","robot_id":"reachy","outcome":"..."}
                           {"type":"tool_call","tool_call_id":"...","action":"emote","params":{"emotion":"happy"}}
```

Streamed replies arrive as progressive `say` edits (same `message_id`, growing `content`); the app
diffs and speaks new clauses. `turn_id` lets the app tell an interactive reply (`origin=turn`) from
an unsolicited proactive delivery (`origin=proactive`).

## Safety

Body actions are advisory requests: the robot app owns the allowlist and bounded execution
(preserved antennas / body-yaw, clamped deltas). The adapter carries text and correlates tool
calls; it never drives motors directly.

## License

MIT.
