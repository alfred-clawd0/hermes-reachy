# hermes-reachy

Give your [Hermes Agent](https://github.com/NousResearch/hermes-agent) a physical
[Reachy Mini](https://www.pollen-robotics.com/reachy-mini/) body — speech in, speech out, and a
bounded set of expressive body actions.

> This repository is a fork of [ai-ag2026/hermes-reachy](https://github.com/ai-ag2026/hermes-reachy),
> maintained at [alfred-clawd0/hermes-reachy](https://github.com/alfred-clawd0/hermes-reachy). The original
> MIT license and author credit are retained.

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

It pairs with the [`reachy-hermes-agent`](https://github.com/alfred-clawd0/reachy-hermes-agent)
brain-side runtime and the
[conversation-app fork](https://github.com/alfred-clawd0/reachy_mini_conversation_app/tree/local-agent-backend), but the plugin only needs the robot voice app
to speak its small WebSocket protocol (see below).

## Breaking change in 0.2.0

The robot WebSocket now requires authentication. When upgrading from 0.1.x:

- set `REACHY_WS_API_KEY` (or `REACHY_WS_API_KEY_FILE`) — the platform refuses to start without a
  key, and logs why;
- update the voice app to send `api_key` in its first `hello` frame (companion change in
  [`alfred-clawd0/reachy_mini_conversation_app`](https://github.com/alfred-clawd0/reachy_mini_conversation_app)).
  Older clients are closed with `1008 api_key required`;
- the default bind address moved from `0.0.0.0` to `127.0.0.1` — see [Where to bind](#where-to-bind)
  if the voice app runs on another machine;
- a connection can no longer change its `robot_id` after the hello.

## Install

As a pip plugin:

```bash
pip install hermes-reachy          # once published
# or from a clone:
git clone https://github.com/alfred-clawd0/hermes-reachy.git && cd hermes-reachy
pip install -e .
```

Supported websockets releases are 13.x through 17.x (`websockets>=13,<18`). The adapter maps oversized
pre-auth frames to 1008 through a websockets internal, which was verified on those releases. If a later
release changes it, the adapter falls back to the library's own 1009 and logs a warning once.

The Hermes plugin loader discovers it through the `hermes_agent.plugins` entry point. Alternatively
drop `src/hermes_reachy/` into `~/.hermes/plugins/reachy/` (directory plugin).

## Configure

Everything is environment-driven — no hosts, ports, or ids are baked in (see `.env.example`):

```dotenv
REACHY_WS_PORT=8770             # the robot voice app connects here (required)
REACHY_WS_API_KEY=<random>      # shared secret the app sends in its first hello (required…)
# REACHY_WS_API_KEY_FILE=/path  # …or read it from a file instead (REACHY_WS_API_KEY wins if both)
# REACHY_WS_HOST=127.0.0.1      # bind address (default 127.0.0.1, loopback)
# REACHY_ALLOWED_ROBOTS=reachy  # comma-separated robot ids; unset or empty = reachy
# REACHY_ALLOW_ALL_ROBOTS=      # dev only: accept any robot id (the key is still required)
# REACHY_HOME_CHANNEL=          # default robot id for proactive / cron delivery + body tool
```

Generate a key with `python -c "import secrets; print(secrets.token_urlsafe(32))"` and give the
same value to the voice app. Exactly one of the two key variables is needed. The plugin manifest
lists both as optional because Hermes' installer checks each required name literally, and a
key-file-only setup would otherwise be reported as incomplete. The gateway refuses to start the
platform, and logs why, when:
- the port is not a valid 1-65535 integer;
- no key is configured;
- the key file is missing, unreadable or empty;
- an allowlisted robot id is malformed.

### Where to bind

The default bind address is `127.0.0.1`: the voice app normally runs on the same Mac as Hermes (it
reaches the robot itself over the network), so the WebSocket never has to leave the machine.

Set `REACHY_WS_HOST=0.0.0.0` only if the voice app runs on **another machine**. `ws://` is not
encrypted, so on a LAN the API key and every utterance travel in plaintext. Prefer keeping the
adapter on loopback and reaching it through an SSH tunnel (on the voice-app machine:
`ssh -N -L 8770:127.0.0.1:8770 you@hermes-host`, then connect to `ws://127.0.0.1:8770`) or a
TLS-terminating reverse proxy (`wss://`). The adapter logs a warning when it binds a non-loopback
address.

### Who may talk to the agent

Two layers, both active:

1. **Transport (this adapter)** — the hello must carry the API key, and its `robot_id` must be in
   `REACHY_ALLOWED_ROBOTS` (default `reachy`) unless `REACHY_ALLOW_ALL_ROBOTS=true`.
2. **Gateway** — Hermes' usual checks still run on every message: `REACHY_ALLOW_ALL_ROBOTS`, the
   `REACHY_ALLOWED_ROBOTS` allowlist, `GATEWAY_ALLOWED_USERS`, and approved pairings. When no env
   allowlist is set at all, the gateway defers to the adapter's allowlist, so the default `reachy`
   robot works out of the box.

If you set `GATEWAY_ALLOWED_USERS` (for example for Telegram users), the gateway applies it to
robots too: set `REACHY_ALLOWED_ROBOTS` explicitly (or add the robot ids to
`GATEWAY_ALLOWED_USERS`). The adapter logs a warning at startup when an allowlisted robot would be
denied this way.

## WebSocket protocol

The robot voice app is the client. Frames are JSON.

### Handshake

1. The **first** frame must be a hello, sent within **10 s** of connecting:
   `{"type":"hello","robot_id":"reachy","api_key":"<REACHY_WS_API_KEY>"}`.
   If `robot_id` is omitted, the last URL path segment is used (`ws://host:8770/robot/kitchen` →
   `kitchen`), else `reachy`. A robot id is 1–64 characters from `A-Z a-z 0-9 _ . : @ -`.
2. Until the hello is accepted, every frame is limited to **4 KiB** (a hello is ~100 bytes).
   Don't pipeline larger frames right behind the hello; after it is accepted the limit is 1 MiB, and
   anything bigger closes with websockets' standard 1009.
3. The key is compared in constant time (any UTF-8 string works) and `robot_id` is checked against
   the allowlist.
4. Any failure closes the socket with **1008 (policy violation)** and one of these reasons:
   `hello timeout`, `hello required` (the first frame was not a JSON-object hello),
   `frame too large`, `api_key required`, `authentication failed`, `invalid robot_id`,
   `robot not allowed`, or `too many pending connections` (at most 16 sockets may wait in the
   hello phase at once). These are configuration or overload errors, so a client should back off
   rather than reconnect in a tight loop on 1008.
5. The `robot_id` is fixed for the life of the connection: later frames that carry a different
   `robot_id` are dropped, and a second hello is ignored.
6. If a robot id authenticates while an earlier connection for the same id is still open, the
   older socket is closed with `1000 superseded by a newer connection`, and the newest one wins.
7. A `tool_result` only completes a `tool_call` that was sent to the same robot. If a call is still
   pending when its connection closes (superseded or dropped), it fails at once with
   `{"ok": false, "error": "robot disconnected"}`, unless the robot's replacement connection has
   already answered it.
8. A `robot_id` that contains the API key is rejected (`invalid robot_id`).

The adapter never logs frame contents, keys, URL paths or query strings, rejected robot ids, or
configuration values. Its rejection logs name only a category (for example `invalid JSON` or
`robot_id not allowlisted`) and the peer. Configuration errors name the variable and, for allowlist
entries, the entry's position. Only admitted robot ids are logged. websockets gets a logger that
never emits or enables DEBUG, whatever the logging configuration: its debug output dumps frames,
and frames carry the key.

### Frames

```
inbound  (app → adapter):  {"type":"hello","robot_id":"reachy","api_key":"..."}   (first frame only)
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
