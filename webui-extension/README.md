# Reachy Embodiment Panel

Reachy Embodiment Panel is a read-only Hermes WebUI extension for showing whether the Reachy Mini body endpoint is reachable and in a sane idle/ready state.

## Behavior

- Probes the configured Reachy daemon URL from the browser.
- Uses only GET requests:
  - `/api/daemon/status`
  - `/api/daemon/robot-app-lock-status`
  - `/api/motors/status`
  - `/api/media/status`
  - `/api/move/running`
- Shows a trigger only when Reachy needs attention by default:
  - unreachable/offline;
  - partial endpoint failure;
  - movement currently running;
  - motor status mentions error/fault.
- Drawer includes raw bounded JSON snippets for each read-only probe.
- Dashboard link opens the configured dashboard URL in a new tab.

## Non-goals

- No POST requests.
- No movement controls.
- No microphone access.
- No speaker/audio playback.
- No camera capture.
- No daemon restart.

This is deliberately an embodiment status window, not a robot remote control. The latter is how you accidentally invent a haunted doll with admin rights.

## Manual access

- `Ctrl+Shift+R`
- `window.HermesReachyEmbodimentPanel.open()`
- Settings → Extensions → Reachy Embodiment Panel → Visibility → Always show trigger

## Network permission

This extension declares `network_external: true` because the Reachy daemon usually lives at `http://reachy-mini.local:8000`, which is not the WebUI origin. Fetches are read-only and use `credentials: omit`.
