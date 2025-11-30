# 04 – WebSocket Echo

Handles the WebSocket protocol:
1. Waits for `websocket.connect`.
2. Sends `websocket.accept` to complete the handshake.
3. Echoes incoming frames back via `websocket.send`.
4. Stops when `websocket.disconnect` arrives or when `websocket.receive` contains neither `text` nor `bytes`.

Spec references: `docs/specs/www.mkdn` (WebSocket scope & events).
