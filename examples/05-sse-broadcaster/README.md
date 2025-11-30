# 05 – SSE Broadcaster

Streams `text/event-stream` data by emitting:
- `sse.start` (replaces `http.response.start` for SSE).
- Multiple `sse.send` events with UTF-8 text payloads.
- Stops early if `sse.disconnect` arrives.

Spec references: `docs/specs/www.mkdn` (SSE scope/events).
