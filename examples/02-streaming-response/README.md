# 02 – Streaming Response with Disconnect Handling

Shows how to:
- Drain the incoming `http.request` body (if any) before replying.
- Send multiple `http.response.body` chunks with `more => 1`.
- Emit `http.response.trailers` when `trailers => 1` was advertised.
- Watch for `{ type => 'http.disconnect' }` while streaming and stop if the client drops.

Spec references: `docs/specs/www.mkdn` (HTTP events, trailers, disconnect) and `docs/specs/main.mkdn` (cancellation semantics).
