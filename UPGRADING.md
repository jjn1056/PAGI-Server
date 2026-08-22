# Upgrading PAGI-Server

## 0.002007

### Outgoing-event validation is now mandatory, in every environment

Previously, validation of the events your application sends via `$send`
(shape checks like a required `status`, and send-order checks like "body
before start") was gated behind the `validate_events` option, which
defaulted to off and was only auto-enabled in development
(`$ENV{PAGI_ENV} eq 'development'`). In production, a malformed or
out-of-sequence send could be silently ignored, silently coerced, or could
corrupt the connection instead of failing cleanly.

As of this release, validation runs unconditionally on every send path
(HTTP/1, HTTP/2, WebSocket, SSE, and lifespan), in every environment. A
malformed, mis-sequenced, unrecognized, or unadvertised-extension event now
fails the `$send` Future instead of being silently ignored or defaulted.
`validate_events` is still accepted for backward compatibility but is
deprecated, ignored, and controls nothing; it will be removed in a future
release.

**What to check:** if your application previously ran only in production
(where validation was off by default) and never exercised `validate_events
=> 1` or `-E development` in testing, upgrade may surface send failures
that were previously silent. Common cases:

- Sending `http.response.start` without a `status` field (or with a
  non-integer `status`).
- Misspelled or unrecognized event `type` values (e.g. `websocket.pong`
  instead of a recognized WebSocket event).
- `more` fields set to something other than `0` or `1`.
- Sending a body chunk before `*.response.start`, sending a duplicate
  start, sending after the response/stream is already complete, or sending
  declared trailers that were never advertised.

**The fix:** send conforming events. Each validation failure names the
exact field or event-order violation in its error message (for example,
`"http.response.start requires 'status' field"` or `"cannot send
'http.response.body' before http.response.start"`), so the failed `$send`
Future's error identifies exactly what to correct.
