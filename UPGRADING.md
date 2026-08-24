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

### The Net::HTTP2::nghttp2 floor is raised from 0.008 to 0.009

HTTP/2 trailers support (`http.response.trailers`) needs `submit_trailer`
and the three-value data-callback return, both added in
`Net::HTTP2::nghttp2` 0.009. The floor is now enforced at load time,
regardless of the cpanfile's "recommends" phrasing: constructing a
`PAGI::Server` with `http2 => 1` (or starting `pagi-server` with `--http2`)
against an installed `Net::HTTP2::nghttp2` below 0.009 now dies immediately
instead of starting, with:

```
HTTP/2 support requested but Net::HTTP2::nghttp2 is not installed, or is
older than 0.009.

To install:
    cpanm Net::HTTP2::nghttp2

Or disable HTTP/2:
    http2 => 0
```

**What to check:** any deployment that requests `http2 => 1` (or `--http2`)
against a pinned or system-installed `Net::HTTP2::nghttp2` older than 0.009.
HTTP/1.1-only deployments (no `http2 => 1`) are unaffected.

**The fix:** `cpanm Net::HTTP2::nghttp2` to pick up 0.009 or later, or drop
`http2 => 1` / `--http2` to run HTTP/1.1 only.

### `lifespan_mode => 'off'` is removed

The PAGI Lifespan spec now forbids a server from skipping the lifespan
protocol entirely ("A server must not offer an 'off' switch for this
protocol"). Passing `lifespan_mode => 'off'` now dies wherever it can be
supplied: the `PAGI::Server` constructor, the `configure` setter, and
`pagi-server --lifespan off`.

**What to check:** any deployment or app config that passes
`lifespan_mode => 'off'`, or invokes `pagi-server --lifespan off`.

**The fix:** drop the option, or pass `lifespan_mode => 'auto'` explicitly
(the default). `auto` is decline-tolerant: an application that does not
implement the lifespan scope simply declines it and the server continues
startup normally, at effectively no cost -- there is no need to replace
`'off'` with anything to keep an app that never used lifespan working
unchanged. Use `lifespan_mode => 'on'` instead only if a lifespan decline
should be treated as a fatal startup failure.

### SSE connection detection is now an exact media-range match

Previously, a request was routed to the `sse` scope whenever any `Accept`
header value contained the substring `text/event-stream` anywhere in it,
matched case-sensitively. Detection is now PAGI's media-range client-signal
check: the combined `Accept` header values are parsed as a comma-separated
list of media ranges, and the `sse` scope is assigned only when the exact
range `text/event-stream` appears, case-insensitively, with an effective
quality value greater than zero (`q=0` is an explicit refusal and never
signals SSE; wildcard ranges such as `*/*` and `text/*` do not either, on
both the old and new detection -- a bare wildcard was never treated as an
SSE signal).

**What to check:** a client that previously got routed to `sse` only
because its `Accept` header happened to contain the raw text
`text/event-stream` as a substring while explicitly declining it -- for
example `Accept: text/event-stream;q=0` -- now correctly receives the
`http` scope instead. A real SSE client (`EventSource`,
`fetch-event-source`, curl/httpie sending a bare `Accept:
text/event-stream`) sends the exact media type and is unaffected. Detection
is also now case-insensitive, so a client sending an unusual case like
`Accept: TEXT/EVENT-STREAM` now correctly gets `sse` where it previously
did not.

**The fix:** none needed for a well-behaved SSE client. If your application
relied on the old substring/case-sensitive quirk to route non-SSE requests
into the `sse` scope, send the exact `Accept: text/event-stream` media
range (with `q` greater than zero, or omitted) to opt in explicitly.
