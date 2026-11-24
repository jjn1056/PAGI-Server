# Upgrading PAGI-Server

## 0.003000

### Perl 5.24 is required

PAGI-Server now requires Perl 5.24 (it was 5.18). Future 0.54, which PAGI is
built on, requires 5.20, and 5.24 is where Future::AsyncAwait passes a cancel
on to what an `async sub` awaits. See "Perl version support" in
[PAGI](https://metacpan.org/pod/PAGI) for why, and for the support horizon.

### Production logs are JSON

In production mode (`--env production`, `PAGI_ENV=production`, or no
terminal on STDIN) `pagi-server` writes its diagnostics to STDERR as JSON
lines instead of text. Development mode, and `PAGI::Server->new` used
directly, still default to text.

Before (text):

```
Worker 2 started
```

After (one JSON object per line; a worker's lines also carry `worker`):

```
{"time":"2026-10-01T22:25:20.892Z","level":"info","category":"PAGI::Server","message":"Worker 2 started","pid":74739}
```

**To keep text:** pass `--log-format text`. JSON needs Cpanel::JSON::XS
(recommended, not required); without it production falls back to text and
the startup banner says so. An enabled access log (`--access-log FILE`) in
production is JSON too; `--access-log-format` overrides it.

### PAGI Www 0.6: refusals, SSE endings, WebSocket closes

The server implements sub-spec 0.6. The changes an application can feel:

- A WebSocket or SSE refusal is an ordinary `http.response.*` sent before
  `websocket.accept` or `sse.start`. `websocket.http.response.*` and
  `sse.http.response.*` are gone, and `websocket.close` before accept fails.
- A started SSE stream ends cleanly only with `sse.close`; returning without
  it is an incomplete response.
- A WebSocket close is clean only once the transport (or HTTP/2 stream)
  closes, not when the Close frame is sent.
- The application receives `websocket.disconnect` when the scope has ended:
  after the transport closes, or on HTTP/2 after the client ends its stream
  (bounded by `ws_close_timeout`). It used to arrive at the peer's Close,
  while `pagi.connection` still said connected.
- `pagi.connection`'s `response_started` turns true when `websocket.accept`
  or `sse.start` is accepted, as it does for `http.response.start`.

`PAGI::Upgrading` in the PAGI distribution has before/after code for each.

### HTTP/2 needs Net::HTTP2::nghttp2 0.013

Upgrade it (`cpanm Net::HTTP2::nghttp2`) to keep `--http2` working. Earlier
versions sent only the first 16384 bytes of a response body given in one
send, then ended the stream cleanly, so larger responses over HTTP/2 arrived
cut short with no error.

### Request targets that are not paths are refused

`GET http://example.com/a` is now delivered as `path` `/a` (as RFC 9112
requires), and answered 400 if its host differs from the `Host` header. A
target that is not a path (`GET a/b`), or `*` with a method other than
`OPTIONS`, is answered 400. `path` and `raw_path` now always start with `/`
(or are `*`). Ordinary clients never send these; check any tooling that
does.

### Receives after a scope has ended are bounded

Every `receive` after a scope's end reports that end. An application that
never checks for it used to loop on resolved Futures forever, starving
other connections; after `max_disconnect_receives` (default 100) such
receives now fail. Check for the end event, or set
`--max-disconnect-receives 0` to restore unbounded re-delivery.

### PAGI Www 0.7: `run_until_end` on the connection object

`$scope->{'pagi.connection'}` has `run_until_end($code)`: run work that should
stop when the client leaves in it, and send the terminal event only when the
work finished. See PAGI::Upgrading, "0.6 TO 0.7", in PAGI.

### PAGI Www 0.7: sends go out in the order they were called

An application may now issue a `$send` before an earlier one has resolved,
and the server processes a scope's sends in call order, each whole, the
terminal event last. Scopes report `spec_version` `'0.7'`. Code that
serialises its own sends (awaiting each before issuing the next) keeps
working and no longer needs to. Upgrade PAGI-Tools with it: its helpers now
leave send ordering to the server. What you may notice:

- A send issued while an earlier one waited for the client is no longer
  written first.
- An SSE event or WebSocket message still waiting when the application sends
  `sse.close` or `websocket.close` is now delivered before the close (it was
  dropped, or on HTTP/1.1 SSE written after the end of the stream).
- Trailers sent while a file or filehandle body still streams follow the
  whole body.
- `buffered_amount` counts sends accepted and not yet written, including
  those waiting their turn, and `on_drain` fires once that total is below the
  low watermark: a burst of unawaited sends no longer fires it early.
- A burst of unawaited sends waits its turn in the server rather than piling
  into the write buffer.

### Many streaming responses take turns

Awaiting a `$send` returned to the event loop only when the send had to
wait, so a response sent in small pieces ran until its buffer was full, and
many such responses ran back to back while every other connection on the
worker waited (about a second behind 50 of them). A worker now runs
`max_sends_per_turn` (default 384) sends that leave their scope open --
`http.response.body` with `more` (also for a response that has no body on
the wire: `HEAD`, 204, 304), `sse.send`, `sse.comment`, `websocket.send` --
and then returns to the loop. A send past that count, and past four more
for its scope, has done its work; its Future resolves when its scope's
turn comes round, no sooner than the loop's next poll. What you may notice:

- A send's Future can be pending when the client is not slow. Do not read
  `is_ready` after a send as backpressure; use `pagi.transport`.
- `on_high_water` and `on_drain` now fire on every drain cycle for an
  application that awaits its sends (it got one `on_high_water` and no
  `on_drain`).
- A send whose Future was dropped without being awaited still delivers its
  bytes if it is one that is held, but Future::AsyncAwait warns that a
  suspended sub `lost its returning future` when it resumes. Await every
  send, or keep its Future.
- Over HTTP/2 these sends hand their frames to the connection once per loop
  pass, not inside each send.

Set `max_sends_per_turn => 0` (or `--max-sends-per-turn 0`) to turn it off:
no send is held, and over HTTP/2 each send hands its frames over itself, as
before.

### A client that stops reading is disconnected after 60 seconds

While bytes wait to be written to a client, a client that takes none of
them for `write_timeout` seconds (default 60) is now disconnected: the
application sees the disconnect with reason `write_timeout` (a WebSocket
gets code `1006`), and on HTTP/2 only the stalled stream is reset. A client
reading slowly but steadily, and a connection with nothing waiting to be
written, are never cut off. Before, such a client held its connection, and
the application's queued output, for as long as it stayed connected. Set
`write_timeout => 0` (or `--write-timeout 0`) to restore that.

### Sends the wire can't carry now fail

The server now fails a `$send` it used to write as given, and writes
nothing for it (except an `fh` body, below):

- A header name that isn't an RFC 9110 token (`set-cookie:a=b; x`), or a
  value with a control character or leading or trailing whitespace. A
  padded `te: trailers` on HTTP/2 is one of these.
- A status outside 200 to 599: `'0204'`, `600`, or `101` on an SSE or
  WebSocket refusal.
- A `content-length` that isn't one decimal number, or appears twice.
- Over HTTP/1.x, a body that would go past its declared `content-length`,
  or a final body send that leaves bytes owing. HEAD, 204, 304 and HTTP/2
  aren't counted. A `file` counts what it holds, whatever `length` says. An
  `fh` is counted as it streams, so it fails having written what it read:
  an `fh` without a `length` writes up to the declared length, and one that
  ends early writes what it had. After either, the application may send the
  bytes still owed.

Before, the client read these as something the application never meant: a
header split in two, or the next response on a kept-alive connection read
from the wrong bytes. The fix is in the application: send the right length,
or leave `content-length` out and let the server frame the body. The
messages name the event, the header by index, and the problem, never the
header's value:
`http.response.start header 1 ('x-v'): value contains CR (\x0d) at offset 1`.

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
