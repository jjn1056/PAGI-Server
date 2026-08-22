# PAGI::Server Protocol Alignment Design

**Date:** 2026-08-21

**Status:** Draft for user review

**Implementation branch:** `fix/pagi-0.4-alignment`

## 1. Executive summary

PAGI::Server already implements most of PAGI's HTTP, WebSocket, SSE,
lifespan, transport, and HTTP/2 surfaces. The application-loading changes on
this branch also correctly implement the PAGI 0.4 rule that a general-purpose
runner accepts either a native application coderef or an already-instantiated,
blessed application provider with `to_app`.

A fresh comparison against the local PAGI specification found a narrower but
important problem: several mandatory protocol rules are enforced only on the
HTTP/1 path, only when development validation is enabled, or not at all. The
largest concentration is HTTP/2, where HTTP bodies, connection state,
WebSocket keepalive, SSE request bodies, and per-stream lifecycle do not yet
have parity with HTTP/1.

This project aligns those existing implementations in one branch and one
review. It does not create a generic conformance suite and it does not change
the PAGI specification. The work is organized as reviewable commits, but the
result is one coherent server-alignment release.

## 2. Work map and authoritative baseline

### 2.1 PAGI::Server repository

- **Repository:** `/Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server`
- **Worktree:** `/Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/pagi-0.4-alignment`
- **Ticket/project:** PAGI 0.4 application loading and protocol alignment
- **Branch:** `fix/pagi-0.4-alignment`
- **Base branch:** `origin/main`
- **Base commit at design time:** `b2290dd91a97a708084f9aed9c2c098ba2016240`
- **Starting branch commit:** `437566fa0d040288b9a75262d5b33111229bab54`
- **Owned changes:** PAGI::Server implementation, tests, examples, POD,
  compliance documentation, Changes, and this design/plan material
- **Deployment boundary:** one PAGI-Server distribution and CPAN release
- **Push target:** `origin/fix/pagi-0.4-alignment`

The branch already contains:

- `a14ac2e` — fail `$send` when a supplied filehandle cannot be read;
- `437566f` — normalize native coderefs and instantiated `to_app` providers in
  accordance with PAGI 0.4.

Those changes remain part of this branch and final review.

**Coordination with `perf/http-serving-25`:** a separate in-flight
performance branch (checked out in the primary PAGI-Server working
directory) rewrites the HTTP/1 send dispatcher and hot paths this project
also touches. Decision (John, 2026-08-21): the perf branch has its own
issues and is handled separately; this alignment project stays based on
`main` (`b2290dd`) and does NOT rebase onto or read from the perf branch.
The alignment work merges first; the perf branch then merges/rebases on top
of the combined result. All code references in this design and its plans
are against the `main`-based worktree, not the perf branch.

### 2.2 PAGI specification repository

- **Repository:** `/Users/jnapiorkowski/Desktop/PAGI-Project/PAGI`
- **Role:** read-only authoritative contract
- **Branch observed at the end of the audit:** `spec-clarifications`
- **Commit observed at the end of the audit:** `4c4abf05d7b157ecbe8d81f36cc15befbc79e7ae`
- **Current baseline after drift check:** `main` at `a7ed9cc` (merge of
  `spec-clarifications`, five commits `84a85d7..8c43abb`)
- **Owned changes:** none
- **Deployment boundary:** none in this project
- **Push target:** none

Another session is actively reviewing PAGI. Therefore the commit above is a
comparison snapshot, not a request to freeze or modify that repository. Before
implementation begins, and again before final verification, the implementer
must compare the relevant current PAGI clauses with this design. Contract drift
is handled by updating this design or recording an explicit deviation; it is
not resolved by editing PAGI from this project.

### 2.3 Specification drift check (2026-08-21, after spec merge)

The spec review completed and merged to PAGI `main` as `a7ed9cc` after this
design's audit snapshot. Five clarifications landed; their disposition here:

1. **`http.response.body` payload keys** (`84a85d7`): at most one of
   `body`/`file`/`fh`; omitting all three means an empty body chunk. This
   design already said "at most one" in section 7.1. No change.
2. **HEAD response semantics** (`4c4abf0`): server suppresses the body on
   every HTTP version; `Content-Length` passthrough; no chunked framing;
   `file`/`fh` never opened or statted; trailers discarded. Section 8.2
   already matches. No change.
3. **Incomplete response after `more => 1`** (`bbf9c09`): NEW normative
   section "Application Left a Response Incomplete". Forced abnormal
   closure on both transports, never synthesized terminal framing,
   `on_disconnect` with reason `server_error` (a standard token, not an
   `x-` reason), never `on_complete`. This puts the HTTP/1 incomplete-
   response path in scope (it previously kept "current backstop behavior")
   and pins the HTTP/2 reason. Sections 9.3, 15.3, and 21 updated.
4. **SSE detection** (`3cac188`): the substring Accept match is no longer
   conforming. Detection is an exact `text/event-stream` media-range match
   with `q > 0`; wildcards and `q=0` never signal SSE. Both server
   detection sites use substring matching today. New section 11.5 and
   tests in 15.5.
5. **Lifespan modes** (`8c43abb`): strict mode (`on`) is sanctioned as
   explicit operator configuration; a decline-tolerant `auto` remains the
   required default; **an `off` switch is now expressly nonconforming**
   ("A server must not offer an 'off' switch for this protocol"). This
   reverses this design's earlier decision to keep `off` as a documented
   override. Section 12 updated; `off` is removed in this project.

## 3. Evidence from the alignment audit

The branch was verified under Perl `5.42.2@default` after installing
`Net::HTTP2::nghttp2 0.008`.

- The complete existing suite passed: 110 files, 587 tests.
- The focused HTTP/2 suite passed: 27 files, 130 tests.
- Focused HTTP/1, WebSocket, SSE, lifespan, connection-state, validation,
  file-response, and runner tests passed: 9 files, 115 tests.
- `t/08-tls.t` skipped because `IO::Async::SSL` is not installed in that Perl
  environment.

The green suite does not disprove the findings below. Direct probes exercised
paths the current tests do not cover and confirmed that:

- an HTTP/2 response without `status` is accepted as 200;
- an HTTP/2 `file` body is emitted as an empty body;
- a valid HTTP/2 trailers event is rejected;
- an HTTP/2 HEAD response transmits body bytes;
- an HTTP/2 stream close leaves `pagi.connection->is_connected` true;
- an unknown HTTP/2 WebSocket send event succeeds silently;
- a `disconnect_future` first requested after clean completion resolves with
  `undef` instead of remaining untriggered;
- an SSE dispatch containing U+00E9 remains a UTF-8-flagged character string
  rather than encoded wire bytes.

## 4. Goals

1. Enforce PAGI's mandatory outgoing-event contract in every environment and
   on every supported protocol/transport path.
2. Give HTTP/2 normal HTTP responses the same PAGI-visible semantics as
   HTTP/1, including HEAD, `file`, `fh`, trailers, backpressure, and terminal
   response handling.
3. Make each HTTP/2 stream own its lifecycle and mutable protocol state.
4. Correct HTTP/2 WebSocket keepalive and disconnect behavior.
5. Correct SSE request-body, Unicode encoding, keepalive, and multiplexing
   behavior on HTTP/1 and HTTP/2.
6. Make lifespan sends obey the base event-validation contract and propagate
   relevant configuration into workers.
7. Remove smaller capability-advertisement and server-header inconsistencies
   encountered in the same paths.
8. Preserve the already-completed application-loading and filehandle fixes.

## 5. Non-goals

This project does not:

- change the PAGI specification;
- build the separate reusable conformance suite intended for other PAGI server
  authors;
- add `response_complete()` merely because the connection-state code is being
  touched; it remains a SHOULD-level capability;
- redesign TLS, certificate extraction, HTTP parsing, the event loop, or the
  public server construction API;
- redesign filehandle I/O around threads or a new asynchronous filehandle API;
- promise that arbitrary application-provided filehandles are non-blocking;
- introduce a new public stream-state class;
- refactor unrelated portions of the large `PAGI::Server::Connection` module;
- make unsupported extensions appear supported.

The project may expose pre-existing application bugs. An application that
sends malformed or out-of-sequence events and happened to work because the
server ignored or defaulted them will now receive a failed `$send` Future.
That behavior change is intentional protocol alignment.

## 6. Governing invariants

### 6.1 Transport-neutral protocol semantics

HTTP/1 and HTTP/2 may use different framing, but a PAGI application must see
the same event rules, resource ownership, HEAD suppression, send completion,
and failure behavior on both transports.

### 6.2 Post-close behavior precedes validation

Before closure, malformed, wrongly typed, out-of-sequence, and unrecognized
outgoing events fail the Future returned by `$send`. After closure, every
`$send` is a successfully resolved no-op, even if the event itself is
malformed.

Every send boundary therefore follows this order:

1. Determine whether the relevant connection or HTTP/2 stream is already
   closed.
2. If closed, return a successful Future without validating or delivering the
   event.
3. Validate the event and its legality in the current protocol state.
4. Consume the event and any attached resource.
5. Resolve only after the server has accepted the output into its outbound
   path and finished consuming any resource tied to that send.

### 6.3 HTTP/2 state is per stream

HTTP/2 multiplexes independent applications over one connection. Response
state, connection-state callbacks, backpressure queues, WebSocket keepalive,
SSE keepalive, idle timers, disconnect reasons, and terminal flags must never
be shared between streams.

### 6.4 Extra fields remain forward-compatible

Validation rejects missing required fields, wrong field types, illegal field
combinations, invalid protocol sequences, and unknown event types. It does not
reject extra fields.

### 6.5 Validation is cheap and mandatory

Mandatory checks must be suitable for production. They consist of event type
dispatch, key existence, reference/primitive shape checks, anchored numeric
checks, header tuple and byte-safety checks, mutually exclusive fields, and
protocol-state checks. Development-only introspection may add better messages
or diagnostics, but it must not determine whether invalid events are accepted.

## 7. Mandatory outgoing-event validation

### 7.1 `PAGI::Server::EventValidator`

The existing validator becomes the shared mandatory validator rather than a
development-only feature. Each supported send family has a complete set of
recognized event types:

- HTTP: `http.response.start`, `http.response.body`,
  `http.response.trailers`, and advertised `http.fullflush`;
- WebSocket: `websocket.accept`, `websocket.send`,
  `websocket.keepalive`, `websocket.close`, and the advertised denial-response
  events;
- SSE: `sse.start`, `sse.send`, `sse.comment`, `sse.keepalive`, `sse.close`,
  `sse.http.response.start`, and `sse.http.response.body`;
- lifespan: the two startup and two shutdown completion/failure events.

Unknown types fail rather than falling through. Extension event types are
accepted only when their extension is advertised on that scope.

Primitive numeric validation uses whole-string anchors (`\A` and `\z`).
Boolean-like PAGI integer fields accept only 0 or 1. Non-negative counts,
offsets, lengths, retry values, and intervals reject signs, whitespace,
newlines, fractional text where an Int is required, references, and undefined
values when the field is required.

Headers are validated as an array of two-element arrays. Names and values are
byte strings and retain duplicates. Header byte-safety rules reject CR, LF,
NUL, and forbidden controls through a failed send Future. Header validation is
shared by normal HTTP, WebSocket denial, and SSE decline/start responses.

Payload combinations are checked before dispatch:

- `http.response.body` permits at most one of `body`, `file`, and `fh`;
- `websocket.send` requires exactly one non-null `bytes` or `text` value;
- required SSE payload/comment fields must be defined strings;
- `sse.send` validates newline restrictions and non-negative retry;
- close codes, statuses, offsets, lengths, and timeout fields have the types
  required by their PAGI event definitions.

### 7.2 `validate_events` configuration

`validate_events => 0` can no longer disable mandatory protocol validation.
For compatibility with existing configuration, the option remains accepted
during this release, but is documented as controlling only any supplemental
development diagnostics. If no supplemental diagnostics remain after the
refactor, it is accepted as a deprecated no-op rather than silently restoring
non-conforming behavior.

The runner's automatic development default may remain, but it cannot affect
whether core validation runs.

### 7.3 Sequence validation

Send dispatchers use explicit states instead of silently returning when an
event arrives at the wrong time.

For HTTP:

- `initial` accepts response start;
- `started` accepts body, an advertised fullflush, and trailers only when the
  start event declared them;
- a terminal body without declared trailers, a file/fh body, or trailers moves
  to `complete`;
- events before start, duplicate start, body after completion, and undeclared
  trailers fail;
- `closed` is the universal successful no-op state.

For WebSocket:

- `connecting` accepts accept, close/rejection, or an advertised denial start;
- `accepted` accepts send, keepalive, and close;
- `denial` accepts only denial body events until their terminal event;
- denial and accepted-frame paths cannot be mixed;
- invalid transitions fail; closed sends are no-ops.

For SSE:

- `initial` accepts `sse.start` or decline start;
- `streaming` accepts send, comment, keepalive, and close;
- `declining` accepts only decline body events;
- start and decline are first-send-wins alternatives;
- sends after `sse.close` fail except a repeated close, which remains
  idempotent;
- transport closure makes later sends no-ops.

Lifespan accepts startup results only while startup is pending and shutdown
results only while shutdown is pending. Unknown and wrong-phase events fail
their send Future.

## 8. HTTP/2 HTTP response parity

### 8.1 Shared event semantics, transport-specific framing

The HTTP/1 and HTTP/2 send paths continue to own their framing, but use shared
validation and shared body-source semantics. The implementation may introduce
a small private helper that consumes `body`, `file`, or `fh` and emits byte
chunks into a transport-specific sink. It must not expose a new public API.

`file` continues to use `PAGI::Server::AsyncFile` for chunked asynchronous
reads. The server opens and closes it. `fh` remains application-owned; the
server reads it and the application may close it only after the send Future
resolves. Read/open/seek failures fail the same Future. Offset, optional length,
offset-past-EOF, and implicit completion behave identically on HTTP/1 and
HTTP/2.

HTTP/2 places file/fh chunks into that stream's existing outbound queue and
uses the same per-stream high/low-watermark mechanism as ordinary streaming
body events. A file response must not be loaded into memory as one scalar.

### 8.2 HEAD

The HTTP/2 send path receives the request method or reads it from immutable
stream scope state. For HEAD:

- response-start headers are emitted normally;
- application `Content-Length` is preserved;
- body bytes are discarded for every body event;
- `file` and `fh` are neither opened nor statted;
- trailers events are accepted and discarded;
- the same terminal event that would complete GET completes HEAD;
- send Futures resolve normally and connection-state completion proceeds as
  for GET.

HTTP/1 retains the same rules. Tests must ensure future refactoring does not
regress its existing suppression.

### 8.3 HTTP/2 trailers

When response start declares `trailers => 1`, a terminal body event does not
end the HTTP/2 stream. A following `http.response.trailers` submits trailing
HEADERS with END_STREAM and marks the response terminal. Without the
declaration, the trailers event fails. On HEAD, it is validated and discarded
while still completing the response.

### 8.4 Fullflush capability

If the `fullflush` extension is advertised on an HTTP/2 HTTP scope,
`http.fullflush` must cause pending HTTP/2 output to be handed to the session's
write path and then resolve. If that behavior cannot be implemented reliably,
the extension must be removed from that scope. Advertising it and rejecting
the event is forbidden.

### 8.5 Completion and resource lifetime

The final send Future resolves only after its event and body resource have
been consumed into the HTTP/2 outbound machinery. This does not assert network
delivery. Stream completion and resource cleanup are safe if the client resets
the stream while a file read or drain wait is pending.

## 9. HTTP/2 lifecycle and error handling

### 9.1 Per-stream connection state

Every HTTP/2 HTTP stream continues to receive its own `pagi.connection` object.
The server wires it into all stream terminal paths:

- normal terminal response and clean application completion call
  `_mark_complete` once;
- client RST_STREAM, abnormal END_STREAM, write failure, timeout, and server
  cancellation call `_mark_disconnected` once with the applicable standard
  reason;
- response start, including a server-generated 500, marks
  `response_started`;
- a stream can transition only once and callbacks fire in the specified order;
- deleting stream bookkeeping cannot occur before pending receive/send Futures
  and connection-state observers have been resolved appropriately.

Two concurrent HTTP/2 streams must have independent connection flags,
callbacks, Futures, response-started state, and reasons.

### 9.2 Late `disconnect_future`

`PAGI::Server::ConnectionState` distinguishes `connected`, `disconnected`, and
`completed`, rather than treating both terminal states as merely not connected.

- First access after an abnormal disconnect returns an already-resolved Future
  carrying the reason.
- First access after clean completion returns a Future that does not resolve
  as a disconnect signal. It must not be completed with `undef`.
- A Future created before clean completion remains pending.
- Disconnect and completion callbacks remain mutually exclusive.

### 9.3 Application failure and incomplete responses

A response is **incomplete** per PAGI's "Application Left a Response
Incomplete" section when the application's Future resolves after response
start but before the terminal event: the final body event with `more => 0`,
a `file`/`fh` body, or — when start declared `trailers => 1` — the trailers
event. The server must make truncation observable to the client and must
never synthesize the missing terminal framing.

For a still-connected HTTP/2 stream:

- if the application fails or returns before response start, emit and log the
  server 500 backstop, end that stream, and mark the server response lifecycle;
- if it fails after response start or returns with an incomplete response,
  reset that stream (RST_STREAM, e.g. INTERNAL_ERROR) without signaling
  END_STREAM and without taking down sibling streams;
- pending producer waits and transport callbacks are released or cancelled;
- the incomplete-response and post-start-failure paths report
  `on_disconnect` with the standard reason `server_error` and never fire
  `on_complete`; other abnormal per-stream states use the applicable
  standard PAGI reason, or an `x-`-prefixed server reason when no standard
  token fits.

If the client already cancelled the stream, do not log an application error
and do not synthesize a 500. Treat later sends as no-ops. Per the same spec
section, a client that disconnected before the application's Future resolved
means no incomplete-response error is logged.

HTTP/1 is equally in scope: its current behavior on an incomplete response
is nonconforming. Today the keep-alive decision ignores whether the body
framing finished, so a chunked response with no terminator can be kept
alive and even serve a pipelined request, and `_mark_complete` fires
`on_complete` unconditionally when the application returns. After this
project, an HTTP/1 incomplete response:

- closes the connection without writing the chunked terminator (or, for a
  `Content-Length`-framed response, before the declared length), so the
  client observes truncation;
- never keeps the connection alive or serves a pipelined request;
- fires `on_disconnect` with reason `server_error`, never `on_complete`;
- logs at error level unless the client had already disconnected.

Both transports remain covered by parity tests for the same
connected/disconnected distinction.

## 10. HTTP/2 WebSocket alignment

### 10.1 Mandatory validation and state

The HTTP/2 WebSocket send path invokes the shared validator and explicit
WebSocket state machine. Missing payloads, duplicate payload forms, wrong
types, illegal denial/accept mixtures, and unknown types fail their Futures.

### 10.2 Per-stream keepalive

`websocket.keepalive` is implemented separately for each accepted HTTP/2
WebSocket stream:

- interval 0 disables that stream's timer;
- a positive interval schedules WebSocket Ping frames as HTTP/2 DATA for that
  stream;
- timeout state and Pong observation belong to that stream;
- a timeout ends only that stream, reports code 1006 and reason
  `keepalive_timeout`, and releases its timer;
- repeated keepalive events update only that stream's settings;
- HTTP/1 continues using its connection-local writer and timer.

The HTTP/1 keepalive helper must not be reused if it writes raw frames directly
to the shared TCP stream.

### 10.3 Disconnect codes and duplicate delivery

A peer Close frame reports its supplied code, or 1005 when the frame contains
no code. HTTP/2 END_STREAM or RST_STREAM without a WebSocket Close handshake is
an abnormal closure and reports 1006 with the applicable standard reason.

Each WebSocket scope receives exactly one disconnect event. A later generic
HTTP/2 close callback must not enqueue a second event after a Close frame was
already handled.

## 11. SSE alignment

### 11.1 Shared request-body semantics

SSE request receive behavior is built on the same de-chunked request-body
semantics as HTTP:

- HTTP/1 supports Content-Length and chunked bodies;
- `Expect: 100-continue` is handled transparently before waiting for body data;
- HTTP/2 waits for DATA and END_STREAM and emits one or more `sse.request`
  events with truthful `more` values;
- dispatch timing cannot cause the first receive to label an incomplete body
  as terminal;
- empty-body requests receive one empty terminal event;
- disconnect remains observable after the body is complete.

The implementation should share a private body-reader abstraction or helper
with normal HTTP rather than maintaining a third partial parser.

### 11.2 UTF-8 wire encoding

`sse.send` string fields and `sse.comment` text are formatted as characters,
then encoded exactly once to UTF-8 bytes before queueing or HTTP/1 chunk
framing. Generated keepalive comments follow the same rule. Chunk lengths,
buffered byte counts, and transport watermarks use encoded byte lengths.

Invalid Perl strings that cannot be encoded fail the send Future. Already-byte
HTTP decline bodies are not re-encoded.

### 11.3 Per-stream HTTP/2 keepalive and idle state

For HTTP/2, each SSE stream owns:

- its keepalive timer, interval, comment, and writer;
- its idle timer and activity reset;
- its close/disconnect reason;
- its send queue and watermark callbacks;
- its closing and terminal flags.

Starting or updating keepalive on one stream cannot stop, replace, or redirect
another stream's timer. Closing one stream removes only its timers and writer.
The connection-level HTTP/1 fields may remain because HTTP/1 carries only one
active long-lived scope on that connection.

### 11.4 SSE response headers

The server supplies Content-Type, Cache-Control, Date, and HTTP/1 Connection
headers only where required and only when the application has not supplied the
corresponding header, except for connection/framing headers the protocol
requires the server to control. HTTP/2 never emits HTTP/1-only Connection or
chunked-framing headers.

### 11.5 SSE detection

Both detection sites use raw substring matching today (the HTTP/1 header
scan and the HTTP/2 request dispatch), which classifies an explicit refusal
(`Accept: text/event-stream;q=0`) as an SSE request and false-positives on
any token containing the substring. PAGI now defines detection as a boolean
client-signal check:

- combine the values of all `Accept` headers and parse the result as a
  comma-separated list of media ranges (RFC 9110 section 12.5.1);
- assign the `sse` scope iff the exact range `text/event-stream` appears,
  case-insensitively, with an effective quality value greater than zero;
- `q=0` is an explicit refusal and never signals SSE;
- wildcard ranges (`*/*`, `text/*`) never signal SSE;
- media-type parameters other than `q` are ignored for the test.

This is a light parse shared by both transports — not full content
negotiation, which stays application/middleware territory. The
WebSocket-upgrade precedence check is unchanged.

### 11.6 Connection reuse after an SSE stream ends (HTTP/1.1)

Decision (John, 2026-08-22): the server honors the `Connection: keep-alive`
header it emits on `sse.start`. Today `_finish_sse_stream` writes the
chunked terminator and then unconditionally closes the TCP connection,
breaking the reuse promise — pooled clients (Net::Async::HTTP, browsers,
curl) return the socket to their pool and the next request hits a dead
socket (live-reproduced; Phase 1's t/52 works around it with a fresh
client per SSE call). The alternative — declaring `Connection: close` —
was rejected: it would require a PAGI spec change (Www.pod mandates
keep-alive on `sse.start`) and permanently penalize the short
POST-SSE-exchange pattern (fetch-event-source/datastar) with a TCP+TLS
handshake per exchange.

Required behavior after this phase:

- When an HTTP/1.1 SSE stream ends **cleanly** (application return or
  `sse.close`), the server writes the chunked terminator, resets
  per-request SSE state (stream flags, keepalive/idle timers, the send
  closure's sequence state via a fresh request cycle), and returns the
  connection to normal keep-alive request handling — the same contract an
  ordinary HTTP response gets, including the pipelined-data check.
- Keep-alive still yields to the usual overrides: a client `Connection:
  close`, HTTP/1.0 semantics, server shutdown, or an **abnormal** end
  (client disconnect, timeout, write error), which closes as today.
- The clean-end path must not fire `on_disconnect`/`sse.disconnect`
  merely because the stream ended; disconnect events remain reserved for
  abnormal ends per the PAGI spec.
- Phase 1's post-`sse.close` raise contract is unaffected: further sends
  on the ended stream's scope still fail via the sequence machine (which
  no longer depends on the transport being closed).
- t/52's fresh-client-per-SSE-call workaround is removed and replaced
  with a positive assertion: one pooled client performs an SSE exchange
  and a subsequent ordinary request on the same connection.

## 12. Lifespan and worker alignment

The lifespan send function uses the shared base validation rules. Unknown
types, wrong-phase results, and malformed message fields fail the returned
Future instead of succeeding silently.

Worker construction propagates at least:

- `lifespan_mode`;
- `lifespan_startup_timeout`;
- any retained supplemental validation-diagnostics setting.

The mandatory validator itself is unconditional and therefore needs no worker
toggle. Each worker still runs its own lifespan exchange and receives its own
worker metadata as required by the existing implementation.

**`lifespan_mode => 'off'` is removed.** PAGI's Lifespan spec now states
that skipping the protocol is nonconforming ("A server must not offer an
'off' switch for this protocol"), reversing this design's earlier decision
to keep it as a documented override. The constructor and `set_app_config`
reject `'off'` with the same die-on-invalid-value behavior as any other
unrecognized mode, and `pagi-server --lifespan off` fails at startup with a
message pointing at the spec rationale. The remaining modes are `auto`
(decline-tolerant, the required default) and `on` (strict: a decline is a
fatal startup failure), both of which the existing implementation already
handles. This is a breaking change recorded in `Changes` and the upgrading
note.

## 13. Server-supplied headers and advertised capabilities

### 13.1 Date

Normal and generated HTTP responses add `Date` only when the application did
not provide one. This applies consistently to HTTP/1, HTTP/2, SSE start,
WebSocket denial responses, SSE decline responses, and server-generated
responses where the PAGI specification requires Date.

Duplicate application Date headers are not rewritten, but the server does not
append another.

### 13.2 Extension truthfulness

Configured extension data remains available to scopes, but built-in transport
capabilities are filtered or implemented per scope. In particular, an HTTP/2
scope cannot advertise `fullflush` unless the HTTP/2 send path accepts it.
Custom extension mappings are not rejected merely because PAGI::Server does
not interpret their contents.

## 14. Internal structure

The implementation should improve the touched boundaries without attempting a
general rewrite of `PAGI::Server::Connection`.

Recommended private units are:

1. **Event validation:** one module containing protocol event shape checks and
   reusable header/payload validators.
2. **HTTP body consumption:** a small internal helper, or a disciplined pair of
   transport adapters, that gives body/file/fh identical resource semantics.
3. **HTTP/2 stream cleanup:** one idempotent helper that removes timers,
   resolves waiters, drops transport cycles, marks connection state, and then
   releases stream bookkeeping.
4. **Per-stream timer writers:** closures stored on stream state and capturing
   the stream identifier, never a connection-global current writer.

An `H2StreamState` public or private object is not required. The existing hash
may remain if ownership and cleanup are centralized and tested. New helpers
must be private implementation details.

## 15. Testing requirements

Tests are behavioral unless a narrow unit test is the only practical way to
exercise a parser or timer. Source-text regex tests do not establish protocol
conformance.

### 15.1 Validation matrix

For HTTP, WebSocket, SSE, and lifespan, test in both production and development
configuration:

- every recognized event's required fields and legal defaults;
- missing required fields;
- wrong reference and primitive types;
- newline/whitespace numeric near misses;
- invalid header containers, tuples, and byte values;
- mutually exclusive or exactly-one payload fields;
- unknown event types;
- legal extra fields;
- illegal state transitions;
- malformed sends after closure succeeding as no-ops.

Tests must cover HTTP/1 and HTTP/2 dispatch boundaries, not only direct calls
to `EventValidator`.

### 15.2 HTTP/2 HTTP parity

Add focused tests for:

- ordinary complete and streaming bodies;
- file, filehandle, offset, length, offset past EOF, open/read/seek failure,
  and application ownership of `fh`;
- per-stream backpressure while streaming a file;
- trailers declared, transmitted, omitted, and sent illegally;
- `http.fullflush` advertised and absent;
- HEAD with ordinary, streaming, file, filehandle, and trailers events;
- HEAD never opening the supplied file/fh;
- Content-Length preservation;
- send after RST_STREAM as a successful no-op.

### 15.3 Connection lifecycle and errors

Test:

- normal completion;
- client cancellation before and after response start;
- application failure before start;
- application failure after start;
- clean return with no response;
- clean return after start but before a terminal event;
- response start followed by stream cancellation while blocked on drain or
  file I/O;
- two concurrent streams where only one completes or fails;
- callback order and single delivery;
- `disconnect_future` created before disconnect, after disconnect, before
  completion, and after completion.

Tests must assert that a cancelled stream causes neither a synthetic 500 nor an
application-error log.

Incomplete-response tests run on both transports and assert the full
contract: HTTP/1 closes without the chunked terminator and never keeps the
connection alive or serves a pipelined request; HTTP/2 resets only the
affected stream with no END_STREAM; both fire `on_disconnect` with
`server_error`, never `on_complete`; both log at error level unless the
client already disconnected; and a promised-but-unsent trailers event
(`trailers => 1` declared, terminal body sent, no trailers event) also
counts as incomplete.

### 15.4 WebSocket over HTTP/2

Test:

- all send event shapes and state transitions;
- unknown events failing;
- Ping and Pong DATA framing;
- interval update and disable;
- keepalive timeout code/reason;
- peer Close with a code, peer Close without a code, END_STREAM without Close,
  and RST_STREAM;
- exactly one disconnect event;
- two WebSocket streams with independent timers and outcomes.

### 15.5 SSE

Test:

- HTTP/1 Content-Length, chunked, and Expect/continue request bodies;
- HTTP/2 DATA arriving before and after the application first calls receive;
- accurate `more` values across multiple chunks;
- exact UTF-8 bytes for non-ASCII event, data, id, comment, and keepalive text;
- byte-based HTTP/1 chunk sizes and H2 buffered amounts;
- invalid newline/retry and encoding failures;
- two concurrent H2 SSE streams with different keepalive text and intervals;
- closing one stream without affecting the other;
- start/decline first-send-wins behavior on both HTTP versions;
- detection on both transports: exact `text/event-stream` (with and without
  parameters, mixed case) classifies as `sse`; `text/event-stream;q=0`,
  `*/*`, `text/*`, and substring near-misses (e.g. a longer token
  containing the substring) classify as `http`; repeated `Accept` headers
  combine; a matching range alongside higher-q alternatives still
  classifies as `sse`.

### 15.6 Lifespan, workers, headers, and extensions

Test:

- unknown and wrong-phase lifespan events;
- lifespan configuration propagation into a real or controlled worker seam;
- `lifespan_mode => 'off'` rejected by the constructor, `set_app_config`,
  and `pagi-server --lifespan off`;
- Date preservation without server duplication across relevant response paths;
- extension advertisement matching each transport's accepted events.

### 15.7 Verification gates

Each implementation task runs its focused tests. The final branch runs:

- the complete test suite under the project Perl;
- the complete HTTP/2 group;
- release-sensitive tests where practical;
- Perl 5.16.3 syntax checks for ordinary distribution modules unless a module
  intentionally requires a newer Perl;
- POD validation and `git diff --check`;
- TLS tests after installing `IO::Async::SSL`, or records a clearly visible
  release blocker if that environment cannot be made available.

The complete suite runs once at final verification, not repeatedly after every
small commit.

## 16. Documentation and release notes

Update:

- `PAGI::Server` constructor and `validate_events` POD;
- HTTP/2, WebSocket, SSE, lifespan, connection-state, and compliance POD where
  behavior or previous limitations changed;
- examples only where they demonstrate affected server behavior;
- `Changes` with the user-visible corrections;
- an upgrading note explaining that malformed events now fail in production,
  even when `validate_events` is false.

Documentation must distinguish:

- PAGI MUST behavior now enforced;
- optional capabilities still omitted, such as `response_complete`;
- the explicit operator strict mode `lifespan_mode => 'on'` and the removal
  of `'off'` (with the upgrading note naming the replacement: applications
  that do not use lifespan simply decline);
- known performance characteristics of application-owned filehandles;
- the separate future generic conformance-suite project.

No PAGI specification file is changed by this work.

## 17. Compatibility and operational effects

The meaningful compatibility change is stricter failure of invalid
applications. Examples include missing HTTP status, misspelled event type,
invalid integer text, response body before response start, or an SSE stream
event after decline began. These used to be defaulted or ignored on some paths.

Two further deliberate behavior changes, both spec-driven:

- `lifespan_mode => 'off'` is removed (see section 12); deployments using it
  must drop the option — an application that does not use lifespan declines
  at effectively no cost.
- SSE detection tightens (see section 11.5): requests whose `Accept` only
  matched by substring or wildcard now receive an `http` scope. Real SSE
  clients (`EventSource`, `fetch-event-source`) send the exact media type
  and are unaffected.

Valid applications retain the same API. HTTP/2 clients gain missing behavior;
they should not observe a semantic regression. Per-stream resets replace
connection-wide or hanging failure behavior, improving multiplexed isolation.

Mandatory validation adds a small fixed per-event cost. It must not perform
filesystem access, network I/O, symbol lookup, stack inspection, or deep copies.
File and body streaming remain subject to existing backpressure.

## 18. Risks and mitigations

### 18.1 State-machine divergence

Separate HTTP/1 and HTTP/2 branches can drift again. Shared validation, shared
body-source semantics, and cross-transport behavior tables mitigate this.

### 18.2 HTTP/2 cleanup races

RST_STREAM can race file reads, drain waits, timers, application completion,
and deferred stream deletion. Cleanup must be idempotent, resolve or cancel
every waiter exactly once, and remove only the affected stream. Concurrency
tests are required.

### 18.3 Validation becoming a performance feature flag

Retaining `validate_events` could tempt future code to put mandatory checks
behind it again. POD and tests must state that core validation is unconditional.

### 18.4 Over-refactoring `Connection.pm`

This file carries many protocols and performance-sensitive paths. Extract only
the helpers that establish shared correctness or per-stream ownership. No
style-only rewrite is part of this project.

### 18.5 Specification drift during implementation

The PAGI repository is under concurrent review. Pinning the audit snapshot and
performing start/end drift checks prevents silently coding against a stale
contract. Any material difference is surfaced to the user before implementation
continues.

## 19. Rejected alternatives

### 19.1 Five independent projects or pull requests

Rejected because validation, HTTP/2 stream state, error cleanup, and protocol
tests are shared. Separate branches would create non-conforming intermediate
states and repeated review. One branch with reviewable commits is clearer.

### 19.2 Keep validation development-only

Rejected because PAGI makes failure of malformed and unknown outgoing events a
runtime server requirement, not a diagnostic option.

### 19.3 Validate in each transport independently

Rejected because it caused the current HTTP/1/HTTP/2 drift. Transports frame
events differently but do not define different PAGI event shapes.

### 19.4 Close the entire HTTP/2 connection on one stream failure

Rejected because it breaks multiplexing isolation and penalizes unrelated
applications. Reset or terminate only the failed stream unless the HTTP/2
connection itself is corrupt.

### 19.5 Keep connection-global WebSocket/SSE timers on HTTP/2

Rejected because “last wins” applies within one scope, not among unrelated
multiplexed scopes. Connection-global writers can send keepalive bytes to the
wrong stream.

### 19.6 Require applications to pre-encode SSE strings

Rejected because PAGI explicitly assigns UTF-8 encoding of SSE String fields to
the server.

### 19.7 Add `response_complete()` now

Rejected as unrelated optional surface. The project fixes the lifecycle
methods PAGI::Server already exposes; it does not add every SHOULD-level
capability while touching the class.

### 19.8 Build the reusable conformance suite here

Rejected because that is a distinct, larger deliverable intended for other
server implementations. This project adds server regression tests, not a public
test harness.

## 20. Implementation order

One implementation plan should execute these phases on the existing branch:

1. Mandatory shared event validation and protocol state checks.
2. HTTP/2 HTTP parity: HEAD, file/fh, trailers, fullflush, and body completion.
3. Connection lifecycle, connection state, and error handling: HTTP/2
   per-stream state plus the HTTP/1 incomplete-response fix (both
   transports share the section 9.3 contract).
4. HTTP/2 WebSocket validation, keepalive, and disconnect semantics.
5. SSE request bodies, UTF-8, detection (media-range parse on both
   transports), HTTP/1.1 keep-alive after clean stream end (section 11.6),
   and HTTP/2 per-stream timers/state.
6. Lifespan worker propagation and `off` removal, Date, extension
   truthfulness, and remaining connection-state consistency.
7. Documentation, drift audit, optional TLS verification, and final suite.

Each phase should produce one or more narrow commits with its tests. The final
result remains one branch and one pull request.

## 21. Definition of done

The project is complete when:

1. Every supported outgoing protocol event is validated unconditionally before
   delivery and malformed post-close events remain no-ops.
2. HTTP/1 and HTTP/2 pass the same HTTP response behavior matrix.
3. HTTP/2 correctly handles HEAD, body/file/fh, trailers, backpressure,
   application failure, cancellation, and completion.
4. Every HTTP/2 HTTP stream transitions its own `pagi.connection` state exactly
   once.
5. HTTP/2 WebSocket keepalive and disconnect behavior is per-stream and
   spec-correct.
6. SSE request bodies and UTF-8 output are correct on HTTP/1 and HTTP/2,
   detection follows the media-range rule on both transports, and
   concurrent H2 streams cannot affect one another.
7. Lifespan events validate, worker configuration is preserved, and
   `lifespan_mode => 'off'` is rejected everywhere it could be supplied.
8. An incomplete response is observably truncated on both transports,
   fires `on_disconnect` with `server_error`, and never fires
   `on_complete` or reuses the HTTP/1 connection.
9. Built-in advertised capabilities are truthful and Date is not duplicated.
10. The final local PAGI drift check finds no unaddressed material change.
11. Focused tests, the complete suite, syntax/POD checks, and diff checks pass;
    TLS verification either passes or remains an explicit release blocker.
12. Compliance and upgrading documentation describe the resulting behavior.
13. The branch contains only owned PAGI::Server changes and is ready for one
    user review and one pull request.
