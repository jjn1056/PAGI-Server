# Protocol Alignment Phase 5: SSE Alignment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring SSE into conformance across both transports: media-range detection (spec `3cac188` — both sites still substring-match), UTF-8 wire encoding (character strings currently reach the framing un-encoded), truthful request-body delivery, per-stream h2 keepalive/idle state (currently connection-level fields that a second multiplexed SSE stream hijacks), and the John-ratified §11.6 keep-alive honor after a clean h1 stream end (removing t/52's fresh-client workaround).

**Architecture:** Detection gets one shared private predicate (`_accept_signals_sse`) used by both dispatch sites. Encoding happens exactly once — `_format_sse_event`/`_format_sse_comment` keep producing character strings; the send paths encode the formatted string to UTF-8 bytes (croak → failed Future on invalid) before chunk framing (h1) or queueing (h2), and all byte accounting uses encoded lengths. h2 SSE keepalive/idle state moves from `$self->{sse_*}` to `$ss` (the Phase-4 WS keepalive is the pattern); h1 keeps its connection-level fields (one long-lived scope per connection — design §11.3 allows it). §11.6 rewires `_finish_sse_stream`'s CLEAN path to reset per-request state and return to the keep-alive request loop, honoring the `Connection: keep-alive` header the server itself sends on `sse.start`.

**Tech Stack:** Perl 5 (perlbrew `perl-5.42.2@default`), Test2::V0, IO::Async, Encode, Net::HTTP2::nghttp2 0.008.

**Spec:** design §11 (11.1–11.6), §15.5; PAGI spec `main` @ `f04c029` — Www.pod "SSE Connection Detection" (exact media-range, q>0, wildcards never), "Send SSE" (String fields "MUST be UTF-8 encoded by the server before transmission"), "Request Body – receive event" (truthful `more`), sse.start server-supplied headers.

## Global Constraints

- Worktree `/Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/pagi-0.4-alignment`, branch `fix/pagi-0.4-alignment`, starting HEAD `be05e8e`. Never the perf checkout.
- perlbrew wrapper always; Test2::V0; TDD (RED/GREEN tee'd + READ in full); real servers/raw sockets; foreground only; **commit incrementally**; full suite only at the gate under `caffeinate -i`.
- Phase 1–4 invariants survive: validate→advance ordering and the SSE carve-outs (`$seq eq 'closed' || 'decline_complete'`); the exactly-one-disconnect and per-stream-timer disciplines from Phase 4 are the pattern for §11.3.
- Encode exactly once: decline bodies (`sse.http.response.body`) are already bytes — never re-encoded. Wire assertions compare exact octets (raw clients), not decoded strings.
- Timing tests state computed margins in comments.
- Commit only each task's files; never the plan file; tracking table updated by the controller.

## File Structure

- `lib/PAGI/Server/Connection.pm` — detection sites (~443 h2, ~2796-2805 h1 helper), `_format_sse_event`/`_format_sse_comment` call sites (h1 SSE send closure ~4080+, h2 SSE send closure ~1690+), h2 SSE receive (~1660-1700), h1 SSE receive (~4080+), connection-level `sse_keepalive_*`/`sse_idle_timer` fields (~131-157, ~1795-1860, ~2324-2360), `_finish_sse_stream` (~3971), the h1 request-loop keep-alive reset region.
- Tests: new `t/http2/32-sse-detection-encoding.t` and `t/55-sse-alignment.t`; extend `t/http2/13-sse-detection.t`, `t/http2/15-sse-keepalive.t`, `t/05-sse.t`, `t/52-mandatory-validation.t` (§11.6 workaround removal); fixtures fixed-not-weakened.
- `Changes`, `lib/PAGI/Server/Compliance.pod` — Task 6.

---

### Task 1: Media-range SSE detection (§11.5, spec 3cac188)

**Files:** Modify `lib/PAGI/Server/Connection.pm`; extend `t/http2/13-sse-detection.t`; create `t/55-sse-alignment.t` (detection half).

**Interfaces:**
- Produces: one private predicate used by BOTH sites:

```perl
# SSE client-signal check (PAGI Www.pod "SSE Connection Detection"): the
# exact media range text/event-stream, case-insensitively, with q > 0.
# A boolean signal test, not content negotiation: wildcards never signal
# SSE, and q=0 is an explicit refusal.
sub _accept_signals_sse {
    my ($headers) = @_;
    my @values;
    for my $h (@$headers) {
        push @values, $h->[1] if $h->[0] eq 'accept';
    }
    return 0 unless @values;
    for my $range (split /,/, join(',', @values)) {
        my ($type, @params) = split /;/, $range;
        $type =~ s/\A\s+|\s+\z//g;
        next unless lc($type) eq 'text/event-stream';
        my $q = 1;
        for my $p (@params) {
            $p =~ s/\A\s+|\s+\z//g;
            $q = $1 if $p =~ /\Aq\s*=\s*([0-9.]+)\z/i;
        }
        return 1 if $q > 0;
    }
    return 0;
}
```

  Both call sites (h2 header scan ~443; h1 helper ~2796-2805) delegate to it, passing the request's header arrayref. WebSocket-upgrade precedence unchanged.

- [ ] **Step 1 (RED):** t/55 (h1) + t/http2/13 extensions (h2), same matrix both transports — each request's scope type observed via the app (`$Detect::TYPE = $scope->{type}`):
  - `text/event-stream` → sse; `TEXT/EVENT-STREAM` → sse; `text/event-stream;q=0.4` → sse; `text/event-stream, text/html;q=0.9` → sse (presence, not preference);
  - `text/event-stream;q=0` → http (RED: today sse); `*/*` → http; `text/*` → http; `application/json, text/event-streamer` → http (RED: substring false-positive); two `Accept` headers where only the second carries the range → sse.
  Tee `/tmp/phase5-task1-fail.out`, read.
- [ ] **Step 2 (implement)**; POD for nothing (private sub, comment only).
- [ ] **Step 3 (GREEN + neighbors):** `prove -l t/55-sse-alignment.t t/http2/13-sse-detection.t t/05-sse.t t/sse-decline.t t/52-mandatory-validation.t` — tee `/tmp/phase5-task1-pass.out`, read fully.
- [ ] **Step 4 (commit):** `fix(sse): detection is an exact media-range match with q > 0`

---

### Task 2: UTF-8 wire encoding (§11.2)

**Files:** Modify `lib/PAGI/Server/Connection.pm`; create `t/http2/32-sse-detection-encoding.t`; extend `t/55-sse-alignment.t`.

**Interfaces:**
- Produces: at every point a formatted SSE payload leaves `_format_sse_event`/`_format_sse_comment`/the keepalive comment builder for the wire (h1 chunk framing; h2 `$ss->{send_queue}` push, including the keepalive writer), the string is encoded exactly once:

```perl
my $bytes = eval { Encode::encode('UTF-8', $formatted, Encode::FB_CROAK) };
die "sse payload is not encodable as UTF-8: $@" unless defined $bytes;
```

  (match surrounding die style; the failure surfaces as a failed send Future). All chunk-length math, `send_queue_bytes`, and watermark accounting on the BYTE string. Decline bodies untouched (already bytes).
- Audit first, then fix: grep every write/queue site fed by the two formatters (h1 sse.send/sse.comment/sse.keepalive tick, h2 twins + `sse_keepalive_writer`) — inventory in your report; a site already encoding (unlikely) is evidence, not a conflict.

- [ ] **Step 1 (RED):** raw-socket (h1) and h2-frame (h2) byte assertions: app sends `sse.send` with `data => "caf\x{e9}"`, `event => "\x{e9}vent"`... event names with newline-free non-ASCII; comment `"r\x{e9}sum\x{e9}"`; keepalive comment with non-ASCII. Assert the exact UTF-8 octet sequences on the wire (`"caf\xc3\xa9"` etc.) and that the h1 chunk-size prefix equals the BYTE length (today: RED — wide characters leak or lengths count characters). Also: invalid string (`"bad \x{D800}"` surrogate) → send Future fails, stream still usable for a subsequent valid send.
  Tee `/tmp/phase5-task2-fail.out`, read.
- [ ] **Step 2 (implement)** per the audit.
- [ ] **Step 3 (GREEN + neighbors):** `prove -l t/55-sse-alignment.t t/http2/32-sse-detection-encoding.t t/05-sse.t t/http2/14-sse-events.t t/http2/15-sse-keepalive.t t/http2/20-sse-transport.t` — tee `/tmp/phase5-task2-pass.out`, read fully.
- [ ] **Step 4 (commit):** `fix(sse): encode payloads to UTF-8 exactly once at the wire boundary`

---

### Task 3: Truthful SSE request bodies (§11.1)

**Files:** Modify `lib/PAGI/Server/Connection.pm`; extend `t/55-sse-alignment.t`, `t/http2/32-sse-detection-encoding.t`.

**Interfaces:**
- h2: the SSE receive's first call currently returns `{ type => 'sse.request', body => $ss->{body}, more => 0 }` gated only on `sse_request_sent` — VERIFY whether it awaits `body_complete` first (read the closure above ~1670). If a POST body still in flight can be returned with `more => 0`, that is the design's predicted dispatch-timing bug: fix by awaiting `body_pending` until `body_complete` (single terminal event with the full body — matching the current one-shot shape) OR emitting chunked `sse.request` events with truthful `more` (choose the smaller change; state which).
- h1: verify Content-Length, chunked, and `Expect: 100-continue` bodies reach `sse.request` correctly (CL path exists ~4089-4099; chunked + 100-continue need tracing). Missing support = fix; present support = pin with tests.
- GET semantics unchanged: one empty terminal `sse.request`.

- [ ] **Step 1 (RED where bugs confirmed, pin where correct):** h2 POST-SSE with the client sending DATA in two pieces, END_STREAM delayed until AFTER the app's first `receive()` is already pending → the delivered body must be complete with truthful `more`; h1 POST-SSE with CL body, with chunked body, and with `Expect: 100-continue` (client waits for `100 Continue` before the body); GET → one empty terminal event. Tee `/tmp/phase5-task3-fail.out`, read.
- [ ] **Step 2 (implement)** as the audit dictates.
- [ ] **Step 3 (GREEN + neighbors):** `prove -l t/55-sse-alignment.t t/http2/32-sse-detection-encoding.t t/03-request-body.t t/05-sse.t t/http2/14-sse-events.t` — tee `/tmp/phase5-task3-pass.out`, read fully.
- [ ] **Step 4 (commit):** `fix(sse): request bodies delivered completely with truthful more flags`

---

### Task 4: Per-stream h2 SSE keepalive and idle state (§11.3)

**Files:** Modify `lib/PAGI/Server/Connection.pm`; extend `t/http2/15-sse-keepalive.t` or `t/http2/32-...`.

**Interfaces:**
- Today `sse_keepalive_writer`, `sse_keepalive_timer`, `sse_idle_timer` are `$self->{...}` fields and the h2 `sse.start` branch overwrites the connection-level writer — a second multiplexed h2 SSE stream hijacks the first's keepalive (design §19.5's rejected pattern, live in the code). Move to `$ss` for h2: `$ss->{sse_ka_writer}`, `$ss->{sse_ka_timer}`, `$ss->{sse_idle_timer}` (+ interval/comment fields), following Phase 4's WS-keepalive discipline exactly (weak refs, add_child/remove_child, stop sites: sse.close, decline finalize, `_h2_on_close`, teardown sweep, keepalive-update/interval-0). h1 keeps connection-level fields (single long-lived scope) — untouched.
- Stop-site inventory in the report, Phase-4 style.

- [ ] **Step 1 (RED):** two concurrent h2 SSE streams, keepalive on A (`comment => 'pingA'`, 0.2s) and B (`comment => 'pingB'`, 0.5s): each stream receives ONLY its own comments at its own cadence (today: RED — last writer wins, one stream gets both/wrong); closing A stops A's timer (white-box: `$ss` timer not running) while B keeps ticking; idle-timeout independence if cheaply assertable (else note). Margins commented. Tee `/tmp/phase5-task4-fail.out`, read.
- [ ] **Step 2 (implement)**.
- [ ] **Step 3 (GREEN + neighbors):** `prove -l t/http2/15-sse-keepalive.t t/http2/16-sse-cleanup.t t/http2/20-sse-transport.t t/http2/18-transport-leak.t t/33-ws-sse-idle-timeout.t` — tee `/tmp/phase5-task4-pass.out`, read fully.
- [ ] **Step 4 (commit):** `fix(h2-sse): keepalive and idle state are per-stream`

---

### Task 5: Keep-alive after a clean h1 SSE stream end (§11.6, John-ratified)

**Files:** Modify `lib/PAGI/Server/Connection.pm`; extend `t/55-sse-alignment.t`; update `t/52-mandatory-validation.t`.

**Interfaces (design §11.6 verbatim requirements):**
- `_finish_sse_stream`'s CLEAN path (app return or `sse.close`): write the chunked terminator, reset per-request state (SSE flags/timers, mode flags, receive bookkeeping, response bookkeeping — mirror the http keep-alive reset block), and return the connection to normal keep-alive request handling including the pipelined-buffer check. Honors the `Connection: keep-alive` header the server itself sends on `sse.start`.
- Overrides unchanged: client `Connection: close`, HTTP/1.0, server shutdown, and ABNORMAL ends (client disconnect, timeout, write error) close exactly as today.
- Clean end fires NO `on_disconnect`/`sse.disconnect`; the post-`sse.close` raise contract is untouched (the `$seq` machine owns it and no longer depends on the transport being closed — verify the Phase-1 carve-out still holds when the connection now STAYS OPEN: `$already_closed` keyed on `$seq`, so yes; pin it).
- t/52: remove the fresh-client-per-SSE-call workaround; replace with the positive assertion — one pooled client performs an SSE exchange and a subsequent ordinary request on the SAME connection (raw socket proves same-socket reuse).

- [ ] **Step 1 (RED):** raw-socket h1: SSE request → stream events → clean end → terminator observed → SECOND request on the same socket gets a normal response (today: RED — EOF). Variants: end-by-return and end-by-sse.close; `Connection: close` client → closes (control); abnormal mid-stream disconnect → closes (control); post-close send still raises (`after sse.close`) with the connection alive. Tee `/tmp/phase5-task5-fail.out`, read.
- [ ] **Step 2 (implement)** — this is the phase's riskiest change; keep the diff surgical and comment the state-reset inventory.
- [ ] **Step 3 (GREEN + neighbors):** `prove -l t/55-sse-alignment.t t/52-mandatory-validation.t t/05-sse.t t/sse-close.t t/sse-decline.t t/33-ws-sse-idle-timeout.t t/22-content-length-keepalive.t t/23-connection-cleanup.t` — tee `/tmp/phase5-task5-pass.out`, read fully.
- [ ] **Step 4 (commit):** `feat(sse): honor keep-alive after a clean stream end (spec Www.pod / design 11.6)`

---

### Task 6: Docs (+§11.4 pin)

**Files:** `Changes`, `lib/PAGI/Server/Compliance.pod`; ONE assertion each into existing tests if §11.4 verification finds gaps.

- [ ] §11.4 audit: h2 SSE responses must not carry `Connection`/chunked-framing headers; server-supplied Content-Type/Cache-Control/Date only when the app didn't supply them (both transports). Verify by reading the two sse.start branches; pin anything unpinned with one assertion per fact in the existing SSE test files; fix if wrong (report).
- [ ] Changes + Compliance.pod SSE section: detection rule, UTF-8 encoding, truthful bodies, per-stream h2 timers, keep-alive-after-clean-end. Fact-checked; no temporal POD language; podchecker clean; smoke `prove -l t/00-load.t`.
- [ ] Commit: `docs: record SSE conformance`

---

### Task 7: Phase gate

- [ ] Full suite under `caffeinate -i` → `/tmp/phase5-full-suite.out`; hygiene (`git diff be05e8e --check`, podchecker, `perl -c`); spec drift check (PAGI main still `f04c029`); tracking table + commit.

---

## Tracking

| Task | Status | Commit | Tests (added/passing) | Verification evidence |
|------|--------|--------|-----------------------|-----------------------|
| 1. Media-range detection | complete | 89e5d7b | RED 3+3 (incl. third live bug: missing /i), GREEN 48/48 across 5 files | Review clean; reviewer re-derived RED against pre-diff code |
| 2. UTF-8 encoding | complete | bdc01eb | 29 tests pristine | Review clean; 6-site encode inventory re-derived, no double-encode |
| 3. Request bodies | complete | 0c02724, 199f128, 2398aa5 | 43 tests / 6 files | All three §11.1 bugs live+fixed; Critical queue-recheck fix + `_read_chunked_body` extraction in round 1; re-review clean |
| 4. h2 per-stream timers | complete | fd68dbd, 55dae82 | 9 tests / 5 files; 6× flake runs + RELEASE_TESTING t/33 | Review clean after round 1 (stale POD rewrite); 5 stop sites re-derived |
| 5. Keep-alive clean end | complete | 96388eb, 6dce357, f8ef6c2, 5ece8b4 | t/55 keep-alive matrix; t/05+t/sse-close inversions (§11.6 ratified); t/52 reuse assertion | Opus review; Critical no-response guard fixed round 1; re-review clean; §11.6 authorization linkage double-verified |
| 6. Docs + §11.4 | complete | 0c00349, 5d23bf0 | Pins in t/05-sse.t + t/http2/14 (21/21) | Approved first pass; reviewer independently reproduced RED/GREEN with pre-fix Connection.pm; two real header-duplication bugs fixed supply-when-absent |
| 7. Phase gate | complete | 5a7c06d | full recursive suite 121 files / 750 tests PASS | hygiene clean (`git diff be05e8e --check`, podchecker ×5, `perl -c`); PAGI main still f04c029 (no drift); 3 initial failures triaged: t/http2/29 environmental (session RLIMIT_NOFILE=1048576 → ~4.2s/worker-fork FD sweep in IO::Async::OS; passes 0.030s at nofile=10240, fails identically at base be05e8e — not a regression), t/integration/sse-close-end-to-end.t stale pre-§11.6 EOF pin inverted under the Task 5 ruling (5a7c06d), t/sse-close.t host-sleep flake (passes on re-run) |

## Deviations

None recorded. A deviation gets an ID, a rationale, and John's sign-off here BEFORE work builds on it.
