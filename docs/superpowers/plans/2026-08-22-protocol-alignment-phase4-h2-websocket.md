# Protocol Alignment Phase 4: HTTP/2 WebSocket Keepalive and Disconnect Semantics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Close design §10's two remaining gaps: `websocket.keepalive` on HTTP/2 is validated then silently ignored (the last silent-ignore of a validated event in the server), and h2 WebSocket disconnect delivery is wrong twice over — a scope can receive up to three `websocket.disconnect` events for one close (peer-close path, bare-END_STREAM path, `_h2_on_close`), and the bare-END_STREAM path reports 1005 where the spec requires 1006 with a standard reason token.

**Architecture:** §10.1 (mandatory validation + state machine on the h2 WS send path) landed in Phase 1 — this plan only verifies it stayed intact. Disconnect delivery gets a per-stream `ws_disconnect_delivered` flag guarding every enqueue site, with the code/reason rules from Www.pod's "Disconnect - receive event": peer Close frame → its code (1005 if the frame carried none) + the peer's reason text; close without handshake (bare END_STREAM, RST, timeout) → 1006 + the standard token. Keepalive becomes per-stream state on `$ss` (never the h1 connection-level timer, which writes raw frames to the shared TCP stream — design §10.2 forbids reusing it): an `IO::Async::Timer::Periodic` per accepted WS stream sending RFC 6455 ping frames as h2 DATA via `submit_data`, pong observation clearing the wait flag, and a timeout that ends only that stream with 1006/`keepalive_timeout`.

**Tech Stack:** Perl 5 (perlbrew `perl-5.42.2@default`), Test2::V0, IO::Async (Timer::Periodic/Countdown), Protocol::WebSocket::Frame, Net::HTTP2::nghttp2 0.008 (`submit_data`).

**Spec:** design §10.2, §10.3, §15.4; PAGI spec `main` @ `56bf730` — Www.pod "Keepalive - send event" (interval/timeout semantics, last-wins, `keepalive_timeout` reason, code 1006), "Disconnect - receive event" (code/reason pairing rules), "WebSocket over HTTP/2".

## Global Constraints

- Worktree `/Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/pagi-0.4-alignment`, branch `fix/pagi-0.4-alignment`, starting HEAD `931aaf6`. Never the perf checkout.
- perlbrew wrapper for every Perl command; Test2::V0; TDD (RED/GREEN tee'd + READ); behavioral tests on real h2 socketpairs (harness style from t/http2/07-websocket.t); foreground only; **commit incrementally** (the host sleeps — small green commits, immediately); full suite only at the gate under `caffeinate -i`.
- Exactly-one disconnect is a QUEUE property: at most one `websocket.disconnect` is ever enqueued per stream. (The receive-side synthesized 1006 fallback for an already-reclaimed stream at Connection.pm ~1358-1401 is a separate liveness mechanism — leave it.)
- Per-stream timers must be cleaned up on EVERY exit path: close-frame teardown, `_h2_on_close`, the connection teardown sweep, keepalive-timeout itself. No leaked `IO::Async` children (t/http2/18-transport-leak.t is the canary; `remove_child` like the h1 twin at `_stop_ws_keepalive`).
- Phase 1-3 invariants survive (validate→advance ordering and carve-outs on the ws send path; per-stream ConnectionState untouched — ws streams have none, by spec).
- Commit only each task's files; never the plan file; tracking table updated by the controller.

## File Structure

- `lib/PAGI/Server/Connection.pm` — `_h2_process_ws_frames` (1917+, close-frame path + opcode 9/10 branches), `_h2_on_body` ws-eof path (~509-517), `_h2_on_close` ws branch (~600-612), `_h2_ws_close`, the h2 WS send closure's keepalive dispatch (currently absent — validated event falls through silently), stream-state init (`_h2_on_request`).
- Tests: new `t/http2/31-ws-keepalive-disconnect.t`; possible small extensions to `t/http2/07-websocket.t` / `t/http2/08-websocket-edge.t` fixtures if their assertions relied on duplicate/mis-coded events (extend/fix fixtures, never weaken).
- `Changes`, `lib/PAGI/Server/Compliance.pod` — Task 3.

---

### Task 1: Exactly-one disconnect with correct codes and reasons (§10.3)

**Files:** Modify `lib/PAGI/Server/Connection.pm`; create `t/http2/31-ws-keepalive-disconnect.t` (disconnect half).

**Interfaces:**
- Produces: `$stream->{ws_disconnect_delivered}` flag + a single private enqueue helper inside Connection.pm:

```perl
# Enqueue the scope's single websocket.disconnect event (PAGI: exactly one
# per WebSocket scope). Every h2 delivery site MUST come through here.
sub _h2_ws_enqueue_disconnect {
    my ($self, $stream, $code, $reason) = @_;
    return if $stream->{ws_disconnect_delivered}++;
    push @{$stream->{receive_queue}}, {
        type   => 'websocket.disconnect',
        code   => $code,
        reason => $reason,
    };
    $self->_h2_wake_pending($stream);
}
```

- All existing enqueue sites route through it with spec-correct arguments:
  - `_h2_process_ws_frames` close-frame path: the peer's `$code` (1005 when the frame carried no code) and the peer's `$reason` text — unchanged values, now deduped. The protocol-error sub-paths (1002/1007) keep their codes with reason `'protocol_error'` (Www.pod: server-initiated protocol close pairs the RFC code with the `protocol_error` token).
  - `_h2_on_body` eof path (bare END_STREAM, no close handshake): **1006 + `'client_closed'`** — replacing today's wrong `1005, ''`.
  - `_h2_on_close` ws branch: 1006 + `'client_closed'` (deduped — after a close handshake or eof already delivered, this is a no-op).
  - The connection-teardown sweep's h2 ws streams (if it enqueues) and `_h2_ws_close` server-initiated closes: same helper.

- [ ] **Step 1 (RED):** create the test file (h2 ws handshake boilerplate from t/http2/07): app accepts, then drains receive events into `@WsEvents::SEEN` (loop until a disconnect arrives, then attempt ONE more bounded receive to catch a second queued event — a synthesized fallback 1006 from a reclaimed stream is tolerated ONLY if the queue itself never held two). Scenarios:
  - peer sends Close(4321, "bye") → exactly one disconnect with code 4321, reason "bye";
  - peer sends Close with empty payload → code 1005;
  - peer sends bare END_STREAM (no close frame) → exactly one, code 1006, reason 'client_closed' (today: a 1005,'' AND a 1006,'' both land — RED);
  - peer RST_STREAM → exactly one, 1006, 'client_closed'.
  Tee `/tmp/phase4-task1-fail.out`, read.
- [ ] **Step 2 (implement)** per Interfaces; flag initialized (absent = false) at stream init.
- [ ] **Step 3 (GREEN + neighbors):** `prove -l t/http2/31-ws-keepalive-disconnect.t t/http2/07-websocket.t t/http2/08-websocket-edge.t t/http2/22-denial-response.t` — tee `/tmp/phase4-task1-pass.out`, read fully. If 07/08 fixtures asserted the old duplicate/1005 behavior, fix the fixture with explanation.
- [ ] **Step 4 (commit):** `fix(h2-ws): exactly one disconnect event, spec-correct codes and reasons`

---

### Task 2: Per-stream keepalive on h2 (§10.2 — the last silent-ignore)

**Files:** Modify `lib/PAGI/Server/Connection.pm`; extend `t/http2/31-ws-keepalive-disconnect.t`.

**Interfaces:**
- Consumes: Task 1's `_h2_ws_enqueue_disconnect`; the h1 twin's frame construction (`Protocol::WebSocket::Frame->new(type => 'ping', buffer => '')`); `submit_data($stream_id, $bytes, 0)` + `_h2_write_pending` for delivery; the opcode dispatch in `_h2_process_ws_frames` (check the existing opcode 9/10 branches — client pings must already be answered; pong (10) must clear this stream's wait flag; add/extend as found, note what existed).
- Produces: a `websocket.keepalive` dispatch branch in the h2 WS send closure (after validate/advance — the machine already permits it in `accepted`); per-stream state on `$ss`: `ws_ka_timer`, `ws_ka_interval`, `ws_ka_timeout`, `ws_ka_waiting_pong`, `ws_ka_pong_timer`. Semantics per Www.pod: last event wins (restart timer with new settings); `interval => 0` stops; omitted `timeout` = no dead-connection detection; pong timeout → stop timers, `_h2_ws_enqueue_disconnect($stream, 1006, 'keepalive_timeout')`, then end ONLY that stream (mirror `_h2_ws_close`'s teardown: submit END_STREAM or RST + write-pending; verify which the existing close path uses and match it). A private `_h2_stop_ws_keepalive($stream)` helper is called from: the keepalive branch (interval 0 / restart), close-frame teardown, `_h2_on_close`, the teardown sweep, and pong-timeout itself. Timers are `add_child`'d to the server and `remove_child`'d on stop (h1 twin's discipline).

- [ ] **Step 1 (RED):** extend the test file:
  - app sends `keepalive interval => 0.2, timeout => 0.3`; client reads DATA frames: a ws ping frame arrives within ~0.6s (today: nothing — RED); client answers pong → connection survives ≥ 2 intervals (no disconnect event);
  - client STOPS answering → within interval+timeout the app's queue gets exactly one disconnect 1006/'keepalive_timeout', and a SIBLING h2 stream on the same connection (plain GET) still serves;
  - two concurrent WS streams, keepalive on stream A only → stream B never receives pings (read B's DATA frames);
  - `interval => 0` after starting → no further pings observed in 2 intervals.
  Bounded loop_once waits; generous-but-bounded timing (0.2s intervals, 5s ceilings — the suite must not flake under load; state margins in comments per the timing-measurement rule). Tee `/tmp/phase4-task2-fail.out`, read.
- [ ] **Step 2 (implement)** per Interfaces.
- [ ] **Step 3 (GREEN + neighbors):** `prove -l t/http2/31-ws-keepalive-disconnect.t t/http2/07-websocket.t t/http2/08-websocket-edge.t t/http2/18-transport-leak.t t/33-ws-sse-idle-timeout.t` — tee `/tmp/phase4-task2-pass.out`, read fully.
- [ ] **Step 4 — h1 parity check (add-only-if-missing):** grep t/04-websocket.t and t/33 for h1 keepalive-timeout coverage asserting the app-facing event carries code 1006 AND reason 'keepalive_timeout' (the spec's pairing). If uncovered, add one h1 subtest to t/04 (or t/33 where the fixture fits); if the h1 event's reason is missing/wrong, fix the h1 delivery (the `ws_disconnect_reason` plumbing at Connection.pm ~149 exists) — report what you found either way.
- [ ] **Step 5 (commit):** `feat(h2-ws): per-stream keepalive with ping/pong and timeout` (+ separate commit if h1 needed fixing)

---

### Task 2b: h2 warns on server-caused abnormal ends (log-parity, ratified fix-now)

**Files:** Modify `lib/PAGI/Server/Connection.pm` (`_h2_dispatch_stream` wrapper); extend `t/http2/24-incomplete-response.t`.

**Interfaces:**
- Consumes: the wrapper's `$client_gone` carve-out (C1 shape: `$cs ? defined($cs->disconnect_reason) : ...`).
- Produces: the carve-out distinguishes WHO caused the abnormal end — a recorded reason of `server_error` means the client did not go anywhere, so the incomplete/threw warns still fire; client-side reasons (`client_closed`, timeouts) stay silent. Per the PAGI spec's incomplete-response section, the log carve-out is only for "the client had already disconnected."
- RED: h2 app declares `trailers => 1`, sends terminal body (server END_STREAMs early — the Phase-2b gap), returns; today: reason `server_error` (correct) but NO warning. Assert exactly one "PAGI application returned with an incomplete response (HTTP/2 stream ...)" warning; client wire behavior unchanged (200 + full body); no double-warn on the plain `/incomplete` path (already-warned case must not regress to two).

- [ ] Step 1 RED (tee `/tmp/phase4-task2b-fail.out`, read); Step 2 implement; Step 3 GREEN + neighbors: `prove -l t/http2/24-incomplete-response.t t/http2/30-connection-state.t t/http2/12-error-handling.t` (tee `/tmp/phase4-task2b-pass.out`, read). Step 4 commit: `fix(h2): warn on server-caused abnormal ends per the spec's log rule`

---

### Task 3: Documentation

**Files:** `Changes`, `lib/PAGI/Server/Compliance.pod`.

- [ ] Changes (existing 0.002007 sections): h2 WS keepalive implemented per-stream; exactly-one disconnect with spec-correct code/reason pairs (bare END_STREAM now 1006/client_closed, was a duplicate 1005). Compliance.pod: WebSocket-over-HTTP/2 section updated (keepalive supported; disconnect code/reason table per Www.pod pairing rules). Fact-check every claim; no temporal language in POD; podchecker clean. Verify + smoke `prove -l t/00-load.t`.
- [ ] Commit: `docs: record h2 websocket keepalive and disconnect conformance`

---

### Task 4: Phase gate

- [ ] Full suite under `caffeinate -i` → `/tmp/phase4-full-suite.out`; grep `^Result:`/`Failed`; hygiene (`git diff 931aaf6 --check`, podchecker, `perl -c`); spec drift check (PAGI main still `56bf730`); tracking table + commit (`docs: phase 4 verification gate results`).

---

## Tracking

| Task | Status | Commit | Tests (added/passing) | Verification evidence |
|------|--------|--------|-----------------------|-----------------------|
| 1. Exactly-one disconnect | not started | — | — | — |
| 2. Per-stream keepalive | not started | — | — | — |
| 2b. Server-caused-end log parity | not started | — | — | — |
| 3. Docs | not started | — | — | — |
| 4. Phase gate | not started | — | — | — |

## Notes

- §10.1 (h2 WS validation/state) was delivered by Phase 1 (commits 9b38f45/33bdb73) — the gate re-verifies via t/http2/26; no task here.
- OPEN John-decision packet rides with this phase's summary (from Phase 3): I4 (h1 vs h2 complete-then-throw), the h2 abnormal-end log-parity gap, the 413-path carve-out edge. None block Phase 4.

## Deviations

None recorded. A deviation gets an ID, a rationale, and John's sign-off here BEFORE work builds on it.
