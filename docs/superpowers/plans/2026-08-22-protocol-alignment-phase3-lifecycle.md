# Protocol Alignment Phase 3: Connection Lifecycle and Incomplete Responses Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every request's `pagi.connection` lifecycle truthful on both transports — h2 streams transition their state exactly once (today a stream close leaves `is_connected` true forever), `disconnect_future` obeys the three-state contract, and an application that leaves a response incomplete triggers the spec's forced abnormal closure (`on_disconnect` with `server_error`, never `on_complete`) instead of today's silent keep-alive/open-stream behavior.

**Architecture:** The send closures already know the response's terminal state (`$seq`); Phase 3 publishes it — every `$seq =` assignment gains a mirror into `$weak_self->{h1_seq}` (h1) / `$ss->{seq_state}` (h2) — so the request-completion paths can distinguish clean/incomplete without touching the dispatch logic. h1's app-return path gains the incomplete branch (close without terminator, no keep-alive, `_mark_disconnected('server_error')`); h2's dispatch wrapper gains the same via `submit_rst_stream`, and `_h2_on_close` becomes the clean/abnormal fork that finally drives per-stream `_mark_complete`/`_mark_disconnected`. `ConnectionState` gets the two-line §9.2 fix (late `disconnect_future` after clean completion stays pending).

**Tech Stack:** Perl 5 (perlbrew `perl-5.42.2@default`), Test2::V0, IO::Async, Net::Async::HTTP, Net::HTTP2::nghttp2 0.008 (`submit_rst_stream` confirmed present on the Session class).

**Spec:** design doc §9 (9.1 per-stream state, 9.2 late disconnect_future, 9.3 application failure and incomplete responses) + §15.3; PAGI spec `main` @ `56bf730` — Www.pod "Application Left a Response Incomplete" (forced closure, no synthesized terminal framing, `server_error`, never `on_complete`, already-disconnected carve-out), "Connection State" (state-transition order; `on_complete` only on full delivery), "Standard Disconnect Reasons".

## Global Constraints

- Worktree `/Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/pagi-0.4-alignment`, branch `fix/pagi-0.4-alignment`, starting HEAD = the commit this plan lands on (verify `git log --oneline -1`). Never the perf checkout.
- perlbrew wrapper for every Perl command; Test2::V0; TDD per behavior change (RED/GREEN tee'd to /tmp and READ in full); behavioral tests on real servers; foreground runs only; full suite only at the gate (under `caffeinate -i`).
- Spec invariants that bind everything here (Www.pod): a response is **incomplete** when `http.response.start` was sent but the terminal event never completed it — the final body with `more => 0`, a `file`/`fh` body, or (when `trailers => 1` was declared) the trailers event; `awaiting_trailers` at app-return IS incomplete. On incomplete: never synthesize terminal framing; close h1 (no keep-alive, no pipelining, no chunked terminator) / `RST_STREAM` only the h2 stream; fire `on_disconnect('server_error')`, never `on_complete`; log at error level UNLESS the client already disconnected (then neither log nor 500). State-transition order on abnormal: connected=false → reason → future → callbacks → disconnect event.
- `_mark_complete`/`_mark_disconnected` are idempotent; "exactly once" means exactly one of them wins per request/stream.
- Phase 1/2 invariants survive: closed-carve-outs, validate→advance ordering, `$seq` rollback paths (each rollback must ALSO update the mirror), D1's purity assumption.
- Commit only each task's files; never the plan file. Tracking table updated by the controller per task.

## File Structure

- `lib/PAGI/Server/ConnectionState.pm` — Task 1 (§9.2 fix + POD).
- `lib/PAGI/Server/Connection.pm` — Tasks 2–4: h1 send closure + `_handle_request` completion region (~2560–2610, `_mark_complete` at ~2581); `_h2_create_send` (mirrors), `_h2_dispatch_stream` wrapper (~640–680), `_h2_on_close` (~573–616), `_handle_disconnect` h2-stream sweep (~3272 region).
- Tests: extend `t/37-connection-state.t` (Task 1), `t/http-incomplete-response.t` + `t/53-trailers-framing.t` (Task 2), new `t/http2/30-connection-state.t` (Task 3), extend `t/http2/24-incomplete-response.t` (Task 4).
- `Changes`, `lib/PAGI/Server/Compliance.pod` — Task 5.

---

### Task 1: ConnectionState — late `disconnect_future` after clean completion (§9.2)

**Files:** Modify `lib/PAGI/Server/ConnectionState.pm`; extend `t/37-connection-state.t`.

**Interfaces:**
- Consumes: existing three-state internals (`_connected`, `_completed`, `_reason`).
- Produces: `disconnect_future()` called for the FIRST time after `_mark_complete` returns a Future that stays pending forever (never `done(undef)`); after `_mark_disconnected` it returns an already-resolved Future carrying the reason (unchanged); created-before-completion Futures stay pending on completion (already correct). POD wording for `disconnect_future` gains the three-state table.

- [ ] **Step 1 (RED):** add to t/37 a subtest constructing a bare `PAGI::Server::ConnectionState->new`, calling `_mark_complete`, THEN `disconnect_future`:

```perl
subtest 'disconnect_future requested after clean completion stays pending' => sub {
    my $conn = PAGI::Server::ConnectionState->new;
    $conn->_mark_complete;
    my $f = $conn->disconnect_future;
    ok( $f, 'future returned' );
    ok( !$f->is_ready, 'deliberately left pending — completion is not a disconnect' );

    my $conn2 = PAGI::Server::ConnectionState->new;
    $conn2->_mark_disconnected('client_closed');
    my $f2 = $conn2->disconnect_future;
    ok( $f2->is_ready, 'after abnormal disconnect: already resolved' );
    is( $f2->get, 'client_closed', 'carries the reason' );
};
```

Run t/37, tee `/tmp/phase3-task1-fail.out`, read: first `ok(!$f->is_ready)` FAILS today (`done(undef)` fires).

- [ ] **Step 2 (implement):** in `disconnect_future`, replace the resolve-if-disconnected block:

```perl
    # Resolve immediately only for an ABNORMAL end. After a clean completion
    # the connection is closed but this Future is deliberately left pending —
    # completion is not a disconnect (on_complete is the completion signal).
    if (!${$self->{_connected}} && !$self->{_completed}) {
        $self->{_future}->done(${$self->{_reason}});
    }
```

Update the method's POD paragraph accordingly.

- [ ] **Step 3 (GREEN + neighbors):** `prove -l t/37-connection-state.t t/connection-state-response-started.t t/39-disconnect-reasons.t` — tee `/tmp/phase3-task1-pass.out`, read; `podchecker lib/PAGI/Server/ConnectionState.pm`.
- [ ] **Step 4 (commit):** `fix(connection-state): late disconnect_future stays pending after clean completion`

---

### Task 2: h1 — publish response terminal state; enforce incomplete-response closure

**Files:** Modify `lib/PAGI/Server/Connection.pm`; extend `t/http-incomplete-response.t`, `t/53-trailers-framing.t`.

**Interfaces:**
- Consumes: the h1 send closure's `$seq`; the request-completion region in `_handle_request` (post-app-return: `_flush_pending_headers`, `_mark_complete` ~2581, keep-alive decision ~2585+); `_mark_disconnected` / `_handle_disconnect_and_close`.
- Produces:
  - **Mirror contract (Tasks 3–4 use the same pattern):** every statement in the h1 HTTP send closure that assigns `$seq` — the hoisted advance, every rollback, the HEAD/trailers-discard assignments — is immediately followed by `$weak_self->{h1_seq} = $seq;`. `$weak_self->{h1_seq}` is reset to `'initial'` where the per-request send state is created and where keep-alive resets per-request state (~2598 region).
  - In the app-return path, AFTER the existing no-response-started branch and BEFORE `_mark_complete`: the incomplete branch —

```perl
    # The application resolved with a started but incomplete response
    # (terminal body/file/fh — or promised trailers — never sent). Per the
    # PAGI spec this is an abnormal end: never synthesize the terminal
    # framing, never keep the connection alive, and report server_error
    # through on_disconnect — a truncated response must be observable as
    # truncated.
    if ($self->{response_started} && ($self->{h1_seq} // 'complete') ne 'complete') {
        $self->_flush_pending_headers;   # headers may still be buffered; no terminator follows
        unless ($self->{closed}) {
            warn "PAGI application returned with an incomplete response\n";
        }
        $self->_write_access_log;
        $self->{server}->_on_request_complete if $self->{server};
        $self->_handle_disconnect_and_close('server_error');
        return;
    }
```

  (Trace `_handle_disconnect_and_close` → `_mark_disconnected('server_error')` fires via the existing `current_connection_state` path — verify; if the sse/ws-mode guard at ~3272 blocks it for plain http it will not, adjust only as needed. The already-disconnected carve-out: when the client vanished first, the earlier disconnect already ran `_mark_disconnected` with its own reason, `{closed}` is true, so the warn is skipped and the mark is an idempotent no-op — spec-conformant.)
  - `_mark_complete` (~2581) now only runs on the complete path (it stays where it is; the incomplete branch returns before it).

- [ ] **Step 1 (RED):** extend `t/http-incomplete-response.t` with app paths + assertions (package vars via `our`):
  - `/half` — start(200, chunked) + body 'partial' `more=>1`, register `on_disconnect` → `$Inc::REASON`, `on_complete` → `$Inc::COMPLETED=1`, then return. Client (raw socket, boilerplate from t/53's chunked reader): reads headers + first chunk, then the connection CLOSES with NO `0\r\n\r\n` terminator (assert exact tail bytes received); `$Inc::REASON` is `'server_error'`; `$Inc::COMPLETED` unset; warning "returned with an incomplete response" captured via `$SIG{__WARN__}`.
  - `/half-cl` — start with `content-length => 100` + body 'short' `more=>1` + return: connection closes before 100 bytes delivered; same reason/warning assertions.
  - keep-alive veto: after `/half`, the same raw socket gets no further response (send a second request on it; expect EOF, not a response).
  - Control `/ok` unaffected; `on_complete` fires for it (assert `$Inc::OK_COMPLETED`).
    Tee `/tmp/phase3-task2-fail.out`, read: today `/half` keep-alives and `on_complete` fires — multiple FAILs expected.
- [ ] **Step 2 (implement)** per Interfaces (mirror lines + incomplete branch + reset sites).
- [ ] **Step 3:** update `t/53-trailers-framing.t` `/cl-trailers` per its Phase-3 cross-phase note: the app-return after the failed trailers send now closes the connection abnormally (`server_error`, no keep-alive) while the body remains fully delivered — extend the assertions (body 'ok' still received; connection closed; `on_disconnect` reason `server_error`; the "connection still usable" follow-up moves to a FRESH connection). Do not weaken the existing framing-error assertion.
- [ ] **Step 4 (GREEN + neighbors):** `prove -l t/http-incomplete-response.t t/53-trailers-framing.t t/52-mandatory-validation.t t/10-http-compliance.t t/22-content-length-keepalive.t t/37-connection-state.t` — tee `/tmp/phase3-task2-pass.out`, read fully. Fixture fixes allowed with explanation; never weaken validation or delete assertions.
- [ ] **Step 5 (commit):** `feat(h1): incomplete responses force abnormal closure per PAGI spec`

---

### Task 3: h2 — per-stream terminal wiring (`is_connected` finally flips)

**Files:** Modify `lib/PAGI/Server/Connection.pm`; create `t/http2/30-connection-state.t`.

**Interfaces:**
- Consumes: `$ss->{connection_state}` (created per stream, ~690); `_h2_on_close($stream_id, $error_code)`; the h2 HTTP send closure's `$seq`.
- Produces:
  - Mirror contract on h2: every `$seq =` assignment in `_h2_create_send` is followed by `$ss->{seq_state} = $seq;` (guarded `if $ss`); the HEAD block and rollback sites included. (`_h2_create_scope`'s stream init sets `seq_state => 'initial'`.)
  - `_h2_on_close` fork, before the existing cleanup (keep all existing teardown):

```perl
    if (my $cs = $stream->{connection_state}) {
        if (($stream->{seq_state} // '') eq 'complete' && !$error_code) {
            $cs->_mark_complete;
        } else {
            # Close before the response finished, or a nonzero error code.
            # NGHTTP2_CANCEL(8)/NO_ERROR-early = the client went away.
            $cs->_mark_disconnected('client_closed');
        }
    }
```

  - Connection-level teardown sweep: in `_handle_disconnect` (the h2-aware region near ~3272), when this is an h2 connection, iterate `values %{$self->{h2_streams}}` and `_mark_disconnected($reason)` any stream whose state hasn't reached terminal (idempotence makes double-marks safe) — so `server_shutdown`/socket-error reasons reach every open stream.
  - WebSocket/SSE h2 streams: their `connection_state` is N/A per spec (ws/sse use disconnect events) — `_h2_create_websocket_scope`/sse scope don't attach one (verify; if one is attached, leave behavior unchanged this task and note it).

- [ ] **Step 1 (RED):** create `t/http2/30-connection-state.t` (harness from t/http2/24): app captures `$scope->{'pagi.connection'}` per path into package vars and registers both callbacks.
  - clean GET → after response completes and the loop settles: `is_connected` false, `disconnect_reason` undef, `$H2CS::COMPLETE` fired, `$H2CS::DISCONNECT` not fired, `response_started` true. (Today: `is_connected` stays TRUE — the audit's evidence — so RED.)
  - client RST mid-stream (app started a streaming response, holds; client sends RST_STREAM then the test settles): `is_connected` false, reason `'client_closed'`, `on_disconnect` fired, `on_complete` not.
  - two concurrent streams on one connection, one completes while one is held: completed stream's state terminal-complete, held stream still `is_connected` — independence per §9.1.
    Tee `/tmp/phase3-task3-fail.out`, read.
- [ ] **Step 2 (implement)** per Interfaces.
- [ ] **Step 3 (GREEN + neighbors):** `prove -l t/http2/30-connection-state.t t/http2/17-h2-ws-sse-no-connection-state.t t/http2/05-request-lifecycle.t t/http2/26-mandatory-validation.t t/http2/29-fullflush-cancel.t` — tee `/tmp/phase3-task3-pass.out`, read fully.
- [ ] **Step 4 (commit):** `feat(h2): stream close drives per-stream connection state exactly once`

---

### Task 4: h2 — incomplete responses and post-start failures reset the stream

**Files:** Modify `lib/PAGI/Server/Connection.pm`; extend `t/http2/24-incomplete-response.t`.

**Interfaces:**
- Consumes: `_h2_dispatch_stream`'s async wrapper (~640–680: currently only the no-response-started 500 backstop + log-only post-start failure); `$ss->{seq_state}` (Task 3); `$session->submit_rst_stream($stream_id, $error_code)` (verify signature in `lib/PAGI/Server/Protocol/HTTP2.pm` / Session.pm; error code = `Net::HTTP2::nghttp2::NGHTTP2_INTERNAL_ERROR()` if the constant exists, else literal `2` with a comment naming it).
- Produces: in the wrapper, after the app's Future resolves, ordered branches:
  1. not started (existing 500 backstop — unchanged, but ALSO `_mark_disconnected('server_error')` on the stream's connection_state so `on_disconnect` fires per spec; today nothing marks it).
  2. started && `seq_state` ne 'complete' && the stream still exists (client hasn't reset): `warn "PAGI application returned with an incomplete response (HTTP/2 stream $stream_id)\n"`; `submit_rst_stream` + `_h2_write_pending`; `_mark_disconnected('server_error')`. Never END_STREAM, sibling streams untouched.
  3. app THREW after start (existing log-only branch): same RST + `_mark_disconnected('server_error')` treatment, keep the existing warn.
  4. Client-already-gone carve-out: stream missing from `h2_streams` or already terminal → no warn, no RST, no 500 (Task 3's `_h2_on_close` already marked it with the client's reason).
- Note: `_h2_on_close` will fire for our own RST; Task 3's fork must not overwrite `server_error` with `client_closed` — `_mark_disconnected` is idempotent (first mark wins), and the wrapper marks BEFORE the RST round-trips. State the ordering in a comment.

- [ ] **Step 1 (RED):** extend t/http2/24 with: `/incomplete` (start + body more=>1 + return; callbacks captured) → stream receives RST (client observes stream error/reset, not clean END_STREAM), reason `server_error`, `on_complete` never, warning captured, a SECOND stream on the same connection still serves; `/throw-after-start` (start + body more=>1 + die) → same observable contract (existing warn text retained); no-response path (existing test) now ALSO asserts `on_disconnect('server_error')` fired. Tee `/tmp/phase3-task4-fail.out`, read.
- [ ] **Step 2 (implement)** per Interfaces.
- [ ] **Step 3 (GREEN + neighbors):** `prove -l t/http2/24-incomplete-response.t t/http2/30-connection-state.t t/http2/12-error-handling.t t/http2/23-rst-rate-limit.t t/http2/28-file-fh.t` — tee `/tmp/phase3-task4-pass.out`, read fully.
- [ ] **Step 4 (commit):** `feat(h2): incomplete responses and post-start failures reset the stream`

---

### Task 5: Documentation

**Files:** `Changes`, `lib/PAGI/Server/Compliance.pod`.

- [ ] Changes (existing 0.002007 sections, no duplicate headings): per-stream h2 connection-state transitions; incomplete responses force abnormal closure on both transports (connection close / RST_STREAM, `on_disconnect('server_error')`); late `disconnect_future` three-state fix.
- [ ] Compliance.pod: connection-state section updated (h2 streams now transition; incomplete-response behavior documented; the `disconnect_future` contract). No temporal language. podchecker clean.
- [ ] Verify: podchecker + `prove -l t/00-load.t`. Commit: `docs: record lifecycle and incomplete-response conformance`

---

### Task 6: Phase gate

- [ ] Full suite under `caffeinate -i` → `/tmp/phase3-full-suite.out`, read the end + grep `^Result:`/`Failed`.
- [ ] `git diff <phase-3 base> --check`; podchecker + `perl -c -Ilib` on touched modules.
- [ ] Spec drift check (PAGI main still `56bf730`, else record).
- [ ] Tracking table finalized + committed (`docs: phase 3 verification gate results`).

---

## Tracking

| Task | Status | Commit | Tests (added/passing) | Verification evidence |
|------|--------|--------|-----------------------|-----------------------|
| 1. ConnectionState §9.2 | complete (review clean, zero findings) | 7b43a10 | +1 subtest / 25 tests, 3 files | /tmp/phase3-task1-pass.out; podchecker OK |
| 2. h1 incomplete | complete (review clean; VERIFY-NOTE resolved: no adjustment needed) | 4d38e5a | /half,/half-cl,+t/53 update / 93/93, 6 files | /tmp/phase3-task2-pass.out; mirror inventory reviewer-re-derived |
| 3. h2 stream state | complete (review clean; RST-during-delivery edge verified conservative-correct) | c081762 | t/http2/30 new / 29/29, 5 files | /tmp/phase3-task3-pass.out |
| 4. h2 incomplete/RST | complete (review clean; 2 judgment calls confirmed: carve-out-first, $cs gate) | 69113af | t/http2/24 extended / 39 tests, 5 files + full t/http2 169/169 | /tmp/phase3-task4-pass.out |
| 5. Docs | complete (review clean; all facts code-verified) | e70f179 | podchecker ×2; smoke 9/9 | task-5-report.md |
| 6. Phase gate | complete | (this commit) | full suite 118 files / 680 tests PASS | /tmp/phase3-full-suite.out; whitespace/POD/syntax clean; spec drift: none (56bf730) |
| Final review (opus, 80baee2..412f85e) | With fixes → wave clean | 43fd7f2, 5d1b797, b7f7f6b, f7f5d55 (+78f46b1 D2/D3 docs) | post-wave full suite 118 files / 687 tests PASS | re-review: all addressed, no new breakage; I4 verified untouched (John decision pending); /tmp/phase3-full-suite2.out |

## Deferred (carried, not this phase)

- Phase 2b (h2 trailers + §15.2 stragglers + preamble dedup) — dep-gated.
- h2-ws `websocket.keepalive` silent-ignore — Phase 4 (§10.2). SSE keep-alive honor — Phase 5 (§11.6). SSE detection media-range — Phase 5 (§11.5).
- Phase-1 minor: optional pinning test for the disconnect-window 500 suppression — fold into Task 2's warnings assertions if convenient, else remains deferred.

## Deviations

- **D2 (Task 4, commit 69113af)** — the dispatch wrapper's incomplete/threw branches are additionally gated on `$ss->{connection_state}` presence, and the client-gone carve-out is evaluated FIRST rather than last as the task's numbered list read. Rationale: ws/sse h2 streams carry no `pagi.connection` (spec: N/A) and no `seq_state` mirrors, so ungated they would be spuriously RST'd as "incomplete" on every normal app return; and the spec's own no-500-after-disconnect rule requires the carve-out to precede the backstop. Reviewer-verified against the spec's Applicability table and the scope-creation code. Controller-ruled 2026-08-22; recorded here per the final review's audit (was previously ledger-only).
- **D3 (final fix wave)** — `_h2_on_close`'s abnormal fork attributes `server_error` (not `client_closed`) when `error_code == 0` and `seq_state` never reached `complete`: a zero-code close with the response short of terminal can only be a server-originated early END_STREAM (today: the promised-trailers Phase-2b gap), which the PAGI spec classes as an incomplete response. Controller-ruled 2026-08-22 from final-review finding I1.
