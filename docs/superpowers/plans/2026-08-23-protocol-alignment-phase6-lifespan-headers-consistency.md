# Protocol Alignment Phase 6: Lifespan, Headers, and Connection-State Consistency

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement design §20 item 6 — lifespan `off` removal and worker propagation, Date and extension truthfulness, and the accumulated connection-state consistency items (two John-ratified) — leaving only Phase 2b and the §15.7 project-end gates.

**Architecture:** All changes live in `PAGI::Server` (lifespan/worker plumbing), `PAGI::Server::Connection` (h1/h2 dispatch and teardown paths), and `PAGI::Server::EventValidator` (one new field check). No new modules; every touched path keeps mandatory validation.

**Tech Stack:** Perl 5.42.2 (perlbrew `@default`), Test2::V0, IO::Async, Net::HTTP2::nghttp2 0.008.

**Spec:** `docs/superpowers/specs/2026-08-21-pagi-server-protocol-alignment-design.md` §12, §13, §14, §15.6, §20 item 6. PAGI spec authority (repo main @ `f04c029`): `PAGI/lib/PAGI/Spec/Lifespan.pod:160-173` (off-switch prohibition), `PAGI/lib/PAGI/Spec/Www.pod:1150-1165` (post-completion exceptions), `Www.pod:1040-1081` (standard disconnect reasons).

**Base commit:** `014c093` (Phase 5 complete).

## Global Constraints

- Perl commands ONLY via: `bash -c 'ulimit -n 10240; source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.42.2@default && <cmd>'` — the `ulimit` is mandatory (session shells carry nofile=1048576, which makes IO::Async::Function worker forks take ~4.2s and breaks timing-sensitive tests).
- TDD per task: failing test first, output captured IN FULL to a /tmp file and read (never judge truncated output), then implement, then green.
- Every outgoing event stays validated through `PAGI::Server::EventValidator`; no bypasses.
- Public behavior/config changes documented (POD/Compliance.pod/Changes) in the same commit that introduces them; POD timeless (no "now", "as before", "previously"); podchecker clean.
- Test output pristine; expected warns captured and asserted.
- Incremental commits (host sleeps); long suite runs under `caffeinate -is`.
- Ratified decisions (John, 2026-08-22, recorded in design §20 item 6): h1 post-completion exception fires `on_complete` (not `on_disconnect`), keeping the shipped warn+close; h2 413 marks `body_too_large` and wakes the pending receive. These are settled — do not re-litigate.
- `pagi.connection` is NOT APPLICABLE to websocket/sse scopes (Www.pod:1176-1180); do not add it to them.

---

### Task 1: Remove `lifespan_mode => 'off'`

Spec: design §12 ("**`lifespan_mode => 'off'` is removed**"); Lifespan.pod:166-173: "Skipping the lifespan protocol entirely is not a conforming option. … A server **must not** offer an 'off' switch for this protocol." Breaking change, pre-ratified (John merged the spec text).

**Files:**
- Modify: `lib/PAGI/Server.pm:2319-2321` (constructor), `:2466-2471` (setter), `:4060-4063` (skip logic — DELETE), POD `:1382-1400`
- Modify: `bin/pagi-server:388-393` (POD only; option at `:58` passes through to the constructor, which now rejects)
- Modify: `Changes` (new `Breaking Changes` heading under 0.002007)
- Test: `t/lifespan-mode.t` (replace the `'off'` subtest at `:48-71`)

**Interfaces:** Produces: constructor/setter accept only `auto|on`; die text `Invalid lifespan_mode '<mode>' - must be 'auto' or 'on' ('off' is nonconforming: the PAGI Lifespan spec forbids skipping the protocol)\n`.

- [ ] **Step 1: RED** — in `t/lifespan-mode.t`, replace the `'off'`-skips subtest with:

```perl
subtest "lifespan_mode 'off' is rejected (Lifespan spec: no off switch)" => sub {
    my $err = dies {
        PAGI::Server->new(app => sub { }, host => '127.0.0.1', port => 0,
                          quiet => 1, lifespan_mode => 'off');
    };
    like($err, qr/Invalid lifespan_mode 'off'/, 'constructor rejects off');
    like($err, qr/nonconforming/, 'error explains why');
};
```

Also extend the existing invalid-mode subtest (`:81-86`) to assert the new message shape still matches `qr/lifespan_mode/`. Run: expect the new subtest FAILS (constructor currently accepts 'off').
- [ ] **Step 2: Implement** — constructor and setter regex become `/\A(?:auto|on)\z/` with the die text above; delete the skip block at `:4060-4063` (with its comment); POD: remove the `off` item, document that `off` is rejected, cite the spec sentence; bin/pagi-server POD `--lifespan auto|on`. Do NOT add CLI-side validation (constructor covers it).
- [ ] **Step 3: GREEN** — `prove -l t/lifespan-mode.t t/06-lifespan.t t/lifespan-decline-clean-return.t t/00-load.t`; podchecker on Server.pm + bin/pagi-server.
- [ ] **Step 4: Changes** — under 0.002007 add `Breaking Changes` heading: lifespan_mode 'off' removed per PAGI Lifespan spec (skipping lifespan is nonconforming); `auto` remains the default, `on` remains strict mode.
- [ ] **Step 5: Commit** (`feat!: reject lifespan_mode 'off' per Lifespan spec`).

### Task 2: Propagate `lifespan_mode` and `lifespan_startup_timeout` to workers

Spec: design §12 ("Worker construction propagates at least: lifespan_mode; lifespan_startup_timeout; any retained supplemental validation-diagnostics setting" — the last is none: `validate_events` is deprecated/ignored since Phase 1).

**Files:**
- Modify: `lib/PAGI/Server.pm:3697-3721` (worker construction arg list)
- Test: `t/lifespan-worker-fields.t` (add behavioral subtest)

**Interfaces:** Consumes Task 1 (only `auto|on` valid).

- [ ] **Step 1: RED** — behavioral test: `workers => 2`, `lifespan_mode => 'on'`, an app that DIES on lifespan scope (decline). With propagation, both workers must fail startup (strict mode is fatal); today workers silently run with the default `auto` and stay up. Use the `t/17-worker-respawn-loop.t` idiom (capture worker stderr, assert `startup failed`, assert server exits). Expect FAIL today (workers stay up).
- [ ] **Step 2: Implement** — add `lifespan_mode => $self->{lifespan_mode}` and `lifespan_startup_timeout => $self->{lifespan_startup_timeout}` to the worker constructor call. (Recon note for the reviewer, NOT in scope: `max_connections`, `heartbeat_timeout`, `listener_backlog`, `loop_type`, `state`, `reuseport` are also unpropagated — record in the ledger as a John-packet observation, do not fix here.)
- [ ] **Step 3: GREEN** — new test + `t/11-multiworker.t t/13-multiworker-signal.t t/17-worker-respawn-loop.t`.
- [ ] **Step 4: Commit** (with POD note on worker inheritance under `lifespan_mode`).

### Task 3: h1 `_handle_request` outcome alignment (post-completion exception + trailers warn)

Spec: Www.pod:1156-1165 ("Delivery defines completion… on_complete fires, disconnect_reason() stays undef… SHOULD log… MAY close"). RATIFIED: keep warn+close, switch the state marking. Design §9.3 shares the contract across transports; h2 already behaves this way (`Connection.pm:826-830` log-only after `_mark_complete`).

**Files:**
- Modify: `lib/PAGI/Server/Connection.pm:3049-3072` (exception branch), `:3093-3106` (incomplete branch, trailers wording)
- Test: `t/23-connection-cleanup.t` (new subtest) or a new `t/56-post-completion-exception.t`

**Interfaces:** Consumes: `$self->{h1_seq}` mirrors the sequence machine ('complete' after terminal event); `_mark_complete` fires on_complete + leaves disconnect_reason undef (clean path uses it at `:3129-3131`).

- [ ] **Step 1: RED** — app sends a complete response (`http.response.start` + final body) then `die "post-completion boom\n"`. Assert via `pagi.connection`: `on_complete` fired, `on_disconnect` did NOT, `disconnect_reason` undef; warn captured matching `qr/PAGI application error/`; connection closed (a second request on the socket gets nothing — the shipped MAY-close stands). Expect FAIL today (`on_disconnect('server_error')` fires).
- [ ] **Step 2: Implement** — in the exception branch: when `($self->{h1_seq} // '') eq 'complete'`, warn `"PAGI application error (after response complete): $error\n"`, `_write_access_log`, `_on_request_complete`, mark completion the same way the clean path does (`current_connection_state->_mark_complete` — read `:3129-3131` and reuse), then `_close` WITHOUT `_handle_disconnect`. The started-but-incomplete and not-started branches stay byte-identical (t/23 and t/http-incomplete-response.t pin them).
- [ ] **Step 3: Trailers wording** — in the incomplete branch (`:3093-3106`), when `($self->{h1_seq} // '') eq 'awaiting_trailers'`, extend the warn to `"PAGI application returned with an incomplete response (trailers were declared but never sent)\n"`. MUST still match `qr/returned with an incomplete response/i` (`t/http-incomplete-response.t:398` pins that regex). Mirror the same parenthetical on the h2 incomplete warn (`Connection.pm:801-815`) keyed on `seq_state eq 'awaiting_trailers'`.
- [ ] **Step 4: GREEN** — new test + `t/23-connection-cleanup.t t/http-incomplete-response.t t/53-trailers-framing.t t/http2/24-incomplete-response.t`.
- [ ] **Step 5: Commit**.

### Task 4: h2 413 overrun marks `body_too_large` and wakes the pending receive

Spec: RATIFIED (design §20 item 6); Www.pod:1077-1079 (`body_too_large`: "Request body exceeded limit"). h1 chunked-overrun parity: `Connection.pm:3284-3291` already marks `body_too_large` and hands the receive a disconnect.

**Files:**
- Modify: `lib/PAGI/Server/Connection.pm:556-588` (`_h2_on_body` overrun branch)
- Test: `t/http2/12-error-handling.t` (extend the 413 subtests at `:340-475`)

**Interfaces:** Produces: after an overrun, the stream's `connection_state` (http scopes) is `_mark_disconnected('body_too_large')`; the receive queue gets the scope-appropriate disconnect event (`http.disconnect` / `sse.disconnect` with `reason => 'body_too_large'`); `body_complete` is set and `_h2_wake_pending` runs BEFORE the `delete $self->{h2_streams}{$stream_id}` — a pending `receive()` resolves instead of hanging. Task 5 consumes the `$cs` marking (its `client_gone` test reads `disconnect_reason`).

- [ ] **Step 1: RED** — h2 POST with `max_body_size => 40` and an app that `await $receive->()` immediately: assert (with a bounded wait, not an unbounded hang) that the app's receive resolves to `http.disconnect`, `on_disconnect` fires with `body_too_large`, and the client still sees the 413. Today: receive hangs and `$cs` stays connected — the test must fail by timeout-bounded assertion, not by hanging the suite (use the loop-iteration polling idiom from `t/http2/29`).
- [ ] **Step 2: Implement** — in the overrun branch, before the delete: `$stream->{body_complete} = 1;` push the disconnect event (http vs sse via `$stream->{is_sse}`); `$cs->_mark_disconnected('body_too_large') if my $cs = $stream->{connection_state};` then `$self->_h2_wake_pending($stream);` then the existing timer stops and delete. Keep the 413 submit exactly as-is (Date is Task 8's business).
- [ ] **Step 3: GREEN** — `t/http2/12-error-handling.t t/http2/16-sse-cleanup.t t/http2/30-connection-state.t`.
- [ ] **Step 4: Commit**.

### Task 5: h2 no-response fallback never submits on a dead stream

Source: P5T3 disclosure (SIGABRT: SSE app returns without send() on an already-RST'd stream) + final-review recon. `pagi.connection` is NOT APPLICABLE to sse/ws scopes — the fix is wrapper logic, not scope changes.

**Files:**
- Modify: `lib/PAGI/Server/Connection.pm:769-800` (dispatch-wrapper fallback)
- Test: `t/http2/22-denial-response.t` or new subtest in `t/http2/25-sse-decline.t`

**Interfaces:** Consumes Task 4 (`$cs` marked on 413 → `client_gone` true there). Produces: the synthesize-500/RST fallback consults a liveness fact that is true if and only if nghttp2 still owns the stream.

- [ ] **Step 1: Reproduce** — port the P5T3 reproduction: h2 SSE request; client sends RST_STREAM; server processes it (`_h2_on_close` deletes the entry — drive the loop); the SSE app then returns without `sse.start`. Today the wrapper's `!$weak_self->{h2_streams}{$stream_id}` fallback should make `client_gone` true — find the exact interleaving P5T3 hit (entry still present but nghttp2-side closed; the RST arrives but the deferred close callback hasn't run). Provoke THAT: feed the RST bytes but assert before/without driving the deferred close. If SIGABRT reproduces, capture it (`$?` signal 6 from a child-process runner — run the server in a fork so the suite survives). If it will not reproduce after honest effort, report NEEDS_CONTEXT with what you measured — do not fake it.
- [ ] **Step 2: Implement** — record stream death at the earliest server-visible moment: in `_h2_on_close` (`:640` area) set `$stream->{h2_closed} = 1` BEFORE any deferred work, and make the wrapper's fallback `my $stream_alive = $weak_self->{h2_streams}{$stream_id} && !$weak_self->{h2_streams}{$stream_id}{h2_closed};` — `client_gone` becomes true for closed-but-not-yet-deleted streams (both the `$cs` and no-`$cs` arms). The synthesize/RST evals stay as a second line of defense.
- [ ] **Step 3: GREEN** — the reproduction now passes quietly (no 500 submitted, no abort); `t/http2/22-denial-response.t t/http2/24-incomplete-response.t t/http2/25-sse-decline.t`.
- [ ] **Step 4: Commit**.

### Task 6: server-initiated h2 teardown reasons + ws receive fallback tokens

Spec: Www.pod:1044-1046 (`idle_timeout` is the standard token; h1 ships it at `Connection.pm:345`, `:2537`); design §6.1 transport-neutral semantics. Today `_h2_on_close` hardcodes `client_closed` (`:661`, `:672-681`), so an h2 SSE idle timeout reports the client closed — misattribution.

**Files:**
- Modify: `lib/PAGI/Server/Connection.pm` — sse idle expiry (`:2422` area), `_h2_on_close` (`:655-681`), ws keepalive pong-timeout teardown (`:2263-2309`), h1 receive fallbacks (`:4878-4940`), h2 receive fallbacks (`:1437-1487`)
- Test: `t/http2/16-sse-cleanup.t` (idle reason), `t/http2/31-ws-keepalive-disconnect.t` (pong-timeout reason unchanged pins), new fallback-reason subtest

**Interfaces:** Produces: `$ss->{server_close_reason}` — set by any server-initiated per-stream teardown before it routes through the generic close; `_h2_on_close` prefers it over the hardcoded fallbacks for both the `$cs` marking and the queued event's `reason`. h1: the receive-closure early returns use `_ws_disconnect_event` (`:3819-3830`) instead of the `{code => 1006, reason => ''}` literal, so a recorded `ws_disconnect_reason` survives the race.

- [ ] **Step 1: RED (h2 sse idle)** — per-stream `sse_idle_timeout => 1` on an h2 SSE stream that goes quiet: assert the app's `sse.disconnect` event has `reason => 'idle_timeout'` (today: `client_closed`).
- [ ] **Step 2: Implement h2** — sse idle expiry sets `$weak_ss->{server_close_reason} = 'idle_timeout'` before `resume_stream`; ws pong-timeout sets `'keepalive_timeout'` before its RST teardown (check `t/http2/31`'s current wire pins FIRST and keep them — the wire close frame semantics from Phase 4 must not change; only the receive-queue event/cs reason may gain the token). `_h2_on_close`: `my $reason = $stream->{server_close_reason};` — use it in the `$cs` marking arm (`$cs->_mark_disconnected($reason // 'client_closed')` in the error-code arm; the zero-error-code arms are untouched) and in both queued-event pushes.
- [ ] **Step 3: RED+fix (fallback tokens)** — h1: replace all `{ type => 'websocket.disconnect', code => 1006, reason => '' }` literals in `_create_websocket_receive` with `$weak_self->_ws_disconnect_event` (which already defaults to 1006/''). h2: replace the seven literals with a small `$fallback_disconnect->()` that prefers `$ss->{server_close_reason}` when the stream state is still reachable, else 1006/''. Pin one case: server shutdown (or idle) racing a pending ws receive yields the recorded token, not ''.
- [ ] **Step 4: GREEN** — `t/http2/16-sse-cleanup.t t/http2/31-ws-keepalive-disconnect.t t/http2/18-transport-leak.t t/14-websocket-invalid-utf8.t t/integration/websocket-disconnect-reason.t` + RELEASE_TESTING=1 `t/33-ws-sse-idle-timeout.t`.
- [ ] **Step 5: Commit**.

### Task 7: `max_requests` counts h1 SSE and WebSocket requests

Source: P5T5 ledgered concern (`_on_request_complete` never called on the h1 SSE path; h1 WS same; h2 counts every stream via `Connection.pm:835`).

**Files:**
- Modify: `lib/PAGI/Server/Connection.pm` — `_handle_sse_request` (`:4206-4272`, both fates), `_handle_websocket_request` (`:4794-4821`, completion)
- Test: `t/26-max-requests.t` (new subtest)

- [ ] **Step 1: RED** — worker with `max_requests => 2`: serve two SSE requests; assert the worker begins graceful shutdown (today it does not — SSE never counts). Reuse the `:67-123` idiom.
- [ ] **Step 2: Implement** — `$self->{server}->_on_request_complete if $self->{server};` at the same point `_write_access_log` runs in each path (SSE: both the keep-alive and close fates, exactly once per request; WS: at `:4817`).
- [ ] **Step 3: GREEN** — `t/26-max-requests.t t/05-sse.t t/04-websocket.t`.
- [ ] **Step 4: Commit**.

### Task 8: Date header consistency (design §13.1)

Spec: design §13.1 — Date added only when the app did not provide one, "consistently to HTTP/1, HTTP/2, SSE start, WebSocket denial responses, SSE decline responses, and server-generated responses."

**Files:**
- Modify: `lib/PAGI/Server/Connection.pm` — h1 normal (`:3523-3525`, UNCONDITIONAL today → duplicate bug), h1 sse-decline (`:4750-4752`), h1 ws-denial (`:5103-5105`), h2 sites with NO Date: 413 (`:512-516`, `:558-562`), synthesized 500 (`:793-797`), ws denial (`:1604-1608`), bare 403 (`:1612-1615`), sse decline (`:2053-2057`)
- Test: `t/10-http-compliance.t` or new `t/57-date-consistency.t`; `t/http2/21-http-date-header.t` (extend)

**Interfaces:** The supply-when-absent idiom is `unless (grep { lc($_->[0]) eq 'date' } @headers)` — identical to `:1293-1296`, `:1916-1920`, `:4634-4643`.

- [ ] **Step 1: RED** — (a) h1 app supplies `['Date', 'Mon, 01 Jan 2024 00:00:00 GMT']` on a normal response → assert exactly ONE Date line on the wire (today: two). (b) h2 413 and ws-denial responses carry a Date (today: none). (c) h1 sse-decline/ws-denial with app-supplied Date → exactly one.
- [ ] **Step 2: Implement** — make sites 3/6/7 conditional; add conditional Date pushes at the six h2 sites (server-built 413/500/403 lists have no app headers, so a plain push is correct there; denial/decline lists carry app headers, so grep first). `_send_error_response` (`:3745`) already builds the whole list itself — leave it.
- [ ] **Step 3: GREEN** — the new tests + `t/http2/21-http-date-header.t t/05-sse.t t/http2/14-sse-events.t t/websocket/30-denial-response.t t/sse-decline.t`.
- [ ] **Step 4: Commit**.

### Task 9: h2 strips connection-specific response headers (design amendment §13.3)

Source: P5 final review, live-reproduced — an app header `connection`/`transfer-encoding` on any h2 response path destroys the response (RFC 9113 §8.2.2 forbids connection-specific header fields in HTTP/2). Pre-existing on all four h2 emission paths.

**Files:**
- Modify: `docs/superpowers/specs/2026-08-21-pagi-server-protocol-alignment-design.md` — add §13.3 recording the rule (h2 strips `connection`, `keep-alive`, `proxy-connection`, `transfer-encoding`, `upgrade`, and `te` [te with any value other than `trailers`] from application-supplied response headers, logging one warn per response; h1 continues to pass them through)
- Modify: `lib/PAGI/Server/Connection.pm` — one private helper applied at all four h2 header-map sites (`:1290-1292` http, `:1905-1907` sse.start, `:1591-1594` ws denial, `:2033-2036` sse decline)
- Test: new `t/http2/33-connection-specific-headers.t`

**Interfaces:** Runs AFTER Task 8 in the same regions — coordinate line drift. Produces: `_h2_strip_connection_headers(\@headers)` returning the filtered list and warning `"PAGI: connection-specific header '<name>' stripped from HTTP/2 response (RFC 9113)\n"` once per stripped name.

- [ ] **Step 1: Amend the design doc first** (own commit): §13.3 text as above, referencing the P5 final-review reproduction and RFC 9113 §8.2.2.
- [ ] **Step 2: RED** — h2 app supplies `['connection','keep-alive']` and `['transfer-encoding','chunked']` on (a) `http.response.start` and (b) `sse.start`: assert the full response/body arrives intact, neither header appears in the client's HEADERS, and the warn fired. Today the response is destroyed (only `:status` arrives) — pin THAT as the RED.
- [ ] **Step 3: Implement + GREEN** — helper + four call sites; `t/http2/33-*.t t/http2/10-basic.t t/http2/14-sse-events.t t/http2/22-denial-response.t` (or nearest basic-h2 file — check `ls t/http2/`).
- [ ] **Step 4: Commit** (implementation; cite design §13.3).

### Task 10: `sse.keepalive` comment validated at arm time

Source: P5 final review M2 (deferred containment). Today an unencodable/non-string `comment` is accepted, the send Future SUCCEEDS, and the die fires later inside the timer tick (uncaught, unwinds `loop_once`). Fix at the validation boundary so the failure lands on the send Future that configured it.

**Files:**
- Modify: `lib/PAGI/Server/EventValidator.pm:381-389` (`_validate_sse_keepalive`) + its POD
- Test: `t/40-event-validation.t` (validator unit), `t/05-sse.t` or `t/55-sse-alignment.t` (behavioral)

**Interfaces:** Produces: `_validate_sse_keepalive` additionally croaks unless `comment` is absent, or a defined non-reference string that round-trips `Encode::encode('UTF-8', $comment, Encode::FB_CROAK)` (message: `sse.keepalive 'comment' must be a UTF-8-encodable string`). The timer writers (`:1939-1953`, `:4654-4664`) are untouched — their encode can no longer fail for validated input and stays as defense in depth.

- [ ] **Step 1: RED** — validator unit tests: `comment => "\x{D800}"` (unpaired surrogate) croaks; `comment => {}` croaks; `comment => 'håndtering'` passes. Behavioral: `sse.keepalive` with the surrogate comment → the send Future FAILS and no keepalive tick ever fires (today: Future succeeds, later tick dies).
- [ ] **Step 2: Implement + GREEN** — `t/40-event-validation.t t/05-sse.t t/55-sse-alignment.t t/http2/15-sse-keepalive.t`.
- [ ] **Step 3: Commit** (EventValidator POD updated same commit).

### Task 11: extension truthfulness (design §13.2)

Untruthful case from recon: WebSocket scopes (h1 `Connection.pm:4865`, h2 `:1429`) advertise `fullflush` whenever configured, but `validate_websocket_send` has no fullflush arm — a ws app that trusts the advertisement gets `Unrecognized event type` (croak). Also: the h1 inline fullflush re-checks (`:3699-3715` http, `:4767-4780` sse) are dead code — the validator croaks first (`EventValidator.pm:92-94`, `:296-298`).

**Files:**
- Modify: `lib/PAGI/Server/Connection.pm` — ws scope extension merges (`:1429`, `:4865`): delete `fullflush` from the per-scope copy; remove the two dead inline re-checks
- Test: `t/07-extensions.t` (extend: ws scope must NOT list fullflush even when configured; http/sse scopes still do)

- [ ] **Step 1: RED** — server configured with `extensions => { fullflush => {} }`: ws scope's `$scope->{extensions}` has NO `fullflush` key (today it does); h1 http scope still has it.
- [ ] **Step 2: Implement** — filter at the two ws scope-build sites (the merge already copies the hash — delete the key from the copy). Remove the dead inline checks; run `t/40-event-validation.t` + `t/http2/29-fullflush-cancel.t` to prove the validator still croaks unadvertised fullflush with pristine output (the removed warn must not be pinned anywhere — grep t/ first; if a test pins that warn text, STOP and report instead of deleting the assertion).
- [ ] **Step 3: GREEN** — `t/07-extensions.t t/40-event-validation.t t/http2/29-fullflush-cancel.t`.
- [ ] **Step 4: Commit**.

### Task 12: documentation sweep

**Files:**
- Modify: `lib/PAGI/Server/Compliance.pod` — new `=head2 Lifespan Modes` (auto default, on strict, off rejected + spec quote); extend `=head2 Response Headers` (`:752`) with the full Date coverage list and the h2 connection-specific stripping; new short `=head2 Extension Advertisement` (truthfulness rule, ws/fullflush case)
- Modify: `Changes` — bullets for Tasks 2-11 under 0.002007 (Task 1 already added `Breaking Changes`)
- Audit: every new public claim greps back to code (the Task-6 §11.4-style audit discipline)
- Test: `t/00-load.t` + podchecker

- [ ] **Step 1:** Write the sections; fact-check each sentence against the shipped code (quote file:line in the report).
- [ ] **Step 2:** podchecker both files; `prove -l t/00-load.t`.
- [ ] **Step 3: Commit**.

### Task 13: Phase gate

- [ ] Full recursive suite: `caffeinate -is bash -c 'ulimit -n 10240; … prove -lr t/'` → `/tmp/phase6-full-suite.out`, read in full. Baseline: 121 files / 751 tests at `014c093` (plus this phase's additions).
- [ ] RELEASE_TESTING=1 pass of `t/33-ws-sse-idle-timeout.t t/31-memory-leak.t` (Task 6 touched idle paths).
- [ ] Hygiene: `git diff 014c093 --check`; podchecker (Server.pm, Connection.pm, Compliance.pod, EventValidator.pm, bin/pagi-server); `perl -c -Ilib lib/PAGI/Server/Connection.pm` ("Subroutine redefined" warnings are pre-existing circular-use artifacts).
- [ ] Spec drift: PAGI repo main still `f04c029` (re-run design §2.2 check if moved).
- [ ] Tracking table below filled with real SHAs/counts; commit.

---

## Tracking

| Task | Status | Commit | Tests (added/passing) | Verification evidence |
|------|--------|--------|-----------------------|-----------------------|
| 1. lifespan off removal | not started | — | — | — |
| 2. Worker propagation | not started | — | — | — |
| 3. h1 outcome alignment | not started | — | — | — |
| 4. h2 413 body_too_large | not started | — | — | — |
| 5. h2 dead-stream guard | not started | — | — | — |
| 6. Teardown reasons | not started | — | — | — |
| 7. max_requests SSE/WS | not started | — | — | — |
| 8. Date consistency | not started | — | — | — |
| 9. h2 header stripping | not started | — | — | — |
| 10. keepalive comment validation | not started | — | — | — |
| 11. Extension truthfulness | not started | — | — | — |
| 12. Docs sweep | not started | — | — | — |
| 13. Phase gate | not started | — | — | — |

## Deviations

None recorded. A deviation gets an ID, a rationale, and John's sign-off here BEFORE work builds on it.

## Deferred / John packet (carried out of Phase 6)

- Unpropagated worker args beyond design scope: `max_connections`, `heartbeat_timeout`, `listener_backlog`, `loop_type`, `state`, `reuseport` (observation, Task 2).
- Production hosts with nofile≈1M pay ~4.2s per IO::Async::Function worker (re)spawn (upstream IO::Async::OS post-fork sweep; measured 2026-08-23).
- h1 passes connection-specific app headers through untouched (legal-but-conflicting; §13.3 scopes the strip to h2 only).
- Phase 2b (h2 trailers via Net::HTTP2::nghttp2 `submit_trailer`), §15.7 branch gates, 0.002007 version collision with `perf/http-serving-25`.
