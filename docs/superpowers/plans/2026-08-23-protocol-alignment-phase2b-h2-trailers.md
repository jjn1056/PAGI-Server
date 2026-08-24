# Protocol Alignment Phase 2b: HTTP/2 Trailers Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement HTTP/2 trailers against Net::HTTP2::nghttp2 0.009 (design §8.3), fix the received-HEADERS classification bug, and close the deferred §15.2 test stragglers and file/fh preamble dedup — completing the Phase 2b punch list.

**Architecture:** `lib/PAGI/Server/Protocol/HTTP2.pm` (nghttp2 callback layer: pending-header-block classification via `headers_category`) and `lib/PAGI/Server/Connection.pm` (h2 send closure: trailers arm replaces the stub; data-provider third return value; constants; preamble dedup). Shared sequence machine (`EventValidator::advance_http`) already models content-vs-response completion (`awaiting_trailers`) — Task 3 AUDITS it against the handoff checklist; nobody rebuilds it.

**Tech Stack:** Perl 5.42.2 (perlbrew `@default`), Net::HTTP2::nghttp2 **0.009** (installed from CPAN, verified: `submit_trailer` wrapper in Session.pm:148 with pseudo-header rejection; three-value data-callback; `submit_data` 4th arg; `:http2_errors`/`:header_categories` exports; `headers_category` on on_frame_recv frames), Test2::V0.

**Spec:** design §8.3 (binding; quoted in full in Task 4's brief), the 0.009 integration handoff notes (John, 2026-08-23 — primary brief, recorded below where load-bearing), PAGI spec main @ `f04c029`. RFC 9113 §8.1 (no pseudo-headers in trailers), RFC 9110 §6.5.

**Base commit:** `49c62ba` (Phase 6 complete). Baseline: full suite 124 files / 787 tests PASS; h2 suite re-verified against 0.009 before Task 1.

## Global Constraints

- Perl commands ONLY via: `bash -c 'ulimit -n 10240; source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.42.2@default && <cmd>'`.
- TDD per task; RED output captured IN FULL to /tmp and read; pristine output; POD timeless, same-commit; podchecker clean.
- Scope guard (John): do NOT modify Net::HTTP2::nghttp2 or the PAGI specification. Spec follow-ups are RECORDED (Task 6), not applied.
- Never use a thrown stale-stream native error as disconnect detection: PAGI state (`h2_closed`, `pagi.connection`) is the authority. Before disconnect, an immediate native failure fails the send Future; after disconnect, sends stay successful no-ops (Phase 6 carve-out: `(!$ss || $ss->{h2_closed})`).
- Never resolve application Futures or re-enter app code from inside a native callback; defer via `loop->later` (existing discipline).
- Send Futures resolve when nghttp2 ACCEPTS a submission, not when bytes reach the peer (existing semantics).
- The seq-mirror discipline holds: `$ss->{seq_state}` is updated at EVERY `$seq =` site.
- No h1 behavior changes except where a task names them; t/53-trailers-framing.t wire pins must survive byte-for-byte.

---

### Task 1: Dependency floor 0.009 + error-code constants

**Files:**
- Modify: `cpanfile:28` (`recommends 'Net::HTTP2::nghttp2', '0.009';`), `lib/PAGI/Server/Protocol/HTTP2.pm:45` (`MIN_NGHTTP2_VERSION => '0.009'`)
- Modify: `lib/PAGI/Server/Connection.pm:983-1001` — `_h2_rst_error_code`/`_h2_rst_cancel_code`: the `can(...)` fallback shims become direct constant calls (`Net::HTTP2::nghttp2::NGHTTP2_INTERNAL_ERROR()` / `NGHTTP2_CANCEL()`); keep the two helper subs (their names carry intent), rewrite bodies + comments (0.009 exports these; drop the 0.008 fallback narrative — timeless comments)
- Modify: `t/http2/20-sse-transport.t:172`, `t/http2/32-sse-detection-encoding.t:527` — replace literal `8` with the exported constant (match each file's import style)
- Test: `t/00-load.t` + the two touched test files

- [ ] **Step 1:** Verify installed 0.009 (`perl -MNet::HTTP2::nghttp2 -E 'say $Net::HTTP2::nghttp2::VERSION'`) and that `Protocol/HTTP2.pm`'s version check passes with the bump (read the check at `:49`).
- [ ] **Step 2:** Make the four edits. RED is not applicable for the constants swap (behavior-identical, values verified 2/8); the MIN bump is pinned by running `t/http2/01-*.t`-era availability tests.
- [ ] **Step 3:** GREEN — `prove -lr t/http2/ t/00-load.t` (full h2 dir: the bump gates every h2 test), podchecker Protocol/HTTP2.pm + Connection.pm.
- [ ] **Step 4:** Commit (include the cpanfile comment lines staying accurate).

### Task 2: Received-HEADERS classification (LIVE BUG, reproduction first)

Recon-confirmed today: `Protocol/HTTP2.pm:193-205` `on_begin_headers` unconditionally reinitializes `{streams}{$stream_id}` on EVERY HEADERS frame (wiping `client_end_stream` so the `:244` guard can't fire), and `on_frame_recv:240-285` re-invokes `on_request` — so a client sending request trailers gets a SECOND app dispatch on the same stream with an empty pseudo hash, destroying the in-flight request's state (`_h2_on_request` at Connection.pm:493-580 replaces `h2_streams{$stream_id}` wholesale). Secondary: the original request's `body_complete` never fires (END_STREAM arrives on the trailer HEADERS, not DATA).

**Files:**
- Modify: `lib/PAGI/Server/Protocol/HTTP2.pm` (`on_begin_headers`, `on_header`, `on_frame_recv`)
- Modify: `lib/PAGI/Server/Connection.pm` `_h2_on_request` (defensive: refuse to overwrite an existing `h2_streams` entry — croak-log-and-ignore rather than replace; belt under the protocol-layer suspenders)
- Test: new `t/http2/34-request-trailers.t`

**Interfaces:** Produces: `on_frame_recv` classifies committed header blocks by `$frame->{headers_category}` — `NGHTTP2_HCAT_REQUEST` (=0) establishes the request (current behavior); `NGHTTP2_HCAT_HEADERS` (=3) on an established stream = request trailers: validate (same name/value byte rules; any pseudo-header in a trailer block → `NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE`, RFC 9113 §8.1 malformed), then DISCARD the fields (PAGI defines no request-trailer receive event yet — recorded in Task 6), then honor END_STREAM: set `client_end_stream` and deliver body-completion exactly as the DATA+END_STREAM arm does (`on_body(..., 1)` with zero bytes — read `:297-305` and reuse). Never reinitialize the committed request block.

- [ ] **Step 1: Reproduce (RED)** — raw 0.009 client: HEADERS (no END_STREAM) + DATA + trailer HEADERS (`x-checksum: abc`, END_STREAM) against a server app that counts invocations and reads the body to completion. Pin today's breakage: second invocation / empty pseudo / body never completing (whichever manifests — capture exactly what does, in full).
- [ ] **Step 2: Implement** — pending-block scheme: `on_begin_headers` initializes `{streams}{$stream_id}{pending} = { headers => [], pseudo => {}, header_list_size => 0 }` when an entry EXISTS, else the entry itself (first block); `on_header` appends to the pending block when present; `on_frame_recv` commits by category. Keep the max_header_list_size accounting on whichever block is accumulating. Keep the `:244` END_STREAM-already guard working (it must now survive trailer accumulation).
- [ ] **Step 3: GREEN** — new tests: single app invocation; request body delivered complete (the app's `receive()` sees `more => 0`/`body_complete`) when END_STREAM rides the trailer HEADERS; trailers discarded silently; pseudo-header in trailers → stream error, original request unharmed or torn down per the TEMPORAL_CALLBACK_FAILURE path (assert which, from the code); response still completes cleanly. Plus regression: `t/http2/02-*.t`-era basic request tests, `t/http2/26-mandatory-validation.t`.
- [ ] **Step 4: Commit.**

### Task 3: Shared state-machine AUDIT (verify, do not rebuild)

The handoff's roadmap item 2 describes machinery Phases 1-3 already shipped. This task verifies each checklist item against code + tests and adds ONLY missing pins. HARD RULE: no redesign of `advance_http`/seq handling; a gap becomes a pin or a report line, not a rewrite.

Checklist (each → verified-with-evidence / pinned-now / gap-reported):
1. content vs response completion: terminal body in `started_t` → `awaiting_trailers`, not complete (EventValidator.pm:474-475) — verify + confirm t/53 pins h1, t/http2/24 pins h2 incomplete.
2. Exactly one trailers event; undeclared/early/repeated rejected (`:462-465`).
3. Empty trailer list valid AND terminal — validator accepts absent/empty `headers` (`:154-160`); h1 wire: empty loop yields `0\r\n\r\n` (Connection.pm:3923-3935) — ADD an h1 wire pin if none exists (grep t/53 + t/ for empty-trailers).
4. Promised-unsent → incomplete per policy (both transports, with the trailers parenthetical) — verify pins exist.
5. HEAD follows the logical sequence with suppressed bytes (h1 :3895-3901, h2 :1258-1274; seq machine method-agnostic) — verify t/http2/27 `/trailers` + t/53 head-trailers cover it.
6. h1 Content-Length + trailers loud failure (D1 advance-then-rollback) — verify t/53 `/cl-trailers`.

- [ ] **Step 1:** Work the checklist; write the evidence table (claim → file:line → test) into the task report.
- [ ] **Step 2:** Add any missing pins (expected: at most the empty-trailers h1 wire pin). RED-first for each added pin where the behavior could regress.
- [ ] **Step 3:** GREEN — `t/53-trailers-framing.t t/40-event-validation.t t/http-incomplete-response.t t/http2/24-incomplete-response.t t/http2/27-head.t` + any new file. Commit (tests only).

### Task 4: HTTP/2 trailers output (the core)

Design §8.3 (binding): "a terminal body event does not end the HTTP/2 stream. A following `http.response.trailers` submits trailing HEADERS with END_STREAM and marks the response terminal. Without the declaration, the trailers event fails. On HEAD, it is validated and discarded while still completing the response."

**Files:**
- Modify: `lib/PAGI/Server/Connection.pm` — remove the stub (`:1276-1282`); new trailers arm in `_h2_create_send`; `$data_callback` third return value (`:1188`, `:1192`); the plain-body terminal arm must route trailers-declared responses through the STREAMING path (a single-shot `submit_response` cannot end content without ending the stream — verify how step 6 currently chooses complete-vs-streaming and force streaming when `started_t`)
- Test: new `t/http2/35-trailers.t` (+ extend `t/http2/27-head.t` if its `/trailers` route needs the non-stub assertion updated)

**Interfaces:** Consumes: Task 1's 0.009 floor; `$ss->{seq_state}` mirror (current inside the callback); the h2_closed carve-out (Phase 6) at the closure top. Produces: on `http.response.trailers` (non-HEAD): advance-then-rollback (h1 D1 pattern) — `my $seq_before = $seq; $seq = advance_http($seq, $event); $ss->{seq_state} = $seq if $ss;` then `eval { $weak_self->{h2_session}->submit_trailer($stream_id, headers => [validated tuples]) }` — on throw with the connection still live: restore `$seq`/mirror, `die` (fails the send Future; app return then hits the existing incomplete arm → RST `NGHTTP2_INTERNAL_ERROR`). Trailer tuples go through `_validate_header_name/_validate_header_value` (byte rules) — EventValidator already shape-checks; pseudo-headers are additionally rejected by the 0.009 wrapper. Empty/absent `headers` still calls `submit_trailer` with `[]` (terminal). After submit: `resume_stream($stream_id)` if streaming + `_h2_write_pending` (flush DATA then trailing HEADERS; nghttp2 orders trailers after queued data).

Data-callback change: at both `$eof=1` return sites, `my $no_end = (($ss->{seq_state} // '') eq 'awaiting_trailers') ? 1 : 0; return ($chunk_or_empty, $eof, $no_end);` — two-value returns elsewhere unchanged.

- [ ] **Step 1: RED** — acceptance shape: `trailers => 1` start, streamed body (`more => 1` then terminal), `http.response.trailers` with `[['x-checksum','abc'],['set-cookie','a=1'],['set-cookie','b=2']]`. Client (raw 0.009) asserts via `on_frame_recv`: final DATA frame WITHOUT `NGHTTP2_FLAG_END_STREAM`; subsequent HEADERS frame (`headers_category == NGHTTP2_HCAT_HEADERS`) WITH END_STREAM carrying the fields in order with duplicates preserved; stream closes cleanly (`on_stream_close` error code 0); `on_complete` fires. Today: the stub dies — capture that as RED.
- [ ] **Step 2: Implement** per Interfaces. Read the step-6 plain-body arm first and handle the single-shot-body-with-trailers case (streaming path). HEAD arm (`:1258-1274`) stays untouched, BEFORE the new arm.
- [ ] **Step 3: More GREEN coverage** (notes item 6, adapted): empty body + empty trailers (start(trailers)→terminal empty body→trailers []) → wire shows zero-length DATA w/o END_STREAM (or none) + empty trailing HEADERS w/ END_STREAM; file-body + trailers and fh-body + trailers (provider path under flow control — reuse t/http2/28 fixtures; withhold WINDOW_UPDATE then release, trailers arrive after all DATA); native-failure rollback (submit a trailer on a stream the client has RST'd BEFORE h2_closed processing — if not provokable deterministically, spy-wrap submit_trailer to throw once and assert seq rollback + Future failure + subsequent incomplete-RST uses NGHTTP2_INTERNAL_ERROR); disconnect-before-trailers → send Future succeeds as no-op (h2_closed carve-out; extend t/http2/12's post-413 idiom); promised-but-missing still RSTs INTERNAL_ERROR (t/http2/24 regression); HEAD `/trailers` completes (t/http2/27). Direct `submit_data` 4-arg path: NOT exercised — plain h2 HTTP never calls submit_data directly (recon item 5: WS-only) — state this in the report rather than testing a path that doesn't exist.
- [ ] **Step 4: GREEN scope** — `t/http2/35-trailers.t t/http2/24-incomplete-response.t t/http2/27-head.t t/http2/28-file-fh.t t/http2/29-fullflush-cancel.t t/53-trailers-framing.t t/http2/12-error-handling.t`. Commit(s) — implementation and extended coverage may be separate commits.

### Task 5: §15.2 stragglers + file/fh preamble dedup

**Files:**
- Test additions: `t/http2/27-head.t` (HEAD×fh route + pin), `t/42-file-response.t` (h1 HEAD×file, HEAD×fh, fh offset-past-EOF), `t/http2/28-file-fh.t` (h2 fh offset-past-EOF; seek-failure-on-pipe), h1 seek-failure-on-pipe (t/42), explicit post-RST no-op send (h2: app sends AFTER peer RST and the Future resolves successfully — extend t/http2/29 or 12)
- Modify: `lib/PAGI/Server/Connection.pm` — extract the duplicated h2 file/fh preamble+tail (advance/mirror + streaming-start + failure-rollback: `:1284-1317` vs `:1364-1387`, tails `:1354-1362` vs `:1406-1414`) into one private helper; the distinct middles (file checks/stat/sync-threshold vs seek/read loop) stay in the arms. NOTE: Task 4 edits the same closures FIRST — re-locate by grep, expect drift.

Straggler matrix from recon (add exactly these): HEAD×fh — missing both transports; fh past-EOF — missing both (h1 has file-past-EOF only); seek-failure-on-pipe — missing both (`Cannot seek` die sites: h2 `:1389`, h1 `:5662`); post-RST no-op send Future-resolution — missing as a direct pin.

- [ ] **Step 1:** RED/GREEN each straggler pin (behavioral, real sockets/streams; a pipe fh for the seek case).
- [ ] **Step 2:** Dedup refactor — pure restructuring: `prove -l t/http2/28-file-fh.t t/http2/27-head.t t/42-file-response.t t/http2/35-trailers.t` must pass identically before/after (capture both runs).
- [ ] **Step 3:** Report (not fix): h1 lacks the `-f`/`-r` pre-checks h2 has (h1 fails at open/stat) — John-packet observation. HTTP1.pm `serialize_trailers` has zero lib/ callers (parallel implementation of the h1 inline writer) — flag for the packet; do NOT remove or rewire without sign-off (public POD'd surface).
- [ ] **Step 4:** Commit(s).

### Task 6: Docs, Changes, and spec-followup recording

**Files:**
- Modify: `lib/PAGI/Server/Compliance.pod:433-446` — remove the Known Limitations trailers item; add a trailers-supported passage under the HTTP/2 section (framing: final DATA without END_STREAM, trailing HEADERS with END_STREAM, empty-list terminal, HEAD discard, request-trailers validated-and-discarded)
- Modify: `Changes` (0.002007 bullets: h2 trailers; request-trailers classification fix; 0.009 floor; straggler tests)
- Modify: design doc — §8.3: mark the dependency gap resolved (0.009, date); append the handoff's "PAGI specification follow-up" list verbatim as recorded-not-applied proposals (content-vs-response completion wording, exactly-one-terminal-trailer-event, DATA/HEADERS mapping, trailer-field validation, HTTP/1.0 + CL-plus-trailers, future request-trailer receive event)
- Audit: every new claim → file:line fact table in the report

- [ ] **Step 1:** Write; fact-check each sentence; podchecker; `prove -l t/00-load.t`.
- [ ] **Step 2:** Commit.

### Task 7: Phase gate

- [ ] Full recursive suite under `caffeinate -is` + `ulimit -n 10240` → `/tmp/phase2b-full-suite.out`, read in full. Baseline 124/787 + this phase's additions.
- [ ] Hygiene: `git diff 49c62ba --check`; podchecker (Connection.pm, Protocol/HTTP2.pm, Compliance.pod, Server.pm); `perl -c -Ilib` both main modules.
- [ ] Spec drift: PAGI main still `f04c029`.
- [ ] Verify the installed Net::HTTP2::nghttp2 is the CPAN 0.009 release (`cpanm --info`), per the handoff's final-verification note.
- [ ] Tracking table + commit.

---

## Tracking

| Task | Status | Commit | Tests (added/passing) | Verification evidence |
|------|--------|--------|-----------------------|-----------------------|
| 1. 0.009 floor + constants | complete | 66cbdc2 | 35 files/236 (h2 dir + load) | Approved first pass; version compare verified numeric (UNIVERSAL::VERSION live probe) |
| 2. Received-HEADERS classification | complete | 0a9668f | RED reproduced live bug (2nd dispatch, empty pseudo, crash) → 5 files/47; new t/http2/34 | Approved first pass; normal path traced zero-change; HPACK repro frame verified real RFC 7541 encoding |
| 3. State-machine audit | complete | 15ecf55 | 5 files/65; +9 empty/absent-trailers wire pins | Approved; all six evidence rows independently re-verified; no lib changes (audit constraint held) |
| 4. h2 trailers output | complete | 7cd734f, 974e48b, ada781c | t/http2/35 (9 subtests); 5 files/51 scoped | Opus review: deferral deviation RATIFIED (byte-exact truncation evidence; DEFERRED-provider mechanism confirmed); fix round 1: _close trailer-wait release + post-await liveness guard + park-teardown/deferred-failure tests; re-review clean ×2 runs |
| 5. Stragglers + dedup | complete | 4e437e6, ce405ff | 6 files/84; dedup before/after diff EMPTY | Approved first pass; seek-on-pipe discriminating power traced; three-closure split accepted (merging would emit headers before -f/-r checks); zero trailers lines moved |
| 6. Docs + spec-followup record | complete | 5e68780, 9e617f4 | 29/29 + podchecker | Approved after round 1 (reviewer caught inverted Changes claim — sub-0.009 --http2 dies loudly, not silent; GOAWAY claim now wire-pinned type-7-and-no-type-3); design §8.3 marked resolved + six spec proposals recorded |
| 7. Phase gate | complete | (this commit) | full recursive suite 126 files / 835 tests PASS | hygiene clean (whitespace, podchecker ×5, perl -c ×2); PAGI main f04c029 (no drift); installed dist verified = CPAN JJNAPIORK/Net-HTTP2-nghttp2-0.009.tar.gz; 11 implementation commits |

| Final review + fix wave | complete | 807f43b, 57b9e3c, 235bd25, b3fde41 | post-wave full suite 126 files / 838 tests PASS | Final whole-phase review (opus): §8.3 clause-by-clause Met, §21 item 3 genuinely met; combined request+response-trailers case built by the reviewer and passing. Fix wave: h2 trailer strip (sixth §13.3 path, te forbidden outright in trailer blocks); combined-case test; POD/comment/die-text corrections; post-await liveness tighten; h2 absent-trailers pin. Deviations D-P2B-1..3 recorded (D-1/D-2 awaiting John sign-off). Re-review all addressed ×2 stable |

## Deviations

- **D-P2B-1 (SIGNED OFF — John, 2026-08-24, conditional on h1/h2 contract sync + no spec contradiction; both verified): trailer submission deferred to the provider's terminal-EOF delivery.** Verification: spec Www.pod:2119-2122 explicitly describes a send Future that "remains pending until the buffer drains enough for the server to continue processing the event" — the deferred trailers send is that clause on h2 framing; contract synced with h1 (both resolve on transport acceptance, both participate in backpressure; h1's trailers append to the byte stream so its send never parks — a framing-inherent difference, not divergence). The plan's Task-4 pseudocode called `submit_trailer` synchronously from the trailers arm. Implemented instead: when the data provider has not drained, the submission is staged and issued at the provider's own terminal (EOF) invocation. Rationale (measured, not argued): calling `submit_trailer` while the provider was DEFERRED caused nghttp2 to silently truncate the body — reproduced with byte-exact evidence (200000-byte body delivered 196607; the 3393-byte loss equals the queued bytes at early-submit time; provider never polled again) and confirmed mechanically (a DEFERRED provider's DATA item is detached from nghttp2's outbound queue, so early trailing HEADERS orphans it; the file/fh path defers constantly). App-visible consequence: under flow control, the trailers send Future can stay pending until the peer's window lets the body drain — trailers participate in backpressure like body sends (documented in Compliance.pod). Ratified controller+opus-reviewer level (ledger, Task 4); design §8.3 binds only the wire shape, which is met.
- **D-P2B-2 (SIGNED OFF — John, 2026-08-24, same conditions; both verified): promised-but-unsent trailers on h2 now reset the stream with INTERNAL_ERROR instead of closing cleanly.** Verification: spec Www.pod:705-746 MANDATES exactly this per transport ("Over HTTP/2 the server resets the stream (RST_STREAM, e.g. INTERNAL_ERROR)"; h1 closes without the chunk terminator) — the stub-era clean close was the nonconforming behavior; h1/h2 warn text, server_error reporting, and wire-observability are synced. Before this phase, the terminal body carried END_STREAM regardless of the trailers declaration (the stub), so an app that declared trailers and never sent them produced a clean wire close; `t/http2/24-incomplete-response.t`'s assertion pinned that. With §8.3 implemented, the terminal body withholds END_STREAM, so the same app now produces an observably incomplete response: RST_STREAM with NGHTTP2_INTERNAL_ERROR (design §9.3 and §21 item 8 demand exactly this). The test assertion was updated accordingly. Proof: t/http2/24 before/after; the old clean close was the stub artifact, not designed behavior.
- **D-P2B-3 (recorded; no behavior change): the dedup helper is three closures, not the plan's "one or two"** — merging the streaming-start into the pre-eval helper would have emitted response headers before the file arm's `-f`/`-r` checks (wire-visible regression, caught by trace). Task 5 ledger.

## Deferred / John packet (carried out of Phase 2b)

- h1 file/fh `-f`/`-r` pre-check divergence (h1 fails at open; h2 pre-checks).
- `PAGI::Server::Protocol::HTTP1::serialize_trailers` — public, POD'd, zero lib/ callers.
- Request-trailer receive event (spec follow-up; server validates-and-discards until versioned).
- The handoff's full spec-clarification list (recorded in design §8.3 by Task 6).
