# Protocol Alignment Phase 2: HTTP/2 HTTP Response Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Give HTTP/2 HTTP responses the same PAGI-visible semantics as HTTP/1: HEAD suppression, `file`/`fh` streaming with backpressure, fullflush, and safe completion/cancellation — plus the Phase 1 carry-list items triaged to this phase (h1 trailers-on-non-chunked loud failure, dead-code cleanup, t/38 conversion).

**Architecture:** All work happens inside the existing per-stream send closure `_h2_create_send` (Connection.pm:798) and its siblings, reusing the Phase 1 order (closed-carve-out → validate → HEAD/stub checks → advance → behavior) and the existing per-stream streaming machinery (`$ss->{send_queue}`, `$data_callback`, `_h2_wait_for_stream_drain`, `resume_stream`, `_h2_write_pending`). File streaming feeds `PAGI::Server::AsyncFile->read_file_chunked`'s awaitable callback into the send queue, so backpressure falls out of the existing watermark waits. **HTTP/2 trailers are NOT in this phase** (Phase 2b, gated on a `Net::HTTP2::nghttp2` `submit_trailer` release — design §8.3): the loud stub stays, except HEAD discards trailers per the PAGI HEAD rules.

**Tech Stack:** Perl 5 (perlbrew `perl-5.42.2@default`), Test2::V0, IO::Async, Net::Async::HTTP, Future::AsyncAwait, Net::HTTP2::nghttp2 0.008.

**Spec:** `docs/superpowers/specs/2026-08-21-pagi-server-protocol-alignment-design.md` §8 (minus §8.3's deferred wire work), §13.2, §15.2; PAGI spec repo `main` @ `a7ed9cc` (`Www.pod` "HEAD Requests" and "Response Body" sections). Phase 1 carry-list: design §8.3 note + the Phase-1 summary's deferred items (M3 dead lexicals, M4 h2-sse fullflush, M6 h1 trailers drop, dead `_unrecognized_event_type` fallbacks + t/38).

## Global Constraints

- Worktree: `/Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/pagi-0.4-alignment`, branch `fix/pagi-0.4-alignment`, starting HEAD `185bce0` (the §8.3 phasing commit; verify with `git log --oneline -1`). Never read or copy from the `perf/http-serving-25` checkout.
- Every Perl command under perlbrew: `bash -c 'source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.42.2@default && <command>'`. Never system perl.
- Test2::V0; behavioral tests on real servers/clients, no mocks; TDD per task (RED captured, GREEN captured; tee to /tmp and READ the files in full — never judge truncated output). Long/full-suite runs wrapped in `caffeinate -i` (host sleep kills runs).
- Phase 1 invariants MUST survive: closed-carve-out ordering; validate → (HEAD/stub checks) → advance; croak-before-first-await = failed Future; failed `file`/`fh` sends restore `$seq` (snapshot before advance, restore in the failure path — mirror commit b34788d's h1 pattern and its code comment).
- h2 file bodies must never be loaded as one scalar (design §8.1); chunk size = the existing `FILE_CHUNK_SIZE` constant.
- `fh` stays application-owned: server reads, never closes; app may close only after the send Future resolves.
- New public functions (none planned) would need same-commit POD; changed behavior needs Changes entries in the same task that lands it.
- Commit only each task's listed files; never the plan file (controller bookkeeping). Update this plan's tracking table via the controller in the same step as each task's completion.
- Full suite runs once, at Task 9. Neighbor lists per task.

## File Structure

- `lib/PAGI/Server/Connection.pm` — all six tasks' code: `_h2_create_send` (798–1002), h2 SSE send closure (~1445+), h1 HTTP send closure (`_create_send`, ~2690+), h1 file/fh helpers reused as reference (`_send_file_response` 4373, `_send_fh_response` 4437), `_h2_write_pending` (407).
- `t/http2/27-head.t`, `t/http2/28-file-fh.t`, `t/http2/29-fullflush-cancel.t` — new h2 behavioral suites (client boilerplate lifted from `t/http2/11-streaming.t` / `t/http2/26-mandatory-validation.t`).
- `t/16-chunked-validation.t` or new `t/53-trailers-framing.t` — h1 trailers-on-non-chunked (Task 6).
- `t/38-unrecognized-event-type.t` — converted to behavioral (Task 7).
- `Changes`, `lib/PAGI/Server/Compliance.pod` — Task 8.

---

### Task 1: HTTP/2 HEAD suppression

**Files:**
- Modify: `lib/PAGI/Server/Connection.pm` (`_h2_create_send`, 798–1002)
- Create: `t/http2/27-head.t`

**Interfaces:**
- Consumes: `$stream_state->{pseudo}{':method'}` (stored at `_h2_on_request` stream init); the closure already receives `($self, $stream_id, $stream_state)`.
- Produces: closure-local `my $is_head = (($stream_state->{pseudo}{':method'} // '') eq 'HEAD');` declared beside `$streaming_started`; a HEAD-handling block that sits BETWEEN shape validation and the Phase-1 stubs (so HEAD is exempted from the file/fh and trailers stubs), used by Tasks 2–3 (their file/fh code is unreachable for HEAD).

- [ ] **Step 1: Write the failing test** — create `t/http2/27-head.t` with the standard h2 client boilerplate (lift verbatim from `t/http2/26-mandatory-validation.t`: same `Net::HTTP2::nghttp2` availability skip, same `create_test_server`/client helpers). App paths and assertions:

```perl
# App: every path answers like a GET would; the server must suppress.
#   /plain        -> start(200, [ct, content-length=5]) + body 'hello' more=>0
#   /streaming    -> start(200,[ct]) + body 'he' more=>1 + body 'llo' more=>0
#   /file         -> start(200,[ct]) + body file => $tmpfile   (app also sets
#                    $Head::FILE_OPENED = -e sentinel check: create the tmpfile,
#                    then unlink it BEFORE the send — a HEAD request must still
#                    succeed because the server never opens it)
#   /trailers     -> start(200, trailers=>1, [ct]) + body '' more=>0 +
#                    trailers event; app captures $@ of each send in package vars
#   /ok-get       -> control: same handler, exercised with GET

# HEAD assertions (client sends :method HEAD):
is( head_response('/plain')->{body}, '', 'HEAD /plain: no DATA' );
is( head_response('/plain')->{headers}{'content-length'}, '5',
    'app Content-Length passes through untouched' );
is( head_response('/streaming')->{body}, '', 'HEAD streaming: all chunks discarded' );
is( head_response('/file')->{body}, '', 'HEAD file: no body' );
ok( !$Head::FILE_SEND_ERROR, 'file body event on HEAD resolves (file was never opened/statted)' );
ok( !$Head::TRAILERS_ERROR, 'trailers event on HEAD is accepted and discarded' );
# control: GET still gets the body
is( get_response('/ok-get')->{body}, 'hello', 'GET unaffected' );
```

The `head_response` helper is `get_h2`-style but with `':method' => 'HEAD'` in the request pseudo-headers — adapt the existing client helper; assert on both received headers and (absence of) DATA frames.

- [ ] **Step 2: Run to verify failure** — `prove -l t/http2/27-head.t` (perlbrew, tee `/tmp/phase2-task1-fail.out`, read). Expected: FAIL — today HEAD transmits DATA frames, and the `/file` path dies on the Phase-1 file stub.

- [ ] **Step 3: Implement** — in `_h2_create_send`:
  1. Beside `my $streaming_started = 0;` add `my $is_head = (($stream_state->{pseudo}{':method'} // '') eq 'HEAD');`
  2. Between the shape-validation call and the Phase-1 stubs insert the HEAD block (order is load-bearing — HEAD exempts these events from the stubs):

```perl
        # HEAD: the server suppresses the body (PAGI Www.pod "HEAD Requests").
        # The app responds exactly as for GET; we discard payloads, never open
        # file/fh, and accept-and-discard trailers. Sequence state still
        # advances so the lifecycle (completion, post-complete raises) matches GET.
        if ($is_head && ($type eq 'http.response.body' || $type eq 'http.response.trailers')) {
            $seq = PAGI::Server::EventValidator::advance_http($seq, $event);
            if ($seq eq 'complete' && !$ss->{h2_head_finished}) {
                $ss->{h2_head_finished} = 1;
                $weak_self->{h2_session}->submit_response($stream_id,
                    status  => $status,
                    headers => \@response_headers,
                    body    => '',
                );
                $weak_self->_h2_write_pending;
            }
            return;
        }
```

  `http.response.start` needs no special-casing (it only stores `$status`/`@response_headers`); `http.fullflush` on HEAD falls through to the normal fullflush handling (Task 4) — flushing headers is legal. Note the headers-only response goes out at the TERMINAL event (`$seq eq 'complete'`), so app-supplied `Content-Length` is preserved and interim `more => 1` events are pure no-ops.

- [ ] **Step 4: Run to verify pass, plus neighbors** — `prove -l t/http2/27-head.t t/http2/26-mandatory-validation.t t/http2/11-streaming.t t/http2/05-request-lifecycle.t` (tee `/tmp/phase2-task1-pass.out`, read fully). Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lib/PAGI/Server/Connection.pm t/http2/27-head.t
git commit -m "feat(h2): HEAD responses suppress the body per PAGI Www.pod"
```

---

### Task 2: HTTP/2 `file` body streaming

**Files:**
- Modify: `lib/PAGI/Server/Connection.pm` (`_h2_create_send`)
- Create: `t/http2/28-file-fh.t`

**Interfaces:**
- Consumes: `PAGI::Server::AsyncFile->read_file_chunked($loop, $path, $callback, offset => ..., length => ..., chunk_size => FILE_CHUNK_SIZE)` — awaits an async callback per chunk (the backpressure hook); the existing streaming machinery (`$ss->{send_queue}`, `$ss->{send_queue_bytes}`, `_h2_wait_for_stream_drain`, `resume_stream`, `_h2_write_pending`, `$data_callback`, `$eof_pending`).
- Produces: a private `async sub $emit_chunk` in `_h2_create_send`'s closure scope — signature `$emit_chunk->($chunk)`, awaits per-stream drain above the high watermark, dies with the closure-local `$STREAM_GONE` sentinel when the stream vanished — consumed verbatim by Task 3's `fh` arm. The `file` half of the Phase-1 stub is removed here (Task 3 removes the `fh` half). Sequence-state snapshot/restore on failure (b34788d parity).

- [ ] **Step 1: Write the failing tests** — create `t/http2/28-file-fh.t` (same boilerplate source as Task 1). Fixture: write a temp file of 300_000 bytes (> 4 × FILE_CHUNK_SIZE) of a known repeating pattern. App paths (each `eval`s its send and reports errors in-band where the response can still carry them, package vars where not):

```perl
#   /file-full      -> start(200,[ct]) + body file => $big     (full stream)
#   /file-range     -> ... file => $big, offset => 1000, length => 1000
#   /file-past-eof  -> ... file => $big, offset => 999_999_999 (zero bytes, clean end)
#   /file-missing   -> start + eval{ body file => '/nonexistent' }; then a
#                      normal terminal body carrying "err=$@" (recovery per the
#                      restore-on-failure contract)
#   /file-after-chunks -> start + body 'x' more=>1 + body file => $big
#                      (file on an already-streaming response)

is( length get_h2('/file-full')->{body}, 300_000, 'full file streamed' );
is( get_h2('/file-full')->{body}, $pattern, 'byte-exact content' );
is( get_h2('/file-range')->{body}, substr($pattern,1000,1000), 'offset+length honored' );
is( get_h2('/file-past-eof')->{body}, '', 'offset past EOF sends zero bytes, stream ends cleanly' );
like( get_h2('/file-missing')->{body}, qr/err=.+/, 'open failure fails the Future; app recovered with a normal body' );
is( get_h2('/file-after-chunks')->{body}, 'x' . $pattern, 'file appended to an in-progress stream' );
```

- [ ] **Step 2: Run to verify failure** — tee `/tmp/phase2-task2-fail.out`, read. Expected: FAIL (file stub dies).

- [ ] **Step 3: Implement** — in `_h2_create_send`, above `return async sub {`:

```perl
    # Shared file/fh chunk pump: pushes produced chunks into this stream's
    # send queue under the per-stream watermark, then marks EOF. The producer
    # is an async sub that receives an async "emit" callback and must await it
    # per chunk; emit dies with the sentinel below if the stream vanishes
    # (client reset) so the pump stops reading without treating it as an error.
    my $STREAM_GONE = "PAGI::h2 stream gone\n";
    my $emit_chunk = async sub {
        my ($chunk) = @_;
        my $ss = $weak_self ? $weak_self->{h2_streams}{$stream_id} : undef;
        die $STREAM_GONE unless $ss && !$weak_self->{closed};
        if (($ss->{send_queue_bytes} // 0) >= $weak_self->{write_high_watermark}) {
            await $weak_self->_h2_wait_for_stream_drain($stream_id);
            $ss = $weak_self ? $weak_self->{h2_streams}{$stream_id} : undef;
            die $STREAM_GONE unless $ss && !$weak_self->{closed};
        }
        if (length $chunk) {
            push @{$ss->{send_queue}}, $chunk;
            $ss->{send_queue_bytes} += length $chunk;
        }
        $ss->{transport_state}->_check_watermarks if $ss->{transport_state};
        $weak_self->{h2_session}->resume_stream($stream_id);
        $weak_self->_h2_write_pending;
        return;
    };
```

Then in the body branch, replace the file/fh stub with dispatch to a new path (the `fh` half arrives in Task 3; until then `fh` keeps its stub line):

```perl
            if (defined $event->{file}) {
                my $file   = $event->{file};
                my $offset = $event->{offset} // 0;
                my $length = $event->{length};

                # Fail BEFORE submitting headers or advancing state so the app
                # can recover (a failed file/fh send must NOT mark the
                # response complete — see the h1 twin in _create_send).
                die "File not found: $file\n"  unless -f $file;
                die "Cannot read file: $file\n" unless -r $file;

                my $seq_before = $seq;
                $seq = PAGI::Server::EventValidator::advance_http($seq, $event);

                my $ok = eval {
                    if (!$streaming_started) {
                        $streaming_started = 1;
                        $ss->{send_queue} //= []; $ss->{send_queue_bytes} //= 0;
                        $weak_self->{h2_session}->submit_response_streaming(
                            $stream_id,
                            status => $status, headers => \@response_headers,
                            data_callback => $data_callback,
                        );
                        $weak_self->_h2_write_pending;
                    }
                    my $loop = $weak_self->{server}->loop;
                    await PAGI::Server::AsyncFile->read_file_chunked(
                        $loop, $file, $emit_chunk,
                        offset => $offset,
                        (defined $length ? (length => $length) : ()),
                        chunk_size => FILE_CHUNK_SIZE,
                    );
                    1;
                };
                if (!$ok) {
                    my $err = $@;
                    return if $err eq $STREAM_GONE;   # client reset: quiet no-op
                    $seq = $seq_before;               # recoverable, per contract
                    die $err;
                }
                $eof_pending = 1;
                my $live = $weak_self->{h2_streams}{$stream_id} or return;
                $weak_self->{h2_session}->resume_stream($stream_id);
                $weak_self->_h2_write_pending;
                return;
            }
```

NOTE on placement: this dispatch must run where the removed stub ran — after validation/HEAD, BEFORE the generic `advance_http` line — because it does its own snapshot/advance. Restructure minimally: hoist the generic `$seq = advance_http(...)` line into the non-file/fh paths (start, plain body, fullflush) or guard it with `unless file/fh`. Keep the diff small and commented.

- [ ] **Step 4: Update the t/http2/26 fixture in this task** — removing the `file` stub breaks t/http2/26-mandatory-validation.t's `/file-body` assertion (it expects the stub message). Update that path's fixture NOW so no commit carries a failing test: the app path streams a small known temp file and the assertion becomes a positive content check (`like(... qr/file:/ ...)` replaced by the file's content or a `file-ok:` marker the app emits after the send resolves). The `/trailers` stub assertion stays untouched (trailers remain stubbed until Phase 2b).

- [ ] **Step 5: Run to verify pass, plus neighbors** — `prove -l t/http2/28-file-fh.t t/http2/26-mandatory-validation.t t/http2/27-head.t t/http2/11-streaming.t t/http2/18-transport-leak.t t/http2/19-transport-callbacks.t` (tee `/tmp/phase2-task2-pass.out`, read). Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add lib/PAGI/Server/Connection.pm t/http2/28-file-fh.t t/http2/26-mandatory-validation.t
git commit -m "feat(h2): stream file bodies through the per-stream send queue"
```

---

### Task 3: HTTP/2 `fh` body streaming

**Files:**
- Modify: `lib/PAGI/Server/Connection.pm` (`_h2_create_send`)
- Modify: `t/http2/28-file-fh.t`

**Interfaces:**
- Consumes: `$emit_chunk` from Task 2; h1 `_send_fh_response` (Connection.pm:4437) as the semantic reference — seek to offset (die on failure), `read($fh, ...)` in `FILE_CHUNK_SIZE` chunks (die on undef read), stop at EOF or after `length` bytes, never close the handle.
- Produces: the `fh` arm beside Task 2's `file` arm; the `fh` stub line removed — after this task NO file/fh stub remains in `_h2_create_send`.

- [ ] **Step 1: Write the failing tests** — extend `t/http2/28-file-fh.t`:

```perl
#   /fh-full     -> open my $fh,'<:raw',$big; body fh => $fh; app records
#                   $FhOwn::CLOSED_OK = (close $fh ? 1 : 0) AFTER the send
#                   Future resolves (ownership: server must not have closed it)
#   /fh-range    -> fh with offset => 1000, length => 1000
#   /fh-closed   -> open, CLOSE the handle, then eval { body fh => $closed };
#                   recover with a normal terminal body carrying "err=$@"
#                   (b34788d parity: state restored, recovery legal — the h2
#                   twin of t/42's closed-fh subtest)

is( get_h2('/fh-full')->{body}, $pattern, 'fh streamed byte-exact' );
ok( $FhOwn::CLOSED_OK, 'application still owns the handle after the send resolves' );
is( get_h2('/fh-range')->{body}, substr($pattern,1000,1000), 'fh offset+length honored' );
like( get_h2('/fh-closed')->{body}, qr/err=.+/, 'closed fh fails the Future; app recovered' );
```

- [ ] **Step 2: Run to verify failure** — tee `/tmp/phase2-task3-fail.out`, read. Expected: the three new paths FAIL (fh stub).

- [ ] **Step 3: Implement** — add the `fh` arm mirroring Task 2's structure exactly (snapshot → advance inside eval → restore on failure → `$STREAM_GONE` quiet path), with the read loop adapted from `_send_fh_response`:

```perl
                    if (my $off = $event->{offset}) {
                        seek($fh, $off, 0) or die "Cannot seek: $!\n";
                    }
                    my $remaining = $event->{length};
                    while (1) {
                        my $to_read = FILE_CHUNK_SIZE;
                        if (defined $remaining) {
                            $to_read = $remaining if $remaining < $to_read;
                            last if $to_read <= 0;
                        }
                        my $bytes_read;
                        { no warnings 'closed';
                          $bytes_read = read($fh, my $chunk, $to_read);
                          die "Failed to read filehandle: $!\n" unless defined $bytes_read;
                          last if $bytes_read == 0;
                          await $emit_chunk->($chunk);
                        }
                        $remaining -= $bytes_read if defined $remaining;
                    }
```

The initial-read failure (closed handle) happens before any chunk is emitted, so the restore leaves the response recoverable, matching h1. Delete the last stub line; update the Phase-1 stub comment to cover only trailers/fullflush (fullflush goes in Task 4).

- [ ] **Step 4: Run to verify pass, plus neighbors** — `prove -l t/http2/28-file-fh.t t/http2/27-head.t t/42-file-response.t t/http2/26-mandatory-validation.t` (the `/file-body` fixture was already updated in Task 2; nothing in 26 references the fh stub, but verify). Tee `/tmp/phase2-task3-pass.out`, read. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lib/PAGI/Server/Connection.pm t/http2/28-file-fh.t
git commit -m "feat(h2): stream application-owned filehandle bodies"
```

---

### Task 4: fullflush on h2 HTTP and h2 SSE

**Files:**
- Modify: `lib/PAGI/Server/Connection.pm` (`_h2_create_send`; h2 SSE send closure ~1445+)
- Create: `t/http2/29-fullflush-cancel.t` (fullflush half; Task 5 adds the cancel half)

**Interfaces:**
- Consumes: `_h2_write_pending` (drains `$session->extract` to the stream); `resume_stream`.
- Produces: fullflush stub removed from `_h2_create_send`; new `http.fullflush` branch in BOTH closures: `$weak_self->{h2_session}->resume_stream($stream_id) if <streaming started>; $weak_self->_h2_write_pending;` then resolve. Extension truthfulness (design §8.4/§13.2) becomes satisfied for h2 scopes: advertised ⇒ accepted.

- [ ] **Step 1: Write the failing tests** — `t/http2/29-fullflush-cancel.t`, servers constructed WITH `extensions => { fullflush => {} }` and one WITHOUT:

```perl
#   /flush-http -> start + body 'a' more=>1 + fullflush (eval, record $@) +
#                  body 'b' more=>0; response must be 'ab' and $@ empty
#   /flush-sse  -> (Accept: text/event-stream) sse.start + sse.send data 'x'
#                  + http.fullflush (eval, record) + sse.close
like( get_h2('/flush-http')->{body}, qr/\Aab\z/, 'fullflush mid-stream is transparent' );
ok( !$Flush::HTTP_ERR, 'advertised fullflush resolves on h2 http' );
ok( !$Flush::SSE_ERR, 'advertised fullflush resolves on h2 sse' );
# unadvertised server: gating croak still applies (validator, unchanged)
like( $Flush::UNADV_ERR, qr/Extension not enabled: fullflush/, 'unadvertised still rejected' );
```

- [ ] **Step 2: Run to verify failure** — tee `/tmp/phase2-task4-fail.out`, read. Expected: `/flush-http` dies on the stub; `/flush-sse` dies in the h2 SSE dispatcher's fallback (the known M4 gap).

- [ ] **Step 3: Implement** — remove the fullflush stub; add to `_h2_create_send`'s dispatch (after the body branch):

```perl
        elsif ($type eq 'http.fullflush') {
            # Hand any pending frames to the session's write path (design §8.4).
            $weak_self->{h2_session}->resume_stream($stream_id) if $streaming_started;
            $weak_self->_h2_write_pending;
        }
```

and the equivalent branch in the h2 SSE send closure (its streaming is always active once `sse.start` ran; guard on its started flag). `advance_http`/`advance_sse` already model fullflush legality (Phase 1) — no machine changes.

- [ ] **Step 4: Run to verify pass, plus neighbors** — `prove -l t/http2/29-fullflush-cancel.t t/07-extensions.t t/http2/14-sse-events.t t/http2/26-mandatory-validation.t` (t/http2/26's fullflush-unadvertised coverage must stay green; if it asserted the stub message for the ADVERTISED case, update that fixture with an explanation). Tee `/tmp/phase2-task4-pass.out`, read. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lib/PAGI/Server/Connection.pm t/http2/29-fullflush-cancel.t t/http2/26-mandatory-validation.t
git commit -m "feat(h2): implement http.fullflush on HTTP and SSE streams"
```

---

### Task 5: Cancellation safety during file/fh transfers (§8.5)

**Files:**
- Modify: `lib/PAGI/Server/Connection.pm` (only if Step 2 exposes a defect — this task is primarily a test task)
- Modify: `t/http2/29-fullflush-cancel.t`

**Interfaces:**
- Consumes: Task 2's `$STREAM_GONE` quiet-abort path; `_h2_on_close`'s drain-waiter release (`_h2_resolve_stream_drain_waiters`) and deferred stream reclaim.
- Produces: pinned behavior — client RST mid-file-transfer stops the pump without an application-error log, without a synthesized 500, and without leaking the drain waiter.

- [ ] **Step 1: Write the test** — app path `/slow-file` streams the 300KB file; the client starts the request, reads the first DATA frame, then sends RST_STREAM and closes. Assertions:

```perl
# capture warnings for the whole exchange
my @warnings; local $SIG{__WARN__} = sub { push @warnings, $_[0] };
# ... drive /slow-file, RST after first frame, wait 0.5s of loop turns ...
ok( !grep({ /PAGI application error/ } @warnings), 'no app-error log on client reset' );
# the app records how its send Future ended:
like( $Cancel::RESULT, qr/\A(resolved|quiet)\z/, 'file send ended quietly (no raised error)' );
# and the server still serves a fresh request on a NEW h2 connection:
is( get_h2('/ok')->{body}, 'ok', 'server healthy after mid-transfer reset' );
```

(the app evals the file send and sets `$Cancel::RESULT = $@ ? ($@ eq the-quiet-path ? 'quiet' : "err:$@") : 'resolved'` — with the `$STREAM_GONE` design the eval RESOLVES, so expect 'resolved'; accept 'quiet' to keep the assertion honest about the contract rather than the mechanism).

- [ ] **Step 2: Run** — tee `/tmp/phase2-task5.out`, read fully. If it passes first try, this is the RED-optional exception: the test pins §8.5 behavior Task 2 built; document that in the report. If it FAILS (hang, error log, leaked waiter), fix within Task 2's machinery (the `$emit_chunk` gone-checks and `_h2_on_close`'s waiter release are the two suspects) and re-run.

- [ ] **Step 3: Neighbors** — `prove -l t/http2/29-fullflush-cancel.t t/http2/23-rst-rate-limit.t t/http2/16-sse-cleanup.t` (tee, read). Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add t/http2/29-fullflush-cancel.t lib/PAGI/Server/Connection.pm
git commit -m "test(h2): client reset during file transfer is quiet and leak-free"
```

---

### Task 6: h1 trailers on non-chunked responses fail loudly (carry M6)

**Files:**
- Modify: `lib/PAGI/Server/Connection.pm` (h1 `_create_send` closure, trailers branch ~2890-2905)
- Create: `t/53-trailers-framing.t`

**Interfaces:**
- Consumes: the closure's existing `$chunked` var (set in the start branch) and `$is_head` equivalent (`$request->{method} eq 'HEAD'` check used by the h1 HEAD path).
- Produces: ordering in the h1 closure for `http.response.trailers`: HEAD-discard first (accept + advance + discard, PAGI HEAD rule), then the framing check `die "http.response.trailers requires chunked framing (response declared content-length)\n"` when `!$chunked`, then the existing chunked trailers write. The silent `return unless $chunked;` is REMOVED. The machine never records `complete` for trailers that never went out — implemented per **Deviation D1** as advance-then-rollback (h1 has one hoisted `advance_http` call site; `advance_http` is pure, and `$seq` is restored before the die propagates, with no await in between), not the h2 stub-before-advance shape originally written here.

- [ ] **Step 1: Write the failing tests** — `t/53-trailers-framing.t` (h1 server, Net::Async::HTTP client, t/52 idioms):

```perl
#  /cl-trailers   -> start(200, trailers=>1, [ct, content-length=2]) + body 'ok'
#                    more=>0 + eval{trailers} -> recover impossible (body done);
#                    record $@ in $T::CL_ERR; server closes (incomplete-response
#                    rules do NOT apply: the trailers event failed, the app then
#                    returns; body itself was complete... assert response 'ok'
#                    delivered and $T::CL_ERR matches qr/requires chunked framing/)
#  /chunked-trailers -> start(200, trailers=>1,[ct]) + body 'ok' more=>0 +
#                    trailers [['x-t','1']]; assert trailer received (raw-socket
#                    client reading chunked framing, boilerplate from t/16)
#  /head-trailers -> HEAD request; same app as /chunked-trailers; assert empty
#                    body, no error, connection sane
like( $T::CL_ERR, qr/requires chunked framing/, 'trailers on content-length response fail the Future' );
```

- [ ] **Step 2: Run to verify failure** — tee `/tmp/phase2-task6-fail.out`, read. Expected: `/cl-trailers` today silently no-ops (`$T::CL_ERR` empty) → FAIL; `/chunked-trailers` passes today (control).

- [ ] **Step 3: Implement** per the Interfaces block. Keep the code comment stating the constraint: trailers ride chunked framing only (RFC 7230); a content-length response has no place to put them, and silently dropping promised trailers lies to the application.

- [ ] **Step 4: Run to verify pass, plus neighbors** — `prove -l t/53-trailers-framing.t t/16-chunked-validation.t t/10-http-compliance.t t/52-mandatory-validation.t` (tee `/tmp/phase2-task6-pass.out`, read). Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lib/PAGI/Server/Connection.pm t/53-trailers-framing.t
git commit -m "fix(h1): trailers on a content-length response fail instead of vanishing"
```

---

### Task 7: Dead-code cleanup and t/38 conversion (carry M3 + deferred items 4/10)

**Files:**
- Modify: `lib/PAGI/Server/Connection.pm`, `t/38-unrecognized-event-type.t`

**Interfaces:**
- Consumes: Phase 1's guarantee that `validate_*_send` rejects unknown types before any dispatcher `else` can fire.
- Produces: the six unreachable `else { _unrecognized_event_type(...) }` branches removed (h1 http/sse/ws, h2 http/sse/ws — each replaced by nothing; the dispatch chains end at their last `elsif`), `_unrecognized_event_type` itself removed once unreferenced, the dead write-only lexicals in h1 `_create_send` removed (`$response_started`, `$body_complete` — verify write-only with grep BEFORE deleting; `$expects_trailers` is LIVE, keep it), and t/38 rewritten as a small behavioral test: one h1 server, app sends a misspelled event type, asserts the failed-Future message `qr/Unrecognized event type .* for http protocol/` end-to-end (unit coverage already lives in t/40; cross-family behavioral coverage in t/52 and t/http2/26 — t/38's header comment points there). No assertions are silently dropped: the file keeps `done_testing` and real assertions.

- [ ] **Step 1: RED** — rewrite t/38 first; it passes against current code (behavior already exists), so the RED here is for the REMOVALS: run `prove -l t/38-unrecognized-event-type.t` with the OLD file to capture it failing after the else-removal would break its source-regex counting — i.e., do the removals (Step 2) and confirm OLD t/38 fails (tee `/tmp/phase2-task7-red.out`), then land the rewritten t/38 and confirm green. Sequence the steps so no commit contains a failing test.
- [ ] **Step 2: Implement removals** — with `grep -n '_unrecognized_event_type\|$response_started\|$body_complete' lib/PAGI/Server/Connection.pm` evidence in the report for each deletion (prove write-only/unreachable before deleting; anything that turns out reachable gets LEFT and reported).
- [ ] **Step 3: Verify** — `prove -l t/38-unrecognized-event-type.t t/52-mandatory-validation.t t/40-event-validation.t t/http2/26-mandatory-validation.t t/01-hello-http.t` + `perl -c -Ilib lib/PAGI/Server/Connection.pm` (tee `/tmp/phase2-task7-pass.out`, read). Expected: PASS.
- [ ] **Step 4: Commit**

```bash
git add lib/PAGI/Server/Connection.pm t/38-unrecognized-event-type.t
git commit -m "refactor: remove dispatcher dead code the mandatory validator obsoleted"
```

---

### Task 8: Documentation

**Files:**
- Modify: `Changes`, `lib/PAGI/Server/Compliance.pod`

**Interfaces:** none new.

- [ ] **Step 1: Changes** — under 0.002007's "Specification Alignment" section add: h2 HEAD suppression; h2 file/fh streaming with backpressure and app-owned handles; h2 fullflush (HTTP + SSE); h1 trailers-on-content-length now fail loudly; note that HTTP/2 trailers remain unimplemented pending a Net::HTTP2::nghttp2 `submit_trailer` release (Phase 2b).
- [ ] **Step 2: Compliance.pod** — update the HTTP/2 section: parity now includes HEAD/file/fh/fullflush; a "Known limitations" entry for h2 trailers naming the dependency gate and the loud-failure behavior an app sees meanwhile. Follow the file's existing structure; podchecker must pass.
- [ ] **Step 3: Verify** — `podchecker lib/PAGI/Server/Compliance.pod` + read both files' diffs. 
- [ ] **Step 4: Commit**

```bash
git add Changes lib/PAGI/Server/Compliance.pod
git commit -m "docs: record h2 parity gains and the trailers dependency gate"
```

---

### Task 9: Phase gate

- [ ] **Step 1:** Full suite under caffeinate: `caffeinate -i bash -c '... && prove -lr t/ 2>&1' > /tmp/phase2-full-suite.out` then read the END and grep `^Result:` / `Failed`.
- [ ] **Step 2:** `git diff <phase-2 base> --check`; `podchecker` + `perl -c -Ilib` over Connection.pm, Compliance.pod.
- [ ] **Step 3:** Spec drift check — PAGI repo main still `a7ed9cc` (or record drift per design §2.2).
- [ ] **Step 4:** Tracking table update + commit (`docs: phase 2 verification gate results`).

---

## Tracking

| Task | Status | Commit | Tests (added/passing) | Verification evidence |
|------|--------|--------|-----------------------|-----------------------|
| 1. h2 HEAD | complete (review clean; plan test-omission fixed by implementer) | 4794ceb | t/http2/27 new / 32/32 across 4 files | /tmp/phase2-task1-pass.out |
| 2. h2 file streaming | complete (review clean; t/http2/26 fixture in-commit) | d921f07 | t/http2/28 new / 35 tests, 6 files | /tmp/phase2-task2-pass.out |
| 3. h2 fh streaming | complete (review clean; plan loop bug fixed by implementer) | d844ed8 | +3 paths / 48/48, 4 files | /tmp/phase2-task3-pass.out |
| 4. fullflush h2 http+sse | complete (review clean; M4 closed) | f9e7a68 | t/http2/29 new / 25 tests, 4 files | /tmp/phase2-task4-pass.out |
| 5. Cancel safety | complete (review clean; carried race measured harmless, test-only) | 61da0d0 | +cancel section / 4 files PASS ×5 runs | task-5-report.md |
| 6. h1 trailers framing | complete (review clean; deviation D1 ruled: advance-then-rollback) | 09a06ab | t/53 new / 66/66, 4 files | /tmp/phase2-task6-pass.out |
| 7. Dead code + t/38 | complete (review clean, zero findings; "six" corrected to five) | 856422a | t/38 behavioral / 58/58 + perl -c | /tmp/phase2-task7-pass.out |
| 8. Docs | complete (review clean, zero findings) | 1b5440d | podchecker OK; smoke 9/9 | task-8-report.md |
| 9. Phase gate | complete | (this commit) | full suite 117 files / 664 tests PASS | /tmp/phase2-full-suite.out; whitespace/POD/syntax clean; spec drift: none (a7ed9cc) |

## Deferred (tracked for after the first pass)

- **Phase 2b — h2 trailers**: gated on Net::HTTP2::nghttp2 gaining `submit_trailer` (John's dist; source repo location TBD from John). Scope when unblocked: XS binding + release + cpanfile floor bump + implement design §8.3 (terminal body with `NO_END_STREAM` when `trailers => 1`, trailing HEADERS with END_STREAM), remove the stub, drop the Compliance.pod limitation, restore the h1-identical "not declared" message for undeclared h2 trailers.
- t/52 hygiene (fixed disconnect-probe budget; client not loop-removed) — revisit if CI flakes.
- Theoretical concurrent-send `$seq` clobber when an app fires sends without awaiting (h1 + h2 file restore paths) — no conforming app does this; revisit if evidence appears.

## Deviations

- **D1 (Task 6, commit 09a06ab)** — h1 non-chunked trailers rejection uses advance-then-rollback instead of the plan's "die before advance". Rationale: h1 has a single hoisted `advance_http` call site (Phase 1 architecture); a literal pre-advance die would require restructuring the closure. Equivalence: `advance_http` is a pure function (no side effects beyond its return value — pinned by a code comment), `$seq` is restored before the die propagates, and no await sits between advance and rollback, so the binding requirement — the machine never records completion for undelivered trailers — holds; reviewer-traced in both the task review and the final whole-phase review. Controller-ruled 2026-08-22; **awaiting John's sign-off** (surfaced in the Phase 2 summary).
