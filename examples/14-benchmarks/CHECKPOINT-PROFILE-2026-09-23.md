# Saved performance checkpoint: fresh NYTProf comparison

The current simplifications and accumulated benchmark evidence are saved in
`b995443` on `experiment/http-simplification`. No further runtime changes were
made for this investigation. Root main's release preparation is untouched.

Fresh profiles support the earlier diagnosis: the remaining release/current
cost is distributed across request lifecycle work. They do not reveal another
starvation bug or an increase in Future allocations. They also identify a large,
pre-existing streaming cost worth investigating separately: repeatedly scanning
the outgoing write queue to measure its size.

## Conditions and validation

- Installed Server 0.002013 versus the exact b995443 checkpoint.
- Installed IO::Async::Loop::EV 0.05 on both, EV 4.37; no adapter override.
- Perl 5.42.2@default, pure-Perl Futures, one production worker, 25 clients.
- `LIBEV_FLAGS=8 PERL_FUTURE_NO_XS=1`; `PAGI_FUTURE_XS` unset.
- Existing GET and 64-chunk response apps. Per workload, order was release,
  checkpoint, checkpoint, release, using fresh server processes.
- GET: 2,000 measured requests, 200 warmup and 3 preflight requests per run.
  Stream: 500 measured, 200 warmup and 3 preflight requests per run.
- Eight successful profiles, correct preflight content and successful response
  counts; loaded Server source hashes verified unchanged after each run.
- NYTProf subroutine profiling: `stmts=0:calls=0:slowops=0:compress=1:start=init`.
  POST was not retried because of the previously observed profiler/async crash.

These are elapsed-wall-time profiles, including startup and warmup, not CPU
profiles. Subroutine instrumentation changes relative costs, especially where
there are many short calls. The request-count-normalized timings below locate
work; they do not measure its unprofiled cost or allocate every percentage point
of the remaining regression. Host activity was not controlled.

The checkpoint's runtime matches the earlier full-suite source: 178 files,
1,267 tests passed. Its ownership test was rerun before commit and passed.
Runtime/test whitespace checks passed. Historical raw `hey` output is preserved
verbatim, including the tool's trailing whitespace.

## What remains, and what the earlier changes already removed

Counts below reproduced in both samples of each variant.

| Operation | GET release | GET checkpoint | Stream release | Stream checkpoint |
| --- | ---: | ---: | ---: | ---: |
| Total requests, including preflight/warmup | 2,203 | 2,203 | 703 | 703 |
| `Future::PP::new`, total | 8,878 | 8,878 | 47,167 | 47,167 |
| `Future::PP::on_done`, total | 53 | 53 | 53 | 53 |
| `_mark_complete`, calls/request | 1 | 2 | 1 | 2 |
| `loop->later` terminal scheduling, calls/request | 0 | 1 | 0 | 1 |
| HTTP state publication callback, calls/request | 0 | 3 | 0 | 66 |
| `scope_send_clean`, calls/request | 0 | 2 | 0 | 65 |
| `_get_write_buffer_size`, calls/request | 2 | 2 | 129 | 129 |

The 53 common `on_done` registrations belong to setup/connection handling.
The original main had additional registrations per send; the earlier send-tail
simplification has removed those. The second `_mark_complete` is a guarded
no-op on the normal path, not duplicate callback delivery. Deferred terminal
callbacks and terminal facts before waking receivers implement required behavior;
removing them would change semantics.

The current receive factory still constructs the per-scope wrapper and lifecycle
callbacks. The shared body coroutine is absent from these GET/stream profiles
because these apps do not call receive. Consequently this investigation cannot
establish the coroutine's body-reading cost; the native POST comparison remains
the relevant evidence for that workload.

## Timing signals

Mean exclusive elapsed microseconds per request, averaged over two profiles:

| Routine | GET release | GET checkpoint | Stream release | Stream checkpoint |
| --- | ---: | ---: | ---: | ---: |
| `_create_receive` | 2.94 | 5.66 | 4.95 | 9.17 |
| `_create_send` | 4.76 | 7.20 | 8.47 | 11.34 |
| `_h1_abort_hook` factory | 0 | 2.59 | 0 | 4.18 |
| `_mark_complete` | 2.02 | 4.46 | 4.04 | 6.60 |
| `_deliver_terminal` | 0 | 3.62 | 0 | 6.86 |
| `IO::Async::Loop::later` | 0 | 1.95 | 0 | 3.39 |
| Adapter `watch_idle` method | 0 | 6.24 | 0 | 10.77 |
| HTTP state publication callback | 0 | 2.79 | 0 | 57.48 |
| `scope_send_clean` | 0 | 2.91 | 0 | 83.50 |
| `_get_write_buffer_size` | 4.15 | 4.37 | 2,986.08 | 3,285.44 |

`watch_idle` is still the adapter method name; 0.05 implements it with the fixed
zero-delay timer. Its presence here does not indicate the old idle watcher bug.
The publication callback times exclude surrounding send logic, and the buffer
measurement times exclude the writer accessor it calls. Do not sum inclusive
and exclusive times together.

These numbers do not justify removing small helpers solely to avoid a call.
There is no single demonstrated replacement that recovers all of the remaining
native gap. In particular, all variants' common stream routines show timing
variation too; a larger timing number with unchanged call counts is not proof
of a newly expensive implementation.

## Source review: two distinct opportunities

The new per-send work is concrete. `_create_send` maintains closure-local `$seq`,
publishes it to the owning scope after each validation advance, and asks the
validator whether the successful send ends output. The publication hook also
lets WebSocket/SSE refusals use ordinary HTTP wire handling. Replacing it blindly
with a connection field assignment would break those owners. Failed file sends
also restore the sequence before propagating failure. Any consolidation must
preserve those paths, trailers, HEAD, and completion before an app returns.

The largest general streaming hotspot is older. `_get_write_buffer_size` walks
`IO::Async::Stream`'s write queue and asks each writer for its data. A body send
measures before writing for producer backpressure, then transport watermark
observation measures after writing; drain handling adds another measurement.
In this workload the result is 129 measurements per 64-chunk response in both
versions. As an unflushed queue grows, repeatedly traversing it can produce
quadratic work in the number of queued chunks.

Across each 703-request stream profile, `IO::Async::Stream::Writer::data` is
called 3,037,663 times, identically in both variants. That total includes the
stream library's own calls as well as Server's queue scans; it is not a count
exclusively attributable to `_get_write_buffer_size`.

The before-write and after-write measurements describe different queue contents.
Reusing the former as the latter is incorrect. Introducing an approximate byte
counter without accounting for partial flushes, files, callbacks and teardown
would also be a poor fix. The current code already reaches into stream internals;
a replacement should reduce that dependence, not add another fragile assumption.

## Recommended next investigation

The original regression now looks like accumulated lifecycle overhead, with a
smaller per-chunk component. The best substantial *general* performance lead is
write-buffer measurement, even though it predates this regression. Review whether
accurate buffer accounting can avoid repeated full scans through an existing
supported stream facility or a simpler integration. If it requires extensive new
state or reliance on more internals, stop and discuss before implementing it.

For work specifically aimed at closing the release/current gap, keep scope
creation and send-state ownership as the smaller alternative. Neither route
should disable validators, observers, receive behavior, or correct completion.
Do not change multiple areas before measuring one bounded candidate.

The latest unprofiled seven-workload comparison remains in
[RELEASE-VS-CURRENT-2026-09-23.md](RELEASE-VS-CURRENT-2026-09-23.md): roughly 3–10%
lower HTTP/SSE mean rates, with WebSocket approximately even. These new profiles
are not a replacement benchmark and do not establish a new throughput result.

## Evidence

[checkpoint-profile-data](checkpoint-profile-data/) contains the driver, work
map, checkpoint/dependency identities, source hashes, raw client output, process
statistics, extracted subroutine profiles, and a checked summary. The driver
uses the existing `profiling-data/profile.py` harness. Reproduction requires a
writable copy with the eight output directories moved aside and the candidate
checkout still at b995443; it verifies that commit before running.

```sh
perlbrew exec --with perl-5.42.2@default python3 -B profile-checkpoint.py
python3 -B summarize.py
```

Large raw NYTProf binary files and server logs remain at
`/tmp/pagi-checkpoint-profile-20260923`; the extracted JSON is committed so the
reported counts and timings do not depend on that temporary directory surviving.
