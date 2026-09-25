# HTTP send-state ownership experiment — rejected

## Decision

Keep the runtime checkpoint `874b120` (documentation base `6dbcf47`) on
`experiment/http-simplification`. The candidate removes the default HTTP state
publisher but adds a scalar-alias lifetime rule. Its small, mixed native results
do not justify that tradeoff. This is not a correctness rejection and does not
invalidate the previously retained improvements. Do not layer another flag,
cache or special path onto this experiment.

## What was tried

Ordinary HTTP currently owns a lexical validator state and publishes it to the
connection so receives and app-return handling can observe it. The candidate
instead gives the send closure a reference to the connection's existing state
scalar. It updates that scalar directly, removing the default publisher closure
and its calls. It adds no public API, option, event-loop dependency or cache.

Refusal delegates still need private HTTP state and the existing callback that
translates it into SSE/WebSocket state labels. The candidate leaves that
translation in place. Invalid-event validation, file/fh/trailer rollback and
terminal notification ordering remain unchanged.

There is a real extra invariant: when a keep-alive request finishes, the old
scalar must be detached before resetting the connection's state for the next
request. Otherwise an old saved send becomes writable again and can corrupt the
next response. The candidate uses `delete` before resetting that slot. It works,
but it is less obvious than the original closure-local scalar. One fewer state
copy did not translate into an unequivocally simpler overall implementation.

## Native AWS comparison

Same standalone c7a.xlarge and installed CPAN release as the prior audit, Perl
5.42.2, IO::Async::Loop::EV 0.05, pure-Perl Future, epoll. One worker on CPU 0,
25 HTTP clients on CPUs 2–3, driver/telemetry on CPU 1. WebSocket uses 20 persistent
connections. No profiler, additional server options or dependency upgrades.

Three ten-second samples per HTTP variant; two per SSE/WebSocket variant.
HTTP orders are release/before/candidate, candidate/before/release,
before/release/candidate; controls use the first two orders. The unchanged harness
adds its one-second HTTP warmup and checks the response before each run, including
split-body POST. Means below are from this experiment only; earlier measurements
are not pooled into them.

| Workload | CPAN release 0.002013 | Saved checkpoint | Candidate | Candidate vs saved | Candidate vs release |
| --- | ---: | ---: | ---: | ---: | ---: |
| GET, req/s | 8,530 | 7,407 | 7,500 | +1.25% | -12.08% |
| Ten-header GET, req/s | 7,560 | 6,788 | 6,787 | -0.02% | -10.23% |
| POST observer, req/s | 7,501 | 6,613 | 6,619 | +0.09% | -11.76% |
| Chunked stream observer, req/s | 502.70 | 499.59 | 493.92 | -1.13% | -1.75% |
| SSE, req/s | 215.15 | 213.71 | 213.81 | +0.05% | -0.62% |
| WebSocket, messages/s | 12,661 | 12,178 | 12,182 | +0.04% | -3.78% |

Paired candidate-versus-saved changes by round:

- GET: +2.25%, +1.68%, -0.17%.
- Headers: -0.26%, +0.65%, -0.44%.
- POST: +0.59%, +1.25%, -1.56%.
- Streaming: -2.20%, -0.70%, -0.51%.
- SSE: +0.59%, -0.49%.
- WebSocket: -2.77%, +2.96%.

Thus even GET's positive mean is not consistent in direction across all rounds.
Streaming is slightly lower in all three rounds. These short samples do not
establish precise universal effects. They are enough to decline an extra lifetime
rule for this small mixed result. WebSocket may be client-limited and is only a
control here, not evidence of server capacity.

All 18 smoke runs and 48 timed runs passed. Timed runs returned 2,017,434 HTTP
responses and checked 740,492 WebSocket echoes. Source and loaded dependency
hashes remained unchanged. All 558 interval vmstat samples report zero swap-in,
swap-out, I/O wait and steal at their reporting resolution. Individual samples,
client logs, process resources and telemetry are preserved; none were discarded.

## Correctness checks

The new two-subtest ownership regression passes against both the saved code and
the candidate. It exercises a saved terminal send across two real keep-alive
requests and proves a retained send does not keep its connection alive. The
candidate plus existing header-boundary and reentrant-cancellation checks pass
(3 files / 19 top-level tests). The ownership test also passes on Linux.

A negative control removes only the scalar detachment. The new test then fails:
the old send accepts another start, and the second request produces 201 instead
of its expected intact 200 response. This confirms the lifetime test catches the
specific mistake introduced by naive shared state.

The candidate passed the complete local suite: **180 files / 1,272 tests**,
including HTTP/2 (1,048 seconds). Opt-in release/stress tests and gated cross-distribution
integration tests were skipped as configured; see the full log. This result
applies to the tested candidate; the final runtime is restored to the saved
checkpoint. The ownership regression, header-boundary and reentrant-cancellation checks
then passed against the restored runtime (3 files / 19 top-level tests).

Independent read-only review found no blocking regression and judged the
ownership modestly simpler, with the scalar-detachment maintenance tradeoff.
It also identified a pre-existing late-SSE-close issue; the original code already
corrupts a later scope's output in that case. It was not expanded into this task.
See `review.md` for the precise scope and limitations of that assessment.

## Reproduction, restoration and host lifecycle

The work map names the only repository, branch, base and deployment boundary.
Only the experiment's runtime/test edits were restored; no other work was reset.
The rejected `candidate.patch` includes the implementation and regression test,
and can be applied to base `6dbcf47` for further research. Neither is part of the
active runtime/test tree after rejection.

`state-ownership-data/` preserves the candidate patch, scripts, source hashes,
raw benchmark output, tests and review. `summarize.py` recreates `summary.json`
from `timed/results.jsonl`. Before/candidate full source copies remain on the
stopped benchmark instance; the recorded patch and commit identify them locally.

The machine's public source IP changed; only the benchmark security group's
existing TCP-22 /32 rule was changed to the new /32. One initial driver setup
attempt failed while JSON-encoding a version object, before launching any
benchmarks; stringifying that version fixed the driver. Its log is retained.
No server code was changed to accommodate the driver.

AWS confirmed instance `i-077635273be935c19` stopped after results were downloaded.
Nothing was pushed or deployed outside the isolated benchmark host.
