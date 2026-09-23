# Shared HTTP receive routine — 2026-09-23

This extends `experiment/http-simplification` at 659ce38 on the same branch.
The baseline already contains the scalar-storage, receive-closure hoisting and
successful-send-tail changes. This experiment measures only the next step:
replacing the per-request async receive-body closure with a named routine.

## Change

`_create_receive` still returns the application-facing receive closure, with
the existing disconnect cap, clean-end predicate and Future tracking. That
wrapper now calls `_receive_http_body($request, $disconnect, $scope_ended)`.
The body routine reads framing values from the existing request record rather
than capturing them in another closure for every request.

The connection is shifted out of the routine's arguments and held weakly before
an await. The receive logic itself is unchanged, apart from moving and dedenting
it. There are no new state flags, options, Futures or loop-specific paths.
Ignoring indentation, the runtime diff is 30 insertions and 26 deletions.

This removes one closure instance per request. It does not remove all per-scope
callbacks, and it moves some argument/framing setup to each receive invocation.
That tradeoff matters for applications that read bodies repeatedly.

## Validation

- New ownership tests cover suspension both before body data and after the body
  completes; retained receive closures must not keep the connection alive.
- They also cover cancellation propagation and repeated body reads across a
  data wait, including the final body boundary.
- The tests passed on the baseline. A deliberate strong connection reference in
  a temporary copy of the old receive coroutine failed both ownership cases,
  demonstrating that the checks detect the retention regression of concern.
- Focused body/lifecycle suite: **10 files, 154 tests, PASS**.
- Full suite using pure-Perl Futures: **178 files, 1,267 tests, PASS**.
- `git diff --check`: clean.

The full command was:

```sh
PERL_FUTURE_NO_XS=1 perlbrew exec --with perl-5.42.2@default prove -lr t
```

The sequential full run took 1,050 seconds. The earlier full-suite record used
`-j4`; this accounts for much of the longer validation time here. Normal suite
skips, including release-only timing tests, remain skips.

## Benchmark conditions

All tests finished before load started. The same benchmark apps, exact-body
preflights and source-hash checks were used for both versions. The baseline is
an unchanged copy of the experiment's `lib` and `bin` at 659ce38, kept under
`/tmp/pagi-shared-receive-20260923/before`; it is not another development branch.

- Perl 5.42.2@default, installed IO::Async::Loop::EV 0.05, EV loop.
- `LIBEV_FLAGS=8 PERL_FUTURE_NO_XS=1`, production, one worker, 25 clients.
- Fresh server processes, 200 warmup requests per run.
- GET and 1 KiB POST with a connection observer: 30,000 measured requests/run.
- 64-chunk streamed response: 5,000 measured requests/run.
- Initial order per workload: before, after, after, before.
- Streaming confirmation: after, before, before, after; same settings.

These isolate the new change from the earlier performance work. They do not
retest the original 16-worker/500-client setup or establish the remaining gap
against the CPAN release. Absolute rates should not be compared directly with
earlier sessions on this shared host.

## Results

Rates are arithmetic means of two runs, requests/second. All measured responses
were successful, with expected counts and preflight content.

| Workload | Before | After | Change |
| --- | ---: | ---: | ---: |
| GET | 3,228.9 | 3,391.5 | +5.0% |
| Small POST with observer | 2,764.6 | 2,772.9 | +0.3% |
| Stream, initial comparison | 232.6 | 253.6 | +9.0% |
| Stream, reverse-order confirmation | 238.7 | 229.7 | -3.8% |

Individual samples, in time order within each variant:

| Workload | Before samples | After samples |
| --- | --- | --- |
| GET | 3,205.2; 3,252.5 | 3,411.2; 3,371.8 |
| POST | 2,860.9; 2,668.3 | 2,848.4; 2,697.4 |
| Stream, initial | 241.8; 223.4 | 253.2; 254.0 |
| Stream, confirmation | 237.0; 240.4 | 238.8; 220.6 |

GET is the clearest positive signal: both updated samples exceeded both
baseline samples. Mean process CPU time also fell from 9.625 to 9.170 seconds
per 30,000 measured requests, about 4.7%. Process totals include startup,
preflight, warmup and shutdown, not just request handling.

POST is effectively unchanged at this resolution. Streaming is inconclusive:
the unexpectedly large initial improvement did not reproduce in reverse order,
and both comparisons contained a noticeably slower late sample. No samples
were excluded. These measurements do not establish a streaming gain or isolate
the cause of its variation. Peak RSS does not show an improvement; its exact
per-run values are in the saved results.

## Assessment

The shared routine is a small, tested structural simplification with a useful
GET result. It has not demonstrated a broad receive-path throughput gain and
does not resolve the original release-to-main regression. In particular, the
body-reading POST workload did not materially improve.

Keep it available for review on the existing performance branch. Do not add
further optimizations on the strength of the noisy streaming numbers. The prior
send-tail simplification remains part of both variants; its earlier benefit
must not be attributed to this change.

## Evidence and reproduction

[shared-receive-data](shared-receive-data/) contains the work map, driver,
summarizer, adapter identity/hash, raw benchmark output and metadata, all
measurements, test logs, and the incremental runtime patch.

The harness internally labels the baseline `main` and the candidate
`experiment`; the driver explicitly redirects `main` to the pre-change snapshot.
The result summary supplies the unambiguous `before` and `after` labels.

To repeat from a writable copy of that directory, first create its `before`
directory from the experiment's 659ce38 `lib` and `bin`. The candidate path in
`compare.py` must point at the checkout with the recorded incremental patch.
Move existing `runs` and `confirm` directories aside rather than overwriting
evidence, then run:

```sh
perlbrew exec --with perl-5.42.2@default python3 -B compare.py
perlbrew exec --with perl-5.42.2@default python3 -B compare.py --confirm-stream
python3 -B summarize.py
```

`summarize.py` verifies the loaded pure-Perl Future implementation, response
counts, and unchanged source hashes in addition to calculating the means.
