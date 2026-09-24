# Byte-counter comparison after the Safari restart

The quieter rerun shows a modest workload tradeoff. Adapting PR13's byte counter
alone improves the burst-streaming workloads by about 5–6%, but reduces single-write
64 KiB throughput by 3.4% and WebSocket echo throughput by 3.8%. The existing
saved runtime remains unchanged. My recommendation is to leave this counter
out for now rather than accept it as a general performance improvement.

This updates the performance assessment in
[PR13-COUNTER-REVIEW-2026-09-23.md](PR13-COUNTER-REVIEW-2026-09-23.md).
It does not remove the counter-lifecycle issues recorded there.

## Load and run selection

The user stepped away and authorized load checks and a repeat. Initial checks
still showed heavy swapping; Safari web content, Calendar and WindowServer
were active. No processes were stopped by the agent. The user then restarted
Safari while the initial streaming check was in progress.

That four-run check is retained as transition evidence, not mixed into the final
comparison. Its first baseline was 151 requests/sec and its last was 262: that
alone shows why comparing across the transition would be misleading. The decision
to start a fresh full set was made before that set ran, after load checks improved.

The final set's 28 pre-run samples showed 75.14–95.10% aggregate CPU idle and
zero swap-outs in each sampled interval. Background CPU activity remained;
these snapshots do not prove isolation throughout the runs. Conditions were
substantially better than the earlier heavily swapping session.

## Sources and method

- Baseline: existing `experiment/http-simplification` at 0925ba5; runtime b995443.
- Candidate: unchanged temporary adaptation recorded in the PR13 counter review,
  SHA256 `c77228808ef12be47e8cf31fea117675df17b2fb8a684683a3adce08183f93ce`
  for Connection.pm. Only that module differs from the baseline library.
- Original counter source: b8dd6ee. This is not a test of all of PR13, nor a new
  comparison against the CPAN release.
- Same Perl 5.42.2@default, installed Loop::EV 0.05, pure-Perl Future implementation;
  `LIBEV_FLAGS=8 PERL_FUTURE_NO_XS=1`, no NYTProf or PAGI_FUTURE_XS override.
- One production worker, 25 HTTP clients or 20 persistent WebSocket clients.
- Per workload: baseline, candidate, candidate, baseline, fresh processes,
  one-second warmup, twenty-second measured interval.
- Source identities matched the previous diagnostic experiment before starting;
  all library hashes were checked after each run and stayed unchanged.
- Existing exact-body preflights and WebSocket echo/Close checks passed. All
  697,805 measured HTTP responses and 420,388 measured WS echoes succeeded.

CPU time and peak RSS come from `/usr/bin/time -l` around the server. HTTP CPU
per request divides total process user+system CPU by measured+warmup+preflight
requests; process totals also include startup and shutdown. These are useful
comparisons, not isolated handler CPU timings. The WebSocket client does not
report its warmup echo count, so CPU per echo is deliberately not calculated.

## Results

Rates below are arithmetic means of two samples: requests/sec, except WebSocket
echoes/sec. Lower CPU/request is better.

| Workload | Saved runtime | Counter | Change | CPU/request change |
| --- | ---: | ---: | ---: | ---: |
| Small GET | 3,285.7 | 3,222.0 | -1.9% | +1.8% |
| 1 KiB POST with observer | 2,978.8 | 3,075.0 | +3.2% | -3.3% |
| 64 KiB single write | 1,810.8 | 1,748.5 | -3.4% | +3.4% |
| 64-chunk response | 263.0 | 275.6 | +4.8% | -4.2% |
| 64 chunks with observer | 255.7 | 268.6 | +5.1% | -4.1% |
| 100-event SSE burst | 123.4 | 131.2 | +6.3% | -5.7% |
| 128-byte WebSocket echo | 5,356.1 | 5,152.0 | -3.8% | Not normalized |

The streamed HTTP, observer-stream and SSE cases have non-overlapping samples
in the favorable direction, supported by lower CPU/request. Single-write HTTP
and WebSocket have non-overlapping samples in the unfavorable direction. GET's
small negative mean remains within the baseline's wider variation. POST's modest
positive result is encouraging but based on only two samples per variant.
No statistical confidence interval or broad platform claim is implied.

| Workload | Saved runtime samples | Counter samples | Mean peak RSS, saved/counter MiB |
| --- | --- | --- | --- |
| Small GET | 3,405.0; 3,166.4 | 3,225.0; 3,219.0 | 37.49 / 37.37 |
| 1 KiB POST with observer | 2,918.8; 3,038.7 | 3,088.2; 3,061.8 | 37.59 / 37.59 |
| 64 KiB single write | 1,813.6; 1,808.0 | 1,756.5; 1,740.4 | 39.15 / 39.19 |
| 64-chunk response | 265.1; 260.9 | 274.6; 276.5 | 45.16 / 54.39 |
| 64 chunks with observer | 251.3; 260.0 | 267.3; 269.8 | 45.58 / 54.44 |
| 100-event SSE burst | 124.4; 122.4 | 131.8; 130.7 | 42.91 / 40.08 |
| 128-byte WebSocket echo | 5,327.9; 5,384.2 | 5,162.3; 5,141.7 | 37.46 / 38.11 |

Streaming peak RSS increases by roughly 9 MiB in both observer configurations.
That is a repeatable peak-process measurement here, not evidence of a leak or
a universal per-connection memory increase. SSE's measured peak is lower with
the counter; other cases are close.

WebSocket mean-of-run p50 echo latency changes from 3.613 to 3.734 ms, p99 from
5.994 to 6.365 ms, and median Close-reply latency from 6.444 to 6.930 ms. These
are averages of each run's percentile values, not pooled percentiles. Both
versions complete the tested handshakes and all echoes correctly.

## Interpretation and decision

The large NYTProf hotspot did not translate into a correspondingly large native
win for this implementation. It removes queue scans but adds a helper/counter
update and per-write callbacks. Those callbacks also prevent adjacent writers
from combining. The earlier controlled stream probe established that mechanism;
this native comparison measures the net effect on the current runtime.

This does not disprove the value of accurate stream-owned byte accounting that
preserves combining. It does show that PR13's existing callback-based counter,
adapted on its own, is not a clear general win. Its old +9% large-response figure
was measured with other scheduling changes already present and does not transfer
to this baseline.

Keep the saved simplifications, retain this evidence and prototype for possible
future work, and leave this counter out of the runtime for now. There is no need
to contact the IO::Async maintainer on the strength of these results. If the
priority becomes burst streaming specifically, the tradeoff can be reconsidered
with the lifecycle issues fixed and current correctness suites rerun.

No production runtime, dependency, branch integration or PR was changed. The
candidate still has known close/reset limitations and has not passed a current
full-suite/TLS/HTTP2 validation gate. Successful benchmark exchanges are not a
substitute for those checks.

## Evidence and reproduction

[counter-quiet-data](counter-quiet-data/) contains the preflight load samples,
Safari restart marker, transition runs, complete final runs, process CPU/RSS,
source hashes, drivers and machine-readable summary. Server logs remain in
`/tmp/pagi-counter-quiet-20260923`; the committed evidence contains raw client
output and process statistics without those logs.

The driver uses `quiet-rerun-data/harness.py`, the same benchmark apps as before,
and the temporary candidate prepared by `pr13-counter-review-data/prepare.py`.
Use fresh output directories; do not overwrite earlier evidence. In a writable
copy with the recorded candidate and source paths available:

```sh
perlbrew exec --with perl-5.42.2@default python3 -B compare.py --output settled-runs
python3 -B summarize.py
```

The first attempted streaming check stopped at source-identity validation because
macOS `/tmp` and `/private/tmp` paths differed. It performed no load run; the
path was resolved canonically before the retained transition and final runs.
