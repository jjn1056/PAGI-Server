# HTTP simplification experiment — 2026-09-23

This compares unchanged main with `experiment/http-simplification`. Both use
exactly the same benchmark apps and Perl environment. This is a fresh paired
comparison, not a comparison against yesterday's absolute throughput numbers.

The experiment removes scalar-reference indirection in ConnectionState, constructs
the HTTP receive coroutine once per request, and gives the HTTP/1 send coroutine
one successful completion point instead of a per-event on_done wrapper. It adds
no feature switches, observer bypass, or new dependencies. Public terminal
notification deferral, Future tracking and state-machine validation remain.
The three runtime changes were benchmarked together; these results do not isolate
the effect of each individual change.

## Assessment

The simpler code passes the full suite, and the 64-send workload improved in
both screening configurations and the longer confirmation (+6.2%, p99
665.7 → 585.8 ms). Both longer candidate stream samples exceeded both baseline
samples. This is the clearest positive signal in this experiment.

The broader throughput result is mixed: longer GET −3.4%, POST +2.1%, POST with
an observer +1.1%, and one-send 64 KiB −1.7%. GET reversed direction between the
short and longer runs; POST with an observer did too. The one-send case was
slightly slower in all three comparisons. Two samples per implementation and
same-host load are insufficient to call these small differences established
improvements or regressions. Short SSE and WebSocket probes also showed no
compelling improvement (16 workers: −2.8% and +1.2%).

This supports reviewing the changes as a small simplification with a promising
repeated-send benefit. It does **not** establish recovery of the roughly 20%
release-to-main regression, and it is not a recommendation to merge on
performance grounds alone. No further optimization was added during the run.

## Conditions

Baseline: root main at 4625b00, with existing release version declarations.
Candidate: the test branch and commits listed below. Version banners differ
because release-preparation metadata was deliberately left on root main;
source revisions and saved library hashes identify the implementations.

Perl 5.42.2@default, EV, production, LIBEV_FLAGS=8, PERL_FUTURE_NO_XS=1.
One worker/50 HTTP clients, then 16 workers/500 HTTP clients. WebSocket uses
20 persistent connections for both. Same-host clients and servers; no other
benchmark or test suite intentionally runs concurrently. Each workload uses
baseline → candidate → candidate → baseline, ten seconds after a one-second
warmup. HTTP payload/SSE order checked before load; every WS echo checked.

## Results


## 1 worker(s)

| Case | Samples baseline/candidate | Baseline rate | Candidate rate | Change | Baseline p50/p99 ms | Candidate p50/p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| get | 2/2 | 2,742.6 | 2,871.4 | +4.7% | 17.60/28.15 | 16.90/26.05 |
| get-observe | 2/2 | 2,891.4 | 2,931.6 | +1.4% | 17.00/21.55 | 16.80/21.30 |
| post | 2/2 | 2,654.4 | 2,770.3 | +4.4% | 18.55/23.80 | 17.75/22.30 |
| post-observe | 2/2 | 2,669.9 | 2,629.5 | -1.5% | 18.45/22.50 | 18.05/41.15 |
| single | 2/2 | 1,590.9 | 1,542.5 | -3.0% | 31.30/38.65 | 31.25/63.60 |
| single-observe | 2/2 | 1,421.9 | 1,504.2 | +5.8% | 32.55/80.70 | 31.90/61.35 |
| stream | 2/2 | 213.4 | 227.5 | +6.6% | 228.50/464.30 | 213.05/404.15 |
| stream-observe | 2/2 | 212.2 | 218.5 | +2.9% | 228.65/474.50 | 220.15/433.85 |
| sse | 2/2 | 103.0 | 102.5 | -0.5% | 454.15/3157.15 | 454.65/3198.80 |
| websocket | 2/2 | 4,391.2 | 4,483.6 | +2.1% | 4.39/7.99 | 4.33/7.07 |

Rates: HTTP requests/sec, SSE completed streams/sec, WebSocket echoes/sec.
Each latency cell averages per-run percentiles; it is not a pooled percentile.

## 16 worker(s)

| Case | Samples baseline/candidate | Baseline rate | Candidate rate | Change | Baseline p50/p99 ms | Candidate p50/p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| get | 2/2 | 13,786.0 | 14,156.9 | +2.7% | 34.10/78.20 | 34.25/78.30 |
| get-observe | 2/2 | 13,109.3 | 14,187.6 | +8.2% | 35.85/84.30 | 33.25/82.90 |
| post | 2/2 | 12,071.2 | 12,538.5 | +3.9% | 39.90/89.90 | 38.05/84.40 |
| post-observe | 2/2 | 11,854.5 | 11,248.7 | -5.1% | 40.40/95.20 | 39.30/142.15 |
| single | 2/2 | 4,279.5 | 4,161.4 | -2.8% | 114.65/194.60 | 116.60/230.05 |
| single-observe | 2/2 | 4,382.1 | 4,566.9 | +4.2% | 108.25/502.15 | 107.00/180.60 |
| stream | 2/2 | 1,093.4 | 1,140.1 | +4.3% | 444.70/763.45 | 425.60/752.20 |
| stream-observe | 2/2 | 1,026.0 | 1,150.2 | +12.1% | 449.75/878.40 | 425.45/716.85 |
| sse | 2/2 | 604.3 | 587.6 | -2.8% | 771.85/2388.25 | 800.95/2545.00 |
| websocket | 2/2 | 5,066.8 | 5,125.6 | +1.2% | 3.62/6.26 | 3.58/6.06 |

Rates: HTTP requests/sec, SSE completed streams/sec, WebSocket echoes/sec.
Each latency cell averages per-run percentiles; it is not a pooled percentile.

Rates are HTTP requests/sec, completed 100-event SSE streams/sec, or WebSocket
echoes/sec. Percent changes are candidate relative to baseline. Latency cells
average run percentiles; they are not pooled percentiles. Small differences and
single-process WebSocket client results require caution.

## Individual rate samples

| Workers | Workload | Baseline | Candidate |
| ---: | --- | ---: | ---: |
| 1 | get | 2,564.6, 2,920.6 | 2,892.2, 2,850.7 |
| 1 | get-observe | 2,903.0, 2,879.9 | 2,920.3, 2,942.9 |
| 1 | post | 2,650.7, 2,658.1 | 2,777.4, 2,763.1 |
| 1 | post-observe | 2,615.7, 2,724.2 | 2,581.0, 2,678.0 |
| 1 | single | 1,599.8, 1,582.0 | 1,510.4, 1,574.6 |
| 1 | single-observe | 1,411.6, 1,432.3 | 1,504.4, 1,504.0 |
| 1 | stream | 211.2, 215.6 | 230.0, 225.0 |
| 1 | stream-observe | 208.8, 215.6 | 222.0, 214.9 |
| 1 | sse | 101.2, 104.8 | 99.7, 105.3 |
| 1 | websocket | 4,240.4, 4,542.1 | 4,395.4, 4,571.8 |
| 16 | get | 13,969.0, 13,603.1 | 14,430.3, 13,883.4 |
| 16 | get-observe | 12,677.3, 13,541.3 | 13,585.9, 14,789.4 |
| 16 | post | 11,997.4, 12,145.0 | 12,313.9, 12,763.1 |
| 16 | post-observe | 12,466.9, 11,242.1 | 11,148.7, 11,348.7 |
| 16 | single | 4,381.4, 4,177.5 | 4,664.5, 3,658.3 |
| 16 | single-observe | 4,140.2, 4,624.0 | 4,500.7, 4,633.0 |
| 16 | stream | 1,115.0, 1,071.9 | 1,125.8, 1,154.3 |
| 16 | stream-observe | 1,087.7, 964.3 | 1,176.7, 1,123.7 |
| 16 | sse | 549.8, 658.8 | 585.2, 590.0 |
| 16 | websocket | 4,831.7, 5,301.8 | 5,111.0, 5,140.2 |

## Source changes

```text
20655cd test: report unavailable hey percentiles for small samples
ac30cc8 Complete HTTP scopes at the successful send tail
f77e69a Hoist the HTTP receive body closure per scope
d5fe7ee Simplify ConnectionState scalar storage
c00219c test: compare benchmark baseline and candidate checkouts explicitly
87e5714 test: preserve benchmark baseline and HTTP simplification plan

 lib/PAGI/Server/Connection.pm      | 269 +++++++++++++++++--------------------
 lib/PAGI/Server/ConnectionState.pm |  57 ++++----
 2 files changed, 154 insertions(+), 172 deletions(-)
```

## Longer confirmation

Five HTTP cases were repeated at 16 workers/500 clients for 30 seconds per
sample, in the same baseline → candidate → candidate → baseline order.
Selection includes POST with an observer and the single-send response because
both were slower in the screening run. These samples are a separate comparison;
they are not pooled with the ten-second samples.


## 16 worker(s)

| Case | Samples baseline/candidate | Baseline rate | Candidate rate | Change | Baseline p50/p99 ms | Candidate p50/p99 ms |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| get | 2/2 | 16,623.7 | 16,059.6 | -3.4% | 28.80/63.00 | 29.80/60.75 |
| post | 2/2 | 14,604.7 | 14,917.3 | +2.1% | 32.75/62.35 | 31.45/65.35 |
| post-observe | 2/2 | 14,104.1 | 14,258.5 | +1.1% | 33.90/66.15 | 33.15/67.70 |
| single | 2/2 | 5,266.6 | 5,175.1 | -1.7% | 93.00/142.20 | 96.45/141.65 |
| stream | 2/2 | 1,240.8 | 1,317.2 | +6.2% | 390.20/665.70 | 371.90/585.75 |

Rates: HTTP requests/sec, SSE completed streams/sec, WebSocket echoes/sec.
Each latency cell averages per-run percentiles; it is not a pooled percentile.

| Workload | Baseline individual rates | Candidate individual rates |
| --- | ---: | ---: |
| get | 17,355.0, 15,892.4 | 16,664.2, 15,455.1 |
| post | 14,803.1, 14,406.4 | 14,766.2, 15,068.3 |
| post-observe | 14,386.9, 13,821.2 | 14,183.7, 14,333.4 |
| single | 5,276.4, 5,256.8 | 5,327.2, 5,023.1 |
| stream | 1,238.0, 1,243.6 | 1,344.9, 1,289.6 |

## Correctness and review

- Unchanged baseline: 176 test files, 1,249 tests passed.
- Candidate: 177 test files, 1,263 tests passed (305 seconds), using
  `perlbrew exec --with perl-5.42.2@default prove -lr -j4 t`.
- Focused checks covered ConnectionState across protocols, pending receives,
  body limits, unread bodies/keepalive, HEAD, file/fh responses, trailers,
  refusal responses, backpressure, aborts and deferred terminal callbacks.
- Final harness checks: four passed; all five benchmark app checks passed.
- Recorded baseline/candidate library, runner and app hashes were rechecked after
  measurement and still matched.
- Independent runtime and benchmark-harness reviews found no substantive
  correctness or scope issues. This is evidence for evaluation, not approval
  to merge the experiment.
- A small-sample harness issue was corrected: hey sometimes omits p99 for
  fewer than 100 responses. Such percentiles are now recorded as unavailable,
  while missing throughput/mean or request errors still reject the sample.
  The fix was reviewed and exercised with a live SSE smoke run.

There is one observable, spec-permitted ordering difference. The successful send
tail can resume a pending receiver before the send Future becomes ready; the old
on_done wrapper resumed it after readiness. A receiver that explicitly cancels
that outstanding send can therefore leave it cancelled rather than already done.
The new 14-assertion regression passes against both implementations: terminal
facts are settled before receive resumes, the response was written, and terminal
callbacks remain deferred and fire once. PAGI::Spec::Www explicitly allows receive
resumption inside send before its Future resolves. No new state or compensating
hook was added to reproduce the previous incidental order.

## Reproduce

From this experiment worktree, using a fresh output directory each time:

```sh
perlbrew exec --with perl-5.42.2@default python3 examples/14-benchmarks/run.py \
  --baseline-repo /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server \
  --output /tmp/pagi-repeat-w1 --seconds 10 --workers 1 --concurrency 50

perlbrew exec --with perl-5.42.2@default python3 examples/14-benchmarks/run.py \
  --baseline-repo /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server \
  --output /tmp/pagi-repeat-w16 --seconds 10 --workers 16 --concurrency 500

perlbrew exec --with perl-5.42.2@default python3 examples/14-benchmarks/run.py \
  --baseline-repo /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server \
  --output /tmp/pagi-repeat-confirm --seconds 30 --workers 16 --concurrency 500 \
  --cases get post post-observe single stream
```

All 100 measured samples and source/environment metadata are preserved in
[simplification-data/](simplification-data/). Complete raw client reports,
warmup output and server logs are retained locally in the ignored
`local/benchmarks/2026-09-23/` directory. The original release/main results remain
in [BASELINE-2026-09-22.md](BASELINE-2026-09-22.md); different measurement sessions
must not be used to calculate how much of the original release regression was
recovered.

The experiment remains on `experiment/http-simplification`. No merge, push, tag
or release was performed. Root main's release preparation remains untouched.
