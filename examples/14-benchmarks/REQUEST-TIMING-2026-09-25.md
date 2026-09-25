# Conditional request timing — AWS experiment, 2026-09-25

## Finding and disposition

Skipping the request timestamp when access logging is disabled produces no
persuasive performance improvement on this checkpoint. All throughput means
are within 1% of saved code. Small GET, POST, GET500 and WebSocket effects change
direction between rounds; the tiny streaming increase is consistent but only
0.15–0.40% per round. Small GET p99 is unchanged, and GET500 p99 is effectively
unchanged too. Restore the saved runtime; retain this as a neutral experiment,
not another performance mechanism to maintain.

The candidate and its regression test are preserved in
[request-timing-data/candidate.patch](request-timing-data/candidate.patch).
The active runtime remains exactly `040731c`, including configurable 64 KiB
reads and 8 KiB writes. No other optimization was bundled or started here.

## Exact change

```perl
# Saved
$self->{request_start} = [gettimeofday];

# Candidate
$self->{request_start} = $self->{access_log} ? [gettimeofday] : undef;
```

Source inspection found only one consumer of this timestamp: access-log
request duration. The idle/stall/close timers use separate state. This timestamp
is taken when dispatching an HTTP request or SSE/WebSocket handshake, not on
every WebSocket message. A small steady-state echo-rate movement does not by
itself establish a benefit from eliminating a handshake timestamp.

## Results

Arithmetic means of complete ten-second runs. Rates are requests/second except
WebSocket echoes/second. These are fresh comparisons within this experiment;
do not pool them with earlier benchmark sessions.

| Workload | Release 0.002013 | Saved 040731c | Candidate | Change vs saved | Runs/variant |
| --- | ---: | ---: | ---: | ---: | ---: |
| GET, 25 clients | 8,633.23 | 7,924.55 | 7,925.90 | +0.02% | 3 |
| 1 KiB POST + observer | 7,539.08 | 7,053.26 | 7,067.77 | +0.21% | 3 |
| GET, 500 clients | 7,154.60 | 6,620.55 | 6,659.49 | +0.59% | 3 |
| 64-chunk response + observer | 510.93 | 480.87 | 482.16 | +0.27% | 3 |
| 100-event SSE burst | 216.93 | 213.84 | 213.44 | -0.19% | 2 |
| WebSocket echo (messages/s) | 12,688.16 | 12,077.42 | 12,193.05 | +0.96% | 2 |

| Tail metric (mean of run percentiles) | Release | Saved | Candidate |
| --- | ---: | ---: | ---: |
| GET25 p99, ms | 3.00 | 3.30 | 3.30 |
| GET500 p99, ms | 74.37 | 78.70 | 78.77 |

Percentiles are not pooled across runs. The data do not establish an improvement
in response-time tails or close the remaining release gap.

## Method and boundaries

- Dedicated On-Demand c7a.xlarge, 4 cores / 8 GiB, one server worker pinned to
  CPU 0; driver CPU 1; clients CPUs 2–3. No autoscaling or burst credits.
- Perl 5.42.2, IO::Async::Loop::EV 0.05, Linux EV backend 4 and pure-Perl
  Futures. Access logging disabled for all timed variants. Installed release
  stays 0.002013; saved and candidate both report 0.002014.
- Exact source snapshots transferred by SSH with SHA256 manifests; these are
  directories, not additional branches/worktrees. Only Connection.pm differs
  between saved and candidate runtime manifests. Driver verifies source and
  loaded dependency hashes before/through/after the comparisons.
- Saved and candidate both use read65536/write8192. Release retains its native
  read8192/write8192 defaults. The saved/candidate comparison isolates request
  timing; the release column is the overall target, not an isolated timestamp
  comparison.
- Three rotated orders: release/saved/candidate, candidate/release/saved,
  saved/candidate/release. GET25, POST, GET500 and streaming use all three;
  SSE and WebSocket controls use two. HTTP concurrency is 25 except GET500;
  WebSocket uses 20 persistent connections. One-second warmup per run.
- Preflights check actual response content, split-body POST behavior, SSE
  ordering and WebSocket echo/close. Eighteen smoke runs and all 48 timed runs
  completed successfully. No benchmark sample was discarded.
- 559 timed interval vmstat samples show zero swap-in/out and zero reported
  CPU steal. This supports the controlled comparison but does not make the
  sub-percent differences statistically conclusive. No 16-worker or HTTP/2
  performance claim follows from this run.

## Correctness and execution notes

The regression observes the real timestamp function and actual `%D` access-log
output across two requests, with logging enabled and disabled. Saved code
fails because it takes two timestamps with logging off; the candidate passes
and still logs a positive measured duration for both logged requests.

Local candidate checks: **6 files / 46 tests PASS**, including access logging,
log routing, ordinary HTTP, request bodies, WebSocket and SSE. The same access-log
file passes on AWS: **1 file / 14 tests**. This was a bounded experiment;
the full suite was not rerun for the candidate, and the candidate is not retained.
Restored runtime/test bytes are verified against the saved commit.

The first test draft incorrectly assumed access_log_format accepted a callback;
it was corrected to the existing `%D` format before recording the final
RED/GREEN result. The first remote launch stopped before tests or benchmarks
because Perlbrew's startup script is incompatible with shell nounset. The
launcher now follows the established setup without nounset; the failed launch
log is preserved. Neither issue changed runtime design or removed benchmark
samples.

## Evidence and reproduction

[request-timing-data](request-timing-data/) contains the exact patch, work map,
source/dependency identities, apps, reused harness, comparison/summary scripts,
raw client/server output, resource observations, telemetry and correctness logs.
`python3 examples/14-benchmarks/request-timing-data/summarize.py examples/14-benchmarks/request-timing-data`
recreates the numeric summary. The AWS runner has explicit experiment paths;
consult its work map before running it again.

The dedicated AWS instance is stopped after results are downloaded and checked.
