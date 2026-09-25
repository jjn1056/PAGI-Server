# Read/write chunk sizes — AWS experiment, 2026-09-25

## Decision

**Adoption update, 2026-09-25:** After the four-way follow-up, the working server
adopted 65536-byte reads with 8192-byte writes and made both independently
configurable. Larger writes remain opt-in. See the
[tuning guide](README.md#tuning-io-chunk-sizes). The decision and measurements
below describe the original experiment, before this adoption.

Preserve the candidate for discussion; do not change the working runtime yet.
Larger chunks give substantial bulk-transfer gains, but the mixed workload
exposes a tradeoff: almost three times the bulk-download throughput accompanies
10.5% less small-request throughput and about 1 ms higher small-request p99.
This does not meet an unqualified claim of improved throughput without a
fairness cost. The saved runtime on PR #23 remains the baseline.

A useful next experiment would separate larger reads from larger writes. The
upload benefit may be available without the mixed-download tradeoff, but that
has not been measured. No new tuning API or additional variant was introduced.

The subsequent [read-only comparison](READ-SIZE-2026-09-25.md) tests that
separation, including competing uploads. It retains the upload gain but finds
a mixed-upload tradeoff as well; neither candidate has been adopted.

## Exact change

Source snapshots start at `5c960b2` on `experiment/http-simplification`, including
the retained listener. Only the two settings below differ. Local runtime was
left unchanged while the user manually tested it. No new git branch/worktree.

```perl
$stream->configure(
    read_len  => 65536,
    write_len => 65536,
    on_read   => ...,
);
```

The before version uses IO::Async::Stream's default 8192-byte lengths.
Autoflush, read_all, write_all, watermarks, logging and callback ownership are
unchanged. These two lines are preserved in `buffer-size-data/candidate.patch`.
The old PR #13 bundled these settings with autoflush; this experiment isolates
buffer sizing from that scheduling change. It does not isolate reading from
writing, or establish 64 KiB as an optimum.

## Results

Each rate is the median of per-run rates. HTTP/SSE rates count completed
requests/streams; WebSocket rates count verified echoes. Primary cases use
three rotated rounds; small GET/POST, SSE and WebSocket controls use two.
No samples are excluded. Comparisons are within each workload.

| Workload | Release 0.002013 | Saved 5c960b2 | 64 KiB | Change vs saved |
| --- | ---: | ---: | ---: | ---: |
| 1 MiB POST | 223.79 | 220.94 | 898.49 | +306.68% |
| 64 KiB download, one send | 3,977.80 | 3,829.09 | 5,445.78 | +42.22% |
| 64 KiB download, 64 sends | 505.17 | 479.18 | 492.75 | +2.83% |
| Small GET, 500 clients | 6,463.17 | 6,204.06 | 6,174.94 | -0.47% |
| Small GET, 25 clients | 8,683.06 | 7,916.43 | 7,848.75 | -0.85% |
| 1 KiB POST | 7,353.04 | 6,901.40 | 6,888.49 | -0.19% |
| SSE streams | 206.69 | 205.28 | 207.72 | +1.19% |
| WebSocket messages | 12,647.97 | 12,139.12 | 12,227.89 | +0.73% |

The 1 MiB upload is about 4.1 times as fast as saved; its p99 falls from
117.6 to 32.0 ms. Single-send download p99 falls from 7.0 to 4.9 ms.
The 500-client GET's p99 is effectively unchanged: release 80.3 ms, saved
86.2 ms, candidate 86.3 ms. These GET runs include the new-client connection
ramp; they do not isolate steady-state service after a connection barrier.
The larger chunks do not materially close the small-response throughput gap.

## Mixed traffic and fairness

One server serves both a small fixed response and a 64 KiB single-send response.
Twenty-five bulk clients run for ten seconds. One hundred small-response clients
run for twelve seconds, beginning 0.5 seconds before the bulk measurement. The
background window therefore includes time without competing bulk traffic; its
statistics are not a precisely clipped measure of the overlap alone. Both
response bodies are checked before load, and all measured responses must be 200.

| Mixed workload metric | Release | Saved | 64 KiB |
| --- | ---: | ---: | ---: |
| Bulk requests/sec | 349.28 | 341.96 | 995.82 |
| Bulk p99, ms | 76.90 | 81.00 | 30.70 |
| Small requests/sec | 7,242.19 | 6,971.06 | 6,237.43 |
| Small p95, ms | 15.50 | 18.00 | 19.40 |
| Small p99, ms | 15.90 | 18.90 | 19.90 |

This is a closed-loop workload with fixed concurrent clients, not fixed offered
bulk bytes/sec. Faster bulk responses let those clients request more data.
The observed shift is real for this workload, but does not prove that small
requests would lose 10.5% throughput under an equal-byte-rate bulk workload.
The measurements support a throughput/fairness tradeoff, not a claim that one
setting is universally preferable.

## Method and verification

Same isolated c7a.xlarge and installed dependencies as the previous comparisons:
Perl 5.42.2, IO::Async::Loop::EV 0.05, epoll (`LIBEV_FLAGS=4`), pure-Perl Future.
Server worker pinned to CPU 0, clients to CPUs 2–3, driver/telemetry to CPU 1.
Client and server share the host; WebSocket and bulk rates may encounter client
limits. No profiling ran concurrently. This is a Linux one-worker comparison,
not a measurement of the user's sixteen-worker Mac configuration.

Timed runs are ten seconds except 500-client GET, which uses fifteen. Other
HTTP cases have 25 clients; WebSocket uses 20 persistent clients. HTTP warmup
uses a separate one-second client invocation. Upload preflight checks the exact
byte count and includes a body split into two writes; download preflight checks
all 65536 bytes, and SSE verifies ordered events. A 1 MiB upload uses the existing
POST app. A small experiment-only `mixed.pl` dispatches the two fairness bodies.

- 27 smoke runs and 69 timed runs passed, with unchanged source/dependency hashes.
- Timed runs returned 2,309,807 foreground HTTP responses, 733,317 background
  HTTP responses, and 740,374 verified WebSocket echoes.
- Focused Linux correctness checks passed: 11 files / 78 tests, covering request
  bodies, streaming, WebSocket, SSE, TLS, transport state and unread-body reuse.
  Two HTTP/2 test files skipped because the host lacks the required 0.011+
  binding. No HTTP/2 validation or full-suite claim is made for this candidate.
- The first focused invocation lacked example-app fixtures in the copied test
  tree. Its failure log is retained; copying those fixtures resolved the setup
  failures before any benchmark ran.
- All 867 timed interval vmstat samples had zero swap-in, swap-out and CPU steal;
  I/O wait peaked at 2%. Resource logs and all individual results are preserved.

Source snapshots and checks ran only on the benchmark host. The local runtime
still matches the before hashes. AWS was stopped after downloading the results.

## Evidence

`buffer-size-data/` contains the exact patch, source identities, work map,
benchmark scripts/apps, focused-test logs, raw results, summaries and telemetry.
The summary preserves individual samples as well as medians. See its README
for reproduction details and payload-file handling.
