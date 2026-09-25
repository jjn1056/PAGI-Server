# Normal HTTP completion cleanup — 2026-09-24

This experiment removes the second `_mark_complete` call on the normal HTTP application-return path. The successful final send already completes the scope through `_h1_end_scope_output`, before waking receivers. The removal adds no state, callbacks or alternate paths. Error handling and other protocol paths are unchanged. Directly affected comments were corrected.

All work is on `experiment/http-simplification`, based on `07a03b8` (saved runtime `b995443`). Root main release preparation is untouched. See [candidate ledger](PERFORMANCE-CANDIDATES.md) for the deferred header-scan and shared-send ideas.

## Correctness

Before editing, six existing test files passed (68 tests). After editing, 15 files passed (253 tests): connection state, post-completion exceptions, deferred callbacks, reentrant cancellation, incomplete responses, receive ownership, HTTP compliance, files, trailers, aborts, receives after clean end, response before request body, body limits after start, unread-body keep-alive and graceful shutdown. The full repository suite was not rerun for this bounded removal.

The initial sandboxed baseline invocation could not bind local sockets; the host-access rerun passed. Initial benchmark setup accidentally removed the perlbrew library path and failed before any measurement. Restoring that path resolved setup; all measured runs below are retained.

## Native comparison

- Release: installed PAGI::Server 0.002013.
- Saved baseline: lib/bin snapshot of 07a03b8, before this removal.
- Candidate: that baseline plus the normal HTTP completion removal.
- Perl 5.42.2@default, EV adapter 0.05, pure-Perl Futures, production, LIBEV_FLAGS=8.
- One worker, 25 HTTP clients, 20 measured seconds plus one-second warmup and content preflight.
- Each workload: release, baseline, candidate, candidate, baseline, release; fresh processes, sequential runs.
- No profiler or test suite ran concurrently. Host load was sampled before each run; this is a shared machine, not an isolated performance lab.

Arithmetic means of two samples per version; requests/sec.

| Workload | Release | Saved baseline | Candidate | Candidate vs release | Candidate vs baseline |
| --- | ---: | ---: | ---: | ---: | ---: |
| GET | 3,317.0 | 3,019.7 | 2,982.1 | -10.10% | -1.25% |
| 1 KiB POST + completion observer | 2,904.6 | 2,894.0 | 2,801.9 | -3.54% | -3.18% |
| 64 KiB / 64 chunks + completion observer | 242.0 | 240.6 | 220.8 | -8.76% | -8.24% |

## Individual samples

| Workload | Release | Saved baseline | Candidate |
| --- | --- | --- | --- |
| GET | 3,094.87; 3,539.13 | 2,855.85; 3,183.63 | 2,906.99; 3,057.30 |
| 1 KiB POST + completion observer | 2,750.76; 3,058.43 | 2,773.56; 3,014.39 | 2,701.59; 2,902.13 |
| 64 KiB / 64 chunks + completion observer | 246.49; 237.44 | 237.74; 243.45 | 217.39; 224.13 |

## Process resources

CPU is user+system time per request, including warmup/preflight. RSS is mean peak server-process memory, not client memory.

| Workload | Release CPU µs/request | Baseline CPU | Candidate CPU | Release RSS MiB | Baseline RSS | Candidate RSS |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| GET | 304.38 | 335.90 | 340.64 | 35.75 | 37.09 | 37.56 |
| 1 KiB POST + completion observer | 349.49 | 350.70 | 363.82 | 36.04 | 37.66 | 37.10 |
| 64 KiB / 64 chunks + completion observer | 4169.41 | 4180.98 | 4556.45 | 46.50 | 45.27 | 44.68 |

All 18 samples passed: 745,137 measured successful HTTP responses, plus warmup and content preflight. Tracked Server source hashes stayed unchanged throughout. Pre-run aggregate CPU idle was 74.69–92.37%; pre-run interval swapout counts ranged from 0 to 0. These snapshots do not establish the load throughout each measurement.

## Interpretation

The cleanup is retained as a small ownership simplification, with **no demonstrated performance benefit**. GET and POST sample ranges overlap; their means fell 1.25% and 3.18% relative to baseline. The first streaming round was more concerning: both candidate samples were below both baseline samples, with an 8.24% lower mean and higher CPU/request. That justified one bounded recheck rather than dismissing the result or adding another optimization.

The recheck changed only run order (release, candidate, baseline, baseline, candidate, release). Source hashes and measurement settings stayed the same. Its streaming result was:

| Workload | Release | Saved baseline | Candidate | Candidate vs release | Candidate vs baseline |
| --- | ---: | ---: | ---: | ---: | ---: |
| Streaming recheck | 237.2 | 236.4 | 241.0 | +1.59% | +1.93% |

| Recheck samples | Release | Saved baseline | Candidate |
| --- | --- | --- | --- |
| Requests/sec | 235.87; 238.47 | 250.61; 222.17 | 269.47; 212.44 |

All six recheck samples passed (28,603 measured responses). Pre-run CPU idle ranged from 69.93% to 94.95%, with zero sampled swapouts; this was still a shared-host run. The candidate's own samples ranged from 212.44 to 269.47 requests/sec, and the mean delta reversed sign. The initial slowdown did not repeat as a consistent ordering of variants. Neither round establishes a causal gain or loss from this deletion. Do not combine these percentages with previous performance changes or claim the release gap has closed. No additional benchmark rounds or optimizations were added.

The normal HTTP path now has one completion owner; behavioral regression checks pass. This is a code cleanup, not a performance fix. Changes are left on the existing working branch for review, uncommitted and unpushed.

## Evidence

[completion-cleanup-data](completion-cleanup-data/) contains the work map, runtime patch, baseline/after test logs, benchmark driver, identities/hashes, raw client and resource output, load samples and summarizers. Source snapshots and full server logs remain under `/tmp/pagi-completion-cleanup-20260924`.

The driver imports the existing quiet-rerun harness. To repeat, use a new output directory and recreate the baseline snapshot identified by the work map; do not benchmark an edited source tree against itself. This is the one-worker diagnostic setup, not the original 16-worker / 500-client comparison. SSE and WebSocket throughput were not remeasured because this removal is in the ordinary HTTP application-return path.
