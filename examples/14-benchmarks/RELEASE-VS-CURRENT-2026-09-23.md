# Installed release versus all current changes — 2026-09-23

Fresh paired comparison requested to show the combined result, rather than
individual optimization deltas or measurements collected in separate runs.

- **Release:** installed PAGI::Server 0.002013.
- **Current:** `experiment/http-simplification` at 659ce38 plus the uncommitted
  shared HTTP receive routine. This includes the newer Server behavior and all
  performance modifications made so far. Current is identified by checkout and
  source hashes; its version declaration still reads 0.002013.
- Both use installed **IO::Async::Loop::EV 0.05**, Perl 5.42.2@default,
  pure-Perl Futures, production, `LIBEV_FLAGS=8 PERL_FUTURE_NO_XS=1`.
- One worker, 25 HTTP clients, 20 persistent WebSocket clients.
- 15 measured seconds/sample plus the existing one-second warmup and preflight.
  Each workload uses release/current/current/release in fresh server processes.
- Same applications and load-generator environment on both sides. No concurrent
  test suite, profiler or other benchmark was intentionally run.

This is not the original 16-worker/500-client setup. The shared host and small
sample count still allow substantial variation; the results below are sample
means, not statistical confidence claims.

## Combined throughput

Arithmetic means of two samples per version. Rates are HTTP requests/sec,
completed SSE streams/sec, or WebSocket echoes/sec, as named in the workload.

| Workload | Release | Current | Current vs release |
| --- | ---: | ---: | ---: |
| GET | 3,200.2 | 2,889.4 | -9.7% |
| 1 KiB POST + completion observer | 3,069.0 | 2,889.4 | -5.9% |
| 64 KiB, one body send | 1,902.3 | 1,738.8 | -8.6% |
| 64 KiB, 64 chunks | 253.9 | 241.4 | -4.9% |
| 64 chunks + completion observer | 261.9 | 254.8 | -2.7% |
| SSE, 100-event streams | 116.1 | 112.5 | -3.0% |
| WebSocket, 128-byte echoes | 4,875.2 | 4,954.7 | +1.6% |

The combined current implementation remains slower on the HTTP/SSE averages
in this run. WebSocket is approximately even; its small positive mean is not
a demonstrated improvement. These numbers do not establish how much of the
original multiworker regression has been recovered.

## Peak server memory

MiB, mean of each process's peak RSS from `/usr/bin/time -l`. Client memory is
excluded. Each process measurement includes preflight, warmup and shutdown.

| Workload | Release MiB | Current MiB |
| --- | ---: | ---: |
| GET | 35.79 | 37.69 |
| 1 KiB POST + completion observer | 36.12 | 37.31 |
| 64 KiB, one body send | 37.56 | 39.08 |
| 64 KiB, 64 chunks | 43.29 | 43.95 |
| 64 chunks + completion observer | 43.01 | 45.39 |
| SSE, 100-event streams | 41.76 | 43.34 |
| WebSocket, 128-byte echoes | 36.21 | 37.53 |

Current is about 0.7–2.4 MiB above release in these samples. The large old-adapter
backlog is absent because both sides use 0.05. The separate
[quiet-machine adapter comparison](QUIET-RERUN-2026-09-23.md) measures the memory
benefit of 0.04 → 0.05 on fixed current Server code; do not combine its percentage
changes arithmetically with this table.

## Individual rates

Samples appear in time order within each version. None were excluded. GET and
WebSocket in particular show substantial variation in absolute rates.

| Workload | Release samples | Current samples |
| --- | --- | --- |
| GET | 2,762.72; 3,637.76 | 2,494.92; 3,283.87 |
| 1 KiB POST + completion observer | 3,063.74; 3,074.29 | 2,862.09; 2,916.66 |
| 64 KiB, one body send | 1,970.38; 1,834.13 | 1,831.41; 1,646.23 |
| 64 KiB, 64 chunks | 256.23; 251.63 | 247.26; 235.54 |
| 64 chunks + completion observer | 259.67; 264.03 | 257.98; 251.58 |
| SSE, 100-event streams | 119.20; 112.93 | 112.54; 112.56 |
| WebSocket, 128-byte echoes | 4,684.62; 5,065.78 | 4,956.26; 4,953.20 |

## Validation and evidence

All **28 samples** completed successfully: **508,119 measured HTTP responses**
and **294,955 measured WebSocket echoes**, in addition to warmup/preflight.
HTTP/SSE preflight checked exact bodies and SSE event order; measured status
codes and load-generator errors were checked. The WebSocket client checked
every echo and its reciprocal Close. Runtime, application and adapter hashes
remained unchanged throughout.

[release-current-data](release-current-data/) contains the work map, driver,
harness, summarizer, loaded source paths/hashes, full raw output, resource
measurements and individual results. The original directory is
`/tmp/pagi-release-current-20260923`. The harness is the same measurement copy
used for the preceding quiet run.

To reproduce, use a writable copy with a fresh `comparison` directory and the
source/dependency versions recorded in metadata, then run:

```sh
perlbrew exec --with perl-5.42.2@default python3 -B compare.py
python3 -B summarize.py
```

No runtime edits, dependency changes, branches, commits or pushes were made
for this comparison. All current development remains on the existing
`experiment/http-simplification` branch.
