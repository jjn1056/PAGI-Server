# Quiet-machine streaming and EV comparison — 2026-09-23

The repeat streaming measurements are effectively flat for the shared receive
refactor. SSE and WebSocket are also close. Separately, the installed
IO::Async::Loop::EV 0.05 adapter substantially reduces peak memory during busy
HTTP traffic, without demonstrating a general throughput improvement here.

No Server runtime changes, dependency installations or new Git branches were
made for this run. All development remains on `experiment/http-simplification`.

## Two separate comparisons

1. **Receive refactor:** the existing performance work at 659ce38, before the
   latest receive change, versus the same work plus the shared receive routine.
   Both use installed adapter 0.05. The before version is the frozen temporary
   `lib`/`bin` copy used in the preceding experiment, not another branch.
2. **EV adapter:** the current Server code held fixed, with pristine adapter
   0.04 versus installed 0.05. A process-local `PERL5LIB` override selects 0.04
   for those server processes only. The load-generator environment, including
   the WebSocket client's installed dependencies, stays unchanged. The user's
   installed adapter remains 0.05.

“EV upgrade” here means **IO::Async::Loop::EV 0.04 → 0.05**, not a change to the
underlying EV library. Source paths, versions and hashes were checked.

## Conditions and checks

The user requested this run while avoiding desktop activity. No test suite,
profiler or other benchmark was intentionally run alongside it. This is still
a shared host running both client and server, not an isolated performance lab.

- Perl 5.42.2@default; one EV worker; production mode.
- `LIBEV_FLAGS=8 PERL_FUTURE_NO_XS=1`; loaded Future::PP verified.
- 25 HTTP clients; 20 persistent WebSocket clients.
- Receive comparison: 20 measured seconds per sample. Adapter comparison:
  10 measured seconds per sample. Both include the existing one-second warmup.
- Every workload runs baseline/candidate/candidate/baseline in fresh processes.
- HTTP/SSE preflight verifies exact response content and SSE event sequence.
  The WebSocket client checks every echo and the reciprocal Close code.
- `/usr/bin/time -l` measures server process CPU and peak RSS. Client memory is
  excluded. Resource totals include preflight, warmup and shutdown.

All **36 runs** completed: **331,013 measured HTTP responses** and **611,961
measured WebSocket echoes**, plus preflight and warmup traffic. No load-generator
errors or unexpected HTTP status codes were accepted. Runtime, application and
installed-adapter hashes remained unchanged. No samples were dropped.

## Latest receive change, adapter 0.05 on both sides

Arithmetic means of two samples. HTTP rates are requests/sec, SSE rates are
completed 100-event streams/sec, and WebSocket rates are 128-byte echoes/sec.

| Workload | Before receive change | After receive change | Change | Peak RSS before / after, MiB |
| --- | ---: | ---: | ---: | ---: |
| 64 KiB, one body send | 1,671.1 | 1,746.0 | +4.5% | 38.75 / 38.67 |
| 64 KiB, 64 chunks | 258.0 | 258.5 | +0.2% | 44.97 / 44.42 |
| 64 chunks with completion observer | 260.2 | 261.9 | +0.6% | 44.37 / 44.30 |
| SSE | 121.0 | 121.1 | +0.02% | 43.67 / 43.64 |
| WebSocket | 5,079.7 | 5,132.6 | +1.0% | 37.46 / 37.69 |

Individual rates, in time order within each version:

| Workload | Before samples | After samples |
| --- | --- | --- |
| One body send | 1,582.5; 1,759.8 | 1,775.9; 1,716.0 |
| 64 chunks | 257.2; 258.8 | 257.2; 259.8 |
| 64 chunks + observer | 260.7; 259.8 | 260.8; 262.9 |
| SSE | 123.9; 118.2 | 122.6; 119.6 |
| WebSocket | 5,071.6; 5,087.8 | 5,153.4; 5,111.8 |

This does not reproduce either the earlier +9.0% streaming result or its -3.8%
reverse-order result. The quieter samples support treating streaming as
essentially unchanged by this refactor. SSE and WebSocket are useful controls:
their benchmark paths do not use the changed HTTP receive factory.

The one-send average is positive, but its baseline samples vary and overlap the
candidate samples. It is a promising observation, not proof of a universal
4.5% improvement. These runs do not revisit the previous GET/POST comparison.

## Adapter 0.04 versus 0.05, current Server code on both sides

| Workload | Adapter 0.04 rate | Adapter 0.05 rate | Change | Peak RSS 0.04 / 0.05, MiB |
| --- | ---: | ---: | ---: | ---: |
| GET with completion observer | 3,186.8 | 3,197.0 | +0.3% | 175.25 / 36.99 |
| 64 chunks with completion observer | 259.4 | 256.9 | -1.0% | 54.49 / 44.15 |
| SSE | 123.7 | 120.2 | -2.9% | 47.22 / 43.12 |
| WebSocket | 5,059.3 | 5,108.1 | +1.0% | 37.72 / 37.75 |

Individual rates:

| Workload | Adapter 0.04 samples | Adapter 0.05 samples |
| --- | --- | --- |
| GET + observer | 3,197.2; 3,176.4 | 3,205.6; 3,188.4 |
| 64 chunks + observer | 261.2; 257.7 | 257.4; 256.4 |
| SSE | 123.1; 124.4 | 122.1; 118.3 |
| WebSocket | 5,048.7; 5,069.8 | 5,059.8; 5,156.4 |

The GET footprint falls by about **79%**, streaming by **19%**, and SSE by
**9%** in these workloads. Persistent WebSocket echo shows no comparable memory
difference. The workloads complete HTTP/SSE scopes repeatedly, while WebSocket
keeps its 20 connections open during the echo measurement; terminal-cleanup
frequency is therefore different.

The memory result agrees with the previously verified starvation mechanism:
0.04's idle callbacks could leave terminal cleanup queued under continuous I/O,
retaining state until traffic subsided. Version 0.05 delivers that work during
traffic. Older raw request rates therefore do not tell the whole story: some
cleanup was deferred beyond the measured traffic interval. This run measures
peak memory and throughput, not callback-delay distributions; the earlier
[installed-0.05 verification](POST-EV-005-2026-09-23.md) contains that probe.

There is no general throughput gain here. SSE is modestly slower in these two
samples, and the other rate changes are about 1% or less. These data do not
isolate the cause of those small differences or establish a general regression.
Lower memory use provides capacity headroom; it does not automatically improve
throughput in a CPU-bound workload that is not under system memory pressure.

## Latency observations

The summary records every sample's p50 and p99. These are averages of per-run
percentiles, not pooled percentiles or formal confidence intervals.

- Receive comparison: ordinary streaming p99 averaged 130.05 → 117.70 ms;
  streaming with an observer was 118.05 ms on both sides. WebSocket echo p99
  was 6.42 → 6.26 ms.
- Adapter comparison: GET p99 was 10.3 → 10.4 ms; observer-stream p99 was
  119.9 → 118.4 ms; SSE p99 was 413.35 → 461.0 ms; WebSocket p99 was
  6.32 → 6.42 ms.

Those observations are not evidence of a consistent response-latency win.
Response latency and deferred terminal-callback latency are separate quantities.

## Assessment and evidence

Keep the conclusions separate: the shared receive routine remains a tested
structural cleanup, with no established streaming/SSE/WebSocket performance
effect; the upstream adapter fixes callback progress and substantially reduces
retained memory. Neither comparison demonstrates recovery of the original
release-to-main throughput regression. The original 16-worker/500-client setup
was not retested here.

[quiet-rerun-data](quiet-rerun-data/) contains the work map, complete metadata,
source hashes, harness, driver, summary script, all client/server logs, resource
measurements, and individual results. The harness is a copy of the existing
benchmark runner with two measurement-only changes: wrapping the server in
`/usr/bin/time` and allowing a separate server environment. Workloads and clients
are unchanged.

The original output is under `/tmp/pagi-quiet-bench-20260923`. Reproduction uses
`perlbrew exec --with perl-5.42.2@default python3 -B compare.py` followed by
`python3 -B summarize.py`, from a writable copy with fresh `receive` and
`adapter` output directories and the source paths recorded in the work map.
The old adapter override must stay server-only. The installed adapter was not
replaced, and no new full Server test run was needed for this evidence-only work.
