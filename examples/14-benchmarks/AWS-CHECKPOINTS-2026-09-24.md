# AWS checkpoint comparison — 2026-09-24

This comparison remeasures retained changes on the existing isolated EC2
benchmark instance. It adds no runtime changes, revisits no discarded prototype,
and leaves the working branch uncommitted. It is separate from the previous
CPAN/current round; rates are not pooled across those experiments.

## Identities

| Label | Source | What changed |
|---|---|---|
| release | Installed CPAN PAGI-Server 0.002013 | Reference |
| pre | 4625b006aa136844ab80f9163bcffc25db748cb3 | Functional server changes before performance work |
| saved | b99544311ebc6454d3f280adbb90b32d299340a9 | Retained ConnectionState/receive/send simplifications |
| cleanup | Exact pre-header snapshot | Removal of redundant normal HTTP completion call |
| current | Current working tree | Shared checked/public and private header encoding |

Pre is runtime-identical to c00219c before the first optimization. Saved is
runtime-identical to current branch HEAD 07a03b8. Saved to cleanup changes only
Connection.pm; cleanup to current changes only Connection.pm and Protocol/HTTP1.pm.
Current matches all lib/bin hashes from the preceding AWS comparison.
The uncommitted snapshots are identified by per-file hashes, not just Git IDs.

## Method

Same c7a.xlarge instance, restarted; public IP for this boot 54.146.170.184.
Ubuntu 24.04, Perl 5.42.2, IO::Async 0.805, EV 4.37, Loop::EV 0.05,
pure-Perl Future 0.52, Linux epoll. Installed packages were not changed.
Server pinned to CPU 0, driver/monitoring CPU 1, clients CPUs 2–3 with
GOMAXPROCS=2. One server worker and 25 clients, 15 seconds measured plus
one second of warmup per run. Smoke runs use two seconds and are excluded
from performance summaries. All workloads use the same benchmark applications.

Four workloads: GET, ten headers, 1 KiB POST plus completion observer, and
64 KiB streaming in 64 chunks plus completion observer. For each workload,
three rounds use these exact checkpoint orders:

1. release, pre, saved, cleanup, current
2. current, cleanup, saved, pre, release
3. pre, current, release, cleanup, saved

All twenty checkpoint/workload combinations pass smoke validation before the
sixty timed runs begin. The driver checks every snapshot, the existing worktree
manifest, loaded dependency hashes and both harness/driver hashes after each run.
GNU time records server resource usage. vmstat samples the host every second;
pidstat samples active processes without the earlier name filter, so renamed
server workers are included. No profiler or tests run alongside measurements.

This is a small diagnostic comparison, not a many-worker capacity test.
Three samples do not establish a precise statistical confidence interval;
small differences must be assessed against sample spread and order effects.
No samples are excluded. Source snapshots and the archive are retained locally
under /tmp/pagi-aws-checkpoints-20260924 and remotely under
/home/ubuntu/pagi-benchmark/checkpoints. Access/lifecycle instructions are in
AWS-BENCHMARK-2026-09-24.md; no new instance was created.

## Results

All 20 smoke combinations and 60 timed runs completed successfully. Timed runs
returned 4,804,560 successful HTTP responses. All source, dependency, driver and
harness hashes remained unchanged; current's local source was verified again
after collection. No new runtime changes or full-suite run were made in this
measurement-only task. Existing correctness evidence remains separate.

Mean requests/sec, three samples per cell:

| Workload | CPAN release | Before performance work | Saved optimizations | + Completion cleanup | + Header encoder (current) |
|---|---:|---:|---:|---:|---:|
| get | 8,489.2 | 6,917.1 | 7,257.1 | 7,262.0 | 7,373.1 |
| headers | 7,610.2 | 6,255.8 | 6,468.1 | 6,492.6 | 6,824.6 |
| post-observe | 7,441.1 | 6,143.3 | 6,532.2 | 6,592.3 | 6,656.8 |
| stream-observe | 499.0 | 470.2 | 487.6 | 488.4 | 489.2 |

Each step relative to its immediate predecessor:

| Workload | Saved vs pre | Cleanup vs saved | Header encoder vs cleanup | Current vs CPAN |
|---|---:|---:|---:|---:|
| get | +4.91% | +0.07% | +1.53% | -13.15% |
| headers | +3.39% | +0.38% | +5.11% | -10.32% |
| post-observe | +6.33% | +0.92% | +0.98% | -10.54% |
| stream-observe | +3.70% | +0.18% | +0.16% | -1.95% |

Individual samples, in collection order within each variant:

| Workload | Checkpoint | Requests/sec |
|---|---|---|
| get | release | 8,475.89; 8,434.69; 8,556.99 |
| get | pre | 6,899.98; 6,978.00; 6,873.29 |
| get | saved | 7,225.49; 7,261.70; 7,283.96 |
| get | cleanup | 7,321.85; 7,315.92; 7,148.37 |
| get | current | 7,404.75; 7,391.38; 7,323.17 |
| headers | release | 7,574.18; 7,609.79; 7,646.54 |
| headers | pre | 6,237.66; 6,299.68; 6,230.07 |
| headers | saved | 6,526.85; 6,497.67; 6,379.77 |
| headers | cleanup | 6,450.27; 6,495.02; 6,532.44 |
| headers | current | 6,846.33; 6,799.01; 6,828.32 |
| post-observe | release | 7,508.63; 7,368.22; 7,446.51 |
| post-observe | pre | 6,153.26; 6,121.08; 6,155.47 |
| post-observe | saved | 6,504.34; 6,584.11; 6,508.26 |
| post-observe | cleanup | 6,653.03; 6,529.13; 6,594.89 |
| post-observe | current | 6,627.73; 6,683.54; 6,659.23 |
| stream-observe | release | 495.08; 499.18; 502.66 |
| stream-observe | pre | 467.28; 470.16; 473.02 |
| stream-observe | saved | 487.21; 490.83; 484.65 |
| stream-observe | cleanup | 490.39; 485.46; 489.44 |
| stream-observe | current | 490.63; 492.42; 484.62 |

## Interpretation

The saved optimization checkpoint improved all four workloads relative to pre,
with the same direction in all three rounds: GET +4.91%, ten headers +3.39%,
POST +6.33%, streaming +3.70%. This supports the earlier decision to retain
those changes; the measured gains are not solely artifacts of the Mac runs.
This grouped checkpoint does not assign a gain to each constituent commit.

The completion cleanup ranges from +0.07% to +0.92% on means and changes
direction within the individual rounds. Keep its justification as simpler
completion ownership. These data do not establish a speed improvement or the
large streaming penalty suggested by one earlier Mac round.

The header encoder improves ten-header responses by +5.11%, with round
differences +6.14%, +4.68% and +4.53%. GET shows a smaller positive result
(+1.53%, positive in all three rounds). POST and streaming effects are small
and change direction: do not claim those as gains. The earlier apparent POST
regression did not reproduce consistently on this machine. The ten-header
result supports retaining this simplification on measured performance as well
as its existing correctness review. No generic speedup is implied.

Most of the remaining release gap is already present at pre, before the
performance work. Current improves on pre by about 6.6% GET, 9.1% ten headers,
8.4% POST and 4.1% streaming, while still behind CPAN by 13.1%, 10.3%, 10.5%
and 2.0% respectively in this round. This is evidence to keep the retained
work, not to revert functional/spec fixes. It does not isolate which earlier
functional change causes the remaining gap.

Do not reopen all parked ideas based on these data. The byte counter's extra
accounting concerns remain independent of its old timing. If another
performance investigation is authorized, the next useful evidence is a focused
AWS profile of the remaining release/current HTTP gap. No new optimization or
profiling pass was performed here.

## Host checks and limits

All 997 post-initial vmstat samples reported zero swap-in/out, zero CPU steal
and zero I/O wait at one-second/integer precision. All server runs had zero
major faults. The active-process telemetry now includes renamed server workers.
HTTP client CPU peaked at 41% of one core; sampled server CPU averaged
96.6% and peaked at 101%. This supports a server CPU limit for these
workloads rather than a saturated HTTP load generator.

Sample ranges span roughly 0.6–2.4% of their means. Three reordered rounds
provide stronger evidence for the larger effects, but they are not a broad
production workload study. Previous AWS and Mac rates are not pooled here,
and no small delta is promoted to a precise general claim.

Evidence is in `aws-checkpoint-data/results/`, including summaries, raw client
and server logs, resource usage, metadata and both smoke/timed runs. The instance
is stopped after download; its disk and all snapshots are retained for restart.
