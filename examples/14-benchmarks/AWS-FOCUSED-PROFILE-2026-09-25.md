# Focused AWS CPU profile: release versus the canonical checkpoint

## Result

The retained runtime and benchmark work is committed as
`874b1201c58ea97b1eaf67e50fe9c4449ae7e2ba` on
`experiment/http-simplification`. Independent review found no blockers; the
fresh targeted verification passed 15 files / 201 tests and `git diff --check`.
No runtime changes were made during profiling. Nothing was pushed.

Twelve completed profiles (eight subroutine, four statement) support the same
broad explanation as the earlier Mac investigation: added terminal delivery,
send-state publication and lifecycle checks account for visible extra work.
There is no new Future allocation surge, repeated scope construction or duplicate
normal completion call. The profile does not establish a large, safely removable
operation, nor quantify how much of the native release gap each operation causes.

## Keep the native comparison separate

These are the **previous uninstrumented AWS checkpoint audit** means, not new
throughput measurements from this profile. The archived commit's lib/bin files
were verified byte-for-byte against that audit's current checkpoint.

| Workload | CPAN release 0.002013 | Current 874b120 | Versus release |
| --- | ---: | ---: | ---: |
| GET | 8,489 req/s | 7,373 req/s | -13.15% |
| Ten-header GET | 7,610 req/s | 6,825 req/s | -10.32% |
| 1 KiB POST with completion observer | 7,441 req/s | 6,657 req/s | -10.54% |
| Chunked stream with completion observer | 499.0 req/s | 489.2 req/s | -1.95% |

The retained optimizations improve the pre-performance development baseline;
most of the remaining release gap predates them. See
[AWS-CHECKPOINTS-2026-09-24.md](AWS-CHECKPOINTS-2026-09-24.md) for individual
samples and validation. Do not combine its samples with instrumented rates.

## Method and identity

- Same c7a.xlarge, Ubuntu 24.04, installed CPAN Server 0.002013; one worker,
  25 concurrent clients, pure-Perl Future, EV loop, `LIBEV_FLAGS=4` (epoll).
- Exact `git archive` of 874b120, transferred without a push or remote GitHub
  credentials. Archive SHA256:
  `b882719048ed915ad7c16252b938e0ab67dc26e7a86eca4f4586e272bfa9e128`.
  This was a commit-derived archive, not a remote Git clone.
- Devel::NYTProf 6.15 installed and tested on the benchmark host. CPU clock
  `CLOCK_PROCESS_CPUTIME_ID=2`, verified using Time::HiRes on that host.
  Server affinity CPU 0, driver CPU 1, hey CPUs 2–3; `GOMAXPROCS=2`.
- NYTProf: `clock=2:calls=0:slowops=0:compress=1:start=init`,
  `stmts=0` for subroutine profiles and `stmts=1` for statement profiles.
- Each subroutine profile: 3 checked requests, 200 warmup requests, 5,000
  measured requests. Each statement profile: 3 + 200 + 2,000 requests.
  Both workloads run release/current, then current/release for subroutines;
  release/current once for statement profiles. All 48,000 measured requests
  in these twelve runs returned 200 without client errors. Preflight checks
  verified bodies; warmup and measured status/count checks passed.
- Complete profiled process lifetime includes startup and teardown. Numbers
  below divide by all requests in the profile, not just the measured phase.
  Fixed setup contributes to some counts, notably Future allocation counts.
- 67 interval vmstat samples: zero reported swap-in, swap-out and steal;
  maximum reported I/O wait 1%. No samples were discarded. CPU-time profiling
  still imposes substantial overhead: profiled GET throughput is roughly
  1,400–1,600 req/s with subroutine instrumentation and about 300–330 with
  statement instrumentation. These are not estimates of native performance.

The source metadata distinguishes variants even though both modules still
report version 0.002013 on this experimental branch. Each run records loaded
module paths, server source hashes, application hash, command and environment.

## What the counts establish

Each entry is calls per request unless stated otherwise. Counts agree across
both repeated subroutine runs.

| Operation | Release GET | Current GET | Release POST | Current POST |
| --- | ---: | ---: | ---: | ---: |
| ConnectionState construction | 1 | 1 | 1 | 1 |
| Normal completion mark | 1 | 1 | 1 | 1 |
| Deferred terminal delivery / loop `later` | 0 | 1 | 0 | 1 |
| State-publisher callback | 0 | 3 | 0 | 3 |
| `scope_send_clean` | 0 | 2 | 0 | 3 |
| Abort-hook factory | 0 | 1 | 0 | 1 |
| Future::PP::new, total calls per 5,203-request profile | 20,878 | 20,878 | 26,081 | 26,081 |

Zero in the release callback rows means it lacks that operation, not that it
lacks completion behavior: release invokes completion callbacks synchronously.
Current separates immediately visible terminal facts from deferred notification.
The POST case registers an actual completion observer; its additional delivery
work is not merely an artifact of an unobserved hello-world endpoint.

The shared receive routine does not add Future allocations relative to release.
Do not mistake its new named profile entry for entirely new work: release does
that work inside anonymous routines.

## CPU attribution, with limits

Mean exclusive CPU microseconds per request from the two subroutine profiles:

| Operation | Release GET | Current GET | Release POST | Current POST |
| --- | ---: | ---: | ---: | ---: |
| Scope factory | 13.20 | 15.34 | 13.31 | 15.44 |
| Receive factory | 2.63 | 3.68 | 2.66 | 3.67 |
| Send factory | 4.06 | 7.20 | 4.26 | 7.32 |
| Terminal delivery helper | 0 | 5.99 | 0 | 6.01 |
| Loop `later` | 0 | 3.62 | 0 | 3.60 |
| EV adapter `watch_idle` | 0 | 5.34 | 0 | 5.40 |
| State-publisher callback | 0 | 3.92 | 0 | 3.93 |
| Clean-state predicates | 0 | 3.31 | 0 | 4.74 |

These are **instrumented attribution**, not native costs or predicted savings.
The CPU clock avoids charging socket waiting as CPU work, but cannot remove
profiler overhead. Call-heavy code is particularly distorted. Do not add
inclusive time to exclusive time or compare a new helper against zero without
considering where its work lived in release.

The statement profiles corroborate the added publisher, completion checks and
terminal-delivery machinery. Their prominent exception-tail and coroutine-return
lines also include attribution around calls/returns; they do not demonstrate
that a successful request is executing expensive exception handling. No such
claim or optimization is justified here.

## Concrete next candidate, not another optimization yet

`Connection.pm::_create_send` owns a lexical `$seq`, then publishes it into the
connection through `$publish` at creation and after each of the two send events:

```perl
my $seq = 'initial';
my $publish = $opt{on_state}
    // sub { $weak_self->{h1_seq} = $_[0] if $weak_self };
$publish->($seq);
# Each send:
$seq = advance_http($seq, $event);
$publish->($seq);
```

This is a concrete place to examine whether one authoritative state owner could
remove copying and an adapter, while retaining HTTP refusal delegation, invalid
send rollback, weak ownership and terminal observation. It is not permission to
replace the callback with another flag/cache or to bypass state validation.
The observed cost is small; no material speedup is established.

The larger visible terminal-delivery family exists for required callback
ordering. Removing deferral, moving callbacks into the application's send stack,
or weakening abort/receive semantics would not be an acceptable optimization.
A design that merely skips unused work also does not address the POST observer
case. Keep the current checkpoint; discuss a bounded state-ownership experiment
only if its simpler design can be made concrete. Repeated profiling alone is
unlikely to uncover a large missed cost in these two workloads.

## POST profiling limitation and failed attempts

The first driver launch used a relative output path; GNU time failed before
starting the server. That driver path was corrected. Its log is preserved.

The next attempt profiled the existing split-body POST preflight. Release
aborted with:

```
Future::AsyncAwait panic: TODO: Unsure how to handle savestack entry of SAVEt_ALLOC=0
```

This reproduces the earlier instrumentation limitation. No server change was
made to work around it. The final profiling driver uses three buffered requests
and omits only the split-body preflight, for both variants. This permits the
normal buffered POST hot path to be profiled; it does **not** profile suspension
and resumption while waiting for delayed request data. The original uninstrumented
benchmark harness retains its split-body preflight and passed it during the
checkpoint audit. A proposed extra uninstrumented recheck after downloading
results did not run because the host was already unreachable/stopped; do not
count it as new validation.

The instance was already stopped when the final stop call was made. AWS
explicitly confirmed `stopped`; its automatic-stop timer had been active.
No benchmark services remain running on the instance.

## Evidence

[aws-focused-profile-data](aws-focused-profile-data/) contains commands,
extractors, source identities, raw client/server logs, extracted subroutine and
statement data, telemetry, summary and failed-attempt logs. `analyze.py` recreates
`summary.json` from the saved data. Binary profiles remain in the downloaded
46 MiB archive at `/tmp/pagi-aws-profile-20260924/results.tar.gz` and on the
stopped instance; their hashes are recorded with the evidence. They are not
committed to Git. The local /tmp archive is temporary, not durable storage.
