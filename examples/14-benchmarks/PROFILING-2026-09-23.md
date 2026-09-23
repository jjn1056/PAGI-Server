# Release-to-main profiling investigation — 2026-09-23

**Finding:** deferred terminal delivery accumulates during sustained traffic with
the installed IO::Async EV backend. This delays real application cleanup and
retains queued callback state. It deserves attention before further throughput
tuning. The investigation does not establish how much of the overall performance
regression this mechanism causes; no runtime fix was made or benchmarked here.

## Scope and method

- Installed Server 0.002013 versus root main 4625b00 (existing version-only
  library changes say 0.002014). The simplification worktree at 148a01f is a third
  implementation where noted. Main/release preparation and PR13 are untouched.
- Perl 5.42.2@default, Future 0.52, Future::AsyncAwait 0.71, NYTProf 6.15,
  IO::Async 0.805, IO::Async::Loop::EV 0.04, EV 4.37.
- EV, production, one worker, 25 clients, LIBEV_FLAGS=8 and
  PERL_FUTURE_NO_XS=1. All profiles show Future::PP calls. No concurrent load.
- Identical existing GET, POST with completion callback, and 64-send/64-KiB
  streaming apps. Exact-body preflight includes a POST split across two writes.
- Fixed request counts: 200 warmup requests, three preflight requests (four for
  POST), then 10,000 uninstrumented requests or 2,000 requests for the final
  subroutine profiles. Profiles include startup and preflight/warmup; request-path
  call counts are normalized by the actual total of 2,203 for GET/stream.
- Server CPU and peak RSS come from native `/usr/bin/time -l`, around the server
  process only. They include startup, checks, warmup, serving and clean shutdown;
  they exclude the load generator and profile extraction. These are supporting
  measurements, not isolated per-request CPU timings.
- All source hashes captured in metadata were rechecked after collection.

## Uninstrumented measurements

One fixed-count sample per implementation/workload. These corroborate the earlier
benchmark regression but do not replace its repeated comparisons. Same-host load
and run-to-run noise still apply.

| Workload | Release requests/sec | Main requests/sec | Change | Release/main server CPU seconds | Release/main peak RSS MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| GET | 3,614.2 | 2,998.4 | −17.0% | 3.09 / 3.76 | 36.3 / 75.8 |
| Small POST with completion callback | 2,945.0 | 2,705.3 | −8.1% | 3.73 / 4.13 | 36.1 / 80.0 |
| 64-send response | 229.4 | 203.3 | −11.3% | 44.52 / 50.14 | 42.7 / 66.2 |

The memory difference is consistent with retained delivery work, but this is not
a retained-heap attribution: source size, allocations and other state also differ.

## Primary finding: terminal callbacks wait for idle I/O

The path in current main is:

```text
ConnectionState::_mark_complete
  -> _deliver_terminal
     -> IO::Async::Loop::later
        -> IO::Async::Loop::EV::watch_idle
           -> EV::idle
```

Main's `ConnectionState.pm` lines 299–311 schedule the delivery closure with
`$loop->later($code)`. The closure holds the ConnectionState and registered
callbacks alive. The installed Loop::EV implementation (lines 149–166) allocates
one idle watcher per call and removes it when that callback runs.

EV idle watchers run only when no other watchers of equal or higher priority are
pending. The installed backend uses the default priority. Continually ready I/O
can therefore postpone these callbacks across many loop iterations. This matches
the [documented EV idle semantics](https://metacpan.org/pod/EV#IDLE-WATCHERS---when-you've-got-nothing-better-to-do...)
and explains why treating this mechanism as a guaranteed next-turn delivery is
unsafe under load. IO::Async's `later` documentation describes deferred execution
after the current I/O round; the observed backend behavior needs consideration at
that integration boundary, not an assumption that all loops behave this way.

Three checks establish the behavior:

1. **Temporary idle-watcher counter:** no NYTProf; the diagnostic wraps only the
   backend's registration/callback methods without changing scheduling. Main
   GET and POST each accumulated **10,000 pending callbacks**. All were eventually
   delivered and pending was zero at exit. The release scheduled none on these
   paths because its completion callbacks run synchronously.
2. **Independent application-level counter:** no profiler or loop instrumentation.
   A normal app registers `on_complete` and counts completed requests. Its
   response is the same small GET body. At request 10,000, main and the experiment
   had delivered only **203** callbacks: the three preflight and 200 warmup
   requests. Both delivered the measured batch afterward. The release had
   delivered 9,999 at entry to request 10,000. This confirms delayed application
   callbacks, not merely bookkeeping in the diagnostic wrapper.
3. **Standalone loop reproduction:** no PAGI or NYTProf. A pipe stays readable
   for 0.1 seconds while one `later` callback waits. **32,102 read callbacks** ran
   while the `later` callback remained pending; it ran after the readable work
   stopped. The reproduction is preserved as `later-busy-io.pl`.

| Application counter | Release | Main | Simplification branch |
| --- | ---: | ---: | ---: |
| Peak requests minus delivered completion callbacks | 1 | 10,000 | 10,000 |
| Maximum request-entry-to-callback delay | 0.361 ms | 3,336.950 ms | 3,394.243 ms |
| Final callbacks delivered / HTTP requests | 10,203 / 10,203 | 10,203 / 10,203 | 10,203 / 10,203 |

This delay includes the tiny request's work, not just scheduling latency. These
are finite load tests: they establish seconds of delay and a growing backlog, not
a measured infinite delay. The callback closures' lifetime follows the backlog;
resources captured by real cleanup callbacks can consequently remain live longer.
Other terminal outcomes use the same helper, but were not load-tested here.

The release's synchronous delivery is **not** the desired fix: current PAGI
requires terminal callbacks to be deferred outside the application's own send or
receive. Preserve that protection while ensuring delivery makes progress during
busy I/O. Merely batching work onto the same idle mechanism would not by itself
solve starvation. No spec relaxation or observer-disabling option is proposed.

## What the successful profiles show

NYTProf subroutine profiles succeeded for GET and streaming on release, main and
the simplification branch. Their elapsed timings are instrumented and not CPU
measurements. Call counts provide the strongest comparisons:

| Request-path operation per request | Release GET | Main GET | Experiment GET | Release stream | Main stream | Experiment stream |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Send-event validation | 2 | 2 | 2 | 65 | 65 | 65 |
| Added send `on_done` registrations | 0 | 2 | 0 | 0 | 65 | 0 |
| `scope_send_clean` checks | 0 | 2 | 2 | 0 | 65 | 65 |
| `_mark_complete` calls | 1 | 2 | 2 | 1 | 2 | 2 |
| Deferred terminal deliveries / idle watchers | 0 | 1 | 1 | 0 | 1 | 1 |
| Future allocations, including small connection/setup contribution | ~4.03 | ~4.03 | ~4.03 | ~67.03 | ~67.03 | ~67.03 |

The `on_done` row subtracts the common 53 setup/connection registrations from
each run. The other request-path counts are exact multiples of 2,203.

This explains why removing the send wrapper is plausible for the streaming gain:
it removes 65 callback registrations and wrapper executions per response, while
preserving validation. It does not remove per-request deferred delivery. It also
shows that the regression is not explained by a general increase in Future
allocations for these workloads.

The initial 5,000-request GET profiles show additional work concentrated in request
setup and lifecycle management: receive/send closure setup, the abort hook,
publishing send state, the send completion wrapper, deferred delivery and the
extra completion check at app return. Validation and serialization remain major
absolute costs, but their core call counts do not increase. Hot is not synonymous
with responsible for the regression.

For scale only, in the final GET profiles main's `watch_idle` and `EV::Idle::DESTROY`
self times total about 10.3 instrumented microseconds per request; that excludes
the surrounding state transition, `later`, callback wrapper and deferred cleanup.
Do not convert that number into a promised unprofiled speedup. The experiment
removes the send registrations but retains the other lifecycle work.

## Profiler limitations encountered

- Time::HiRes exposes process-CPU clock 12, but this NYTProf build reports
  `clock_gettime not supported on this system` and falls back to its elapsed
  clock. The warning is saved. Final successful profiles omit the unavailable
  clock option; native process accounting provides the separate CPU totals.
- Subroutine profiling crashes the released server in the split-body POST
  preflight with `Future::AsyncAwait panic: TODO: Unsure how to handle savestack
  entry of SAVEt_ALLOC=0`. The documented `slowops=0` mode also fails there.
  No POST load profile is claimed. The unchanged POST preflight and load both
  pass without NYTProf.
- Statement-only profiling fails a small await probe with `panic: unimplemented
  op custom (#390)`. The same small probe passes with subroutine-only profiling;
  this is not a claim that every await fails under subroutine profiling.
- No dependency patch, server change, skipped split-body check, or altered async
  flow was used to get around these failures. GET/stream results are bounded by
  the paths they successfully exercised, not validation of all suspending paths.

## Recommended next work

**First: resolve deferred-delivery progress under sustained I/O.** Decide whether
the appropriate change belongs in the loop adapter or the server's use of its
public scheduling API. Keep the server portable across supported loops; do not
hard-code EV into ConnectionState. The acceptance case is a completion callback
running while sustained traffic continues, with the existing deferral, once-only
delivery, shutdown and cancellation guarantees intact. Then measure throughput,
callback delay and memory. This addresses actual application behavior even if
the throughput benefit is modest.

**Second, only afterward: audit completion ownership.** The terminal send now
marks the scope; the normal app-return path marks it again. The second call is
currently idempotent, so deleting it blindly is unjustified: synthesized errors,
file/fh, HEAD, trailers and protocol refusals must still have one clear owner.
Its direct cost is small, so this is a clarity/correctness audit rather than the
leading performance bet. No recommendation to strip required validation or to
special-case a benchmark app follows from this evidence.

## Evidence and reproduction

[profiling-data/](profiling-data/) contains scripts, the work map, selected exact
subroutine records/callers, metadata, all completed run results, process counters,
application logs and failure summaries. Complete binary profiles and extraction
output are retained locally under ignored
`local/profiling/2026-09-23/` in the experiment worktree. Scripts retain the local
checkout paths deliberately; they are investigation artifacts, not new server
APIs. Adjust ROOT/EXP in profile.py for another machine and use fresh output paths.

The runner uses the existing benchmark runner's exact-response preflight,
hey-result checks and process-group cleanup. Example invocations from the saved
scripts directory, with the project's Perl environment:

```sh
perlbrew exec --with perl-5.42.2@default python3 -B profile.py \
  --output /tmp/pagi-profile-repeat --mode sub --cases get stream \
  --requests 2000 --variants release main experiment

perlbrew exec --with perl-5.42.2@default python3 -B profile.py \
  --output /tmp/pagi-native-repeat --mode off --requests 10000

perlbrew exec --with perl-5.42.2@default python3 -B profile.py \
  --output /tmp/pagi-completion-repeat --mode off --requests 10000 \
  --cases get --variants release main experiment --app "$PWD/completion-get.pl"

perlbrew exec --with perl-5.42.2@default perl later-busy-io.pl
```

No runtime source changed; the full server suite was not rerun for this read-only
investigation. The earlier simplification test result is recorded separately in
[SIMPLIFICATION-2026-09-23.md](SIMPLIFICATION-2026-09-23.md). No merge, push or tag.
