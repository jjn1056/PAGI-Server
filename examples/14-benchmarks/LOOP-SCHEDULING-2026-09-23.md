# Deferred callback scheduling research — 2026-09-23

Research only. No server, spec, installed dependency, or adapter runtime changes.

## Finding

PAGI's use of `$loop->later($code)` matches IO::Async's documented contract.
IO::Async::Loop::EV 0.04 implements that operation with a default-priority native
EV idle watcher. Native idle scheduling can postpone delivery while I/O stays
ready. Poll and Select drain deferred work after processing each I/O round.

This is evidence for a bug in the adapter's implementation of an existing
contract, rather than a need for a PAGI-specific scheduling API.

## Reproduction

The adjacent `loop-scheduling-data/scheduling-probe.pl` runs without PAGI or
NYTProf. A pipe remains readable for five explicit loop iterations. It checks a
callback queued before looping, one queued inside an I/O callback, a nested
deferral, cancellation, and the Future-returning form of `later`. It then removes
the I/O watch and runs two more iterations.

Run each backend in a separate process:

```sh
perlbrew exec --with perl-5.42.2@default perl examples/14-benchmarks/loop-scheduling-data/scheduling-probe.pl Poll
perlbrew exec --with perl-5.42.2@default perl examples/14-benchmarks/loop-scheduling-data/scheduling-probe.pl Select
perlbrew exec --with perl-5.42.2@default perl examples/14-benchmarks/loop-scheduling-data/scheduling-probe.pl EV
```

Environment: Perl 5.42.2@default, IO::Async 0.805, IO::Async::Loop::EV 0.04,
EV 4.37. JSON observations are preserved beside the script.

| Observation | Poll / Select | EV |
| --- | --- | --- |
| Initially queued and I/O-queued callback | Round 1, after I/O | Round 6, after readable activity removed |
| Nested callback | Round 2 | Round 7 |
| Future ready during busy I/O | Yes | No |
| Cancelled callback invoked | No | No |
| All remaining work eventually delivered | Yes | Yes |

The finite probe demonstrates postponement across busy iterations, not an
observed infinite delay. Callback ordering between unrelated registrations is
not treated as a separate bug. Using the Future-returning form does not avoid
the scheduling problem.

## Contract and source evidence

- [IO::Async::Loop later and watch_idle](https://metacpan.org/pod/IO::Async::Loop#watch_idle)
  describe delivery after the current I/O round, no intervening blocking wait,
  and nested deferrals waiting for another round.
- [EV idle watchers](https://metacpan.org/pod/EV#IDLE-WATCHERS---when-you've-got-nothing-better-to-do...)
  wait for the absence of pending watchers at equal or higher priority.
- Core Loop's `_adjust_timeout` prevents blocking while deferrals are pending;
  `_manage_queues` drains a snapshot. Poll and Select invoke that drain after I/O.
- EV 0.04 overrides `watch_idle` with one native idle watcher per registration.
  Its `loop_once` calls `EV::run(EV::RUN_ONCE)` without the core queue drain.
- The installed EV adapter source is byte-for-byte identical to
  [CPAN release 0.04](https://metacpan.org/dist/IO-Async-Loop-EV), the latest release
  returned by MetaCPAN on the research date.
- Its idle test invokes the shared IO::Async::LoopTests idle group. That group
  covers ordinary delivery, nested deferral, cancellation and a future timer,
  but not continuously ready I/O.
- The author's Bazaar branch last-revision endpoint reports revision 33 dated
  2026-04-26. No existing fix or relevant report was verified. The historical RT
  tracker could not be inspected successfully; absence of a known report here
  is not evidence that none exists.

## Recommended boundary

Keep PAGI's portable `later` call. Prepare an upstream adapter report and
regression test for busy-I/O delivery, then test an adapter correction locally.
Do not add EV detection, watcher priorities, or a separate scheduler to PAGI.

The correction needs to preserve non-inline delivery, progress during busy I/O,
nested deferrals waiting for the next round, cancellation and watcher cleanup.
Assess compatibility with native EV watchers and with callers driving EV
directly before choosing a queue-drain design. The adapter already tests sharing
native EV watchers, but those tests drive the loop through IO::Async.

A queue drained at the end of each I/O round is the conceptual model, not a
finished patch. Merely draining from `loop_once` needs the direct-EV-driving
review above. A plain EV check watcher is not an automatic substitute: its
documented callback phase precedes normal event callbacks. Priority changes and
zero-delay timers likewise require ordering analysis rather than assumption.

No new PAGI specification text is needed for this finding. No upstream issue,
message or patch has been posted.

## Performance limit

The prior profiling investigation found 10,000 queued deliveries and seconds
of delay in real PAGI traffic, with increased process memory. This research
isolates the scheduling mechanism; it does not quantify its share of the
original throughput regression.

After a separately reviewed adapter fix, repeat the callback-lag probe and the
existing GET, POST and streaming benchmarks. Report throughput, CPU, peak RSS and
callback delay separately. Do not promise recovery of the original 20% loss.

## Work map

- PAGI-Server evidence only:
  /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/experiment-http-simplification
- Branch: experiment/http-simplification; research starting commit: 659ce38.
- Ticket: session performance investigation; no external ticket assigned.
- Owned changes: this note, standalone diagnostic and recorded observations.
- Deployment boundary: no deployed/runtime change. Push target: none.
- Root main/release preparation, other Server branches, PAGI spec and Tools untouched.
- IO::Async and IO::Async::Loop::EV: installed/released sources read-only;
  no implementation checkout or upstream branch created.

