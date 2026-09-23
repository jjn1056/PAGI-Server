# HTTP Simplification Experiment Implementation Plan

> **For agentic workers:** Use superpowers:subagent-driven-development or executing-plans to implement task-by-task. Keep all experiment changes on experiment/http-simplification.

**Goal:** Test whether simpler HTTP implementation can recover performance without behavior changes or feature switches.

**Architecture:** Keep the current public API, state transitions, validation and Future ownership. Remove scalar-reference indirection; build the HTTP receive coroutine once per request; use one successful exit from the HTTP/1 send coroutine instead of a per-event on_done wrapper. Do not redesign state machines or observer delivery.

**Tech Stack:** Perl 5.42.2@default, Future::AsyncAwait, Future::PP, IO::Async/EV, prove, Python benchmark runner and hey.

**Spec:** Approved conversation design: the three simplifications described on 2026-09-22; existing PAGI contract and repository tests remain authoritative. This is behavior-preserving refactoring, not a spec change.

## Global Constraints

- Test branch only; root main release preparation and benchmark files remain intact.
- No observer-disabling knobs, new callback hooks, new dependencies, or skipped compliance checks.
- No changes to deferred callbacks, late registrations, pending receives, failures/cancellation, HEAD, file/fh, trailers, or refusal behavior.
- Preserve per-scope ownership and terminal timing; transport connection reuse must not change facts on prior scopes.
- No changes to HTTP/2 send structure in this first experiment; shared ConnectionState changes must pass both transports' tests.
- Do not remove receive Future tracking, duplicate app-return completion marks, or state publishers in this experiment. They require separate ownership research.
- Existing behavioral tests are the refactor contract. Add tests only for uncovered observable cases, never assertions on representation or callback counts internal to the implementation.
- Three independently reviewable runtime commits. No merge, push, release or tag.
- Benchmarks run without concurrent test suites/load generators. Preserve all samples and report uncertainty.

## Work map

Repository /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server; worktree .worktrees/experiment-http-simplification; branch experiment/http-simplification; base main 4625b006aa136844ab80f9163bcffc25db748cb3. No ticket number. Local-only experiment. Root main's version-only .pm edits are not copied; source paths/commits identify variants, not the version banner.

## Task 1: Simplify the request path in three reviewable commits

**Files:** lib/PAGI/Server/ConnectionState.pm; lib/PAGI/Server/Connection.pm. Add targeted tests only if existing coverage cannot observe a changed path.
**Consumes:** Existing ConnectionState API and three-argument PAGI receive/send callbacks.
**Produces:** Identical API/behavior with fewer allocations/wrappers.

- [x] Verify unchanged baseline: `perlbrew exec --with perl-5.42.2@default prove -lr -j4 t` (save log). Investigate failures before attributing them to refactoring.
- [x] Replace `_connected => \\$connected` / `_reason => \\$reason` with ordinary scalar fields; replace all `${$self->{_connected}}` / `${$self->{_reason}}` accesses; remove now-unused lexical variables. Confirm no references escape the class with `rg '_connected|_reason' lib t`.
- [x] Run `prove -l t/37-connection-state.t t/connection-state-response-started.t t/69-connection-state-protocol-scopes.t t/84-terminal-callback-deferral.t t/85-terminal-callback-graceful-shutdown.t t/http2/30-connection-state.t` under the same perlbrew environment; commit only this change.
- [x] Hoist the inner HTTP async receive function out of the returned wrapper:

```perl
my $receive_body = async sub { ...existing receive implementation... };
return sub {
    ...existing early guards...
    my $future = $receive_body->();
    ...existing Future tracking and pruning...
    return $future;
};
```

The body keeps call-local `$parked` and response variables. Keep weak connection capture and all existing tracking. Do not touch SSE/WebSocket receive in this experiment.
- [x] Run `prove -l t/03-request-body.t t/75-max-disconnect-receives.t t/76-h1-receive-after-disconnect.t t/77-receive-after-clean-end.t t/78-response-before-request-body.t t/79-body-limit-after-response-started.t t/80-unread-body-keepalive.t`; commit only this change.
- [x] Return the HTTP/1 send async function directly and move its current scope-completion check to its successful common tail. Reorganize HEAD success branches so they reach that tail. Keep transport-gone guards as no-op exits, file/fh rollback and exceptions, state publishing and terminal predicate. Audit every `return` in the function; success paths including HEAD/trailers/refusals must reach the tail, failures must not.
- [x] Run `prove -l t/01-hello-http.t t/10-http-compliance.t t/42-file-response.t t/53-trailers-framing.t t/71-http-refusal-on-protocol-scopes.t t/77-receive-after-clean-end.t t/78-response-before-request-body.t t/http-incomplete-response.t t/84-terminal-callback-deferral.t t/85-terminal-callback-graceful-shutdown.t`; inspect backpressure and failed file tests, adding an observable regression test only if needed; commit separately.
- [x] Review cumulative diff for behavioral changes or added machinery. Run final `prove -lr -j4 t` and `git diff --check`. Independent review must assess early returns, timing, lifetime and failures. If inlining cannot stay small and correct, retain the wrapper and report that limitation rather than inventing new state.

## Task 2: Preserve examples and make the runner compare checkouts

**Files:** examples/14-benchmarks/, examples/README.md. Copy the approved files from root main without changing the workload apps. Only the test-branch runner changes.
**Consumes:** Existing runner baseline (installed release versus checkout), exactly the same app bytes.
**Produces:** Explicit main-versus-candidate comparison with reproducible metadata.

- [x] Copy and commit existing examples/index as a baseline artifact, preserving app hashes and earlier baseline samples.
- [x] Add optional `--baseline-repo PATH` so the baseline slot launches that checkout's `perl -Ilib bin/pagi-server`; retain installed-release comparison by default. Label rows and summary as baseline/candidate when used, record actual module paths and git heads for both, and hash both library trees. Update existing report scripts without silently mixing older runs.
- [x] Exercise runner smoke checks and summary-isolation tests against baseline main and candidate. The measured HTTP/SSE payload checks and all WebSocket echo validations must pass before load.

## Task 3: Benchmark and report

**Files:** examples/14-benchmarks/SIMPLIFICATION-2026-09-23.md and new comparison data; raw artifacts in ignored local/benchmarks/.
**Consumes:** Green runtime branch and unchanged root main; no tests running concurrently.
**Produces:** Honest measured comparison, no speculative performance claim.

- [x] Run baseline→candidate→candidate→baseline for all 10 existing workload variants, 10 seconds each with one-second warmup: one worker/50 HTTP clients then 16 workers/500 HTTP clients. WebSocket uses 20 persistent connections.
- [x] Include means, individual samples, p50/p99 latencies, source revisions, environment and commands. Compare with previous installed-release baseline only as context, not as a fresh paired measurement.
- [x] If gains are small/noisy or a regression appears, rerun the affected HTTP comparisons for 30 seconds in alternating order before concluding. Do not optimize further during measurement.
- [x] Record results, net code changes, test totals and review findings. Keep branch/worktree for user review; do not merge, push, or tag.

## Execution outcome

Completed on experiment/http-simplification. Baseline full suite: 176 files /
1,249 tests; candidate: 177 files / 1,263 tests. Runtime commits d5fe7ee,
f77e69a and ac30cc8 remove 18 net lines. Independent reviews found no substantive
correctness or scope issue. The send change permits receive resumption before
send-Future readiness, an ordering allowed by the current spec; the new regression
covers reentrant cancellation while preserving clean terminal facts and deferred
callbacks. Thus "identical behavior" above means the public contract, not identical
incidental Future readiness inside resumed receive code.

All 100 measured samples completed. Longer comparisons are mixed, with the
clearest positive result in repeated sends (+6.2%). This does not establish
recovery of the release regression. See
[the measured report](../../../examples/14-benchmarks/SIMPLIFICATION-2026-09-23.md)
for all samples, latency, limitations and commands. Branch remains local; no
merge, push or tag. Root main release work remains intact.
