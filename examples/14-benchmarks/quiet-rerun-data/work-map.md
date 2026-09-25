# Quiet-machine benchmark rerun

User requested another streaming/WebSocket comparison while away from the computer, and asked whether installed Loop::EV 0.05 helps through lower memory usage. No runtime implementation changes. One existing development branch.

| Source | Ticket | Branch/base | Owned work | Push/deploy boundary |
| --- | --- | --- | --- | --- |
| /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/experiment-http-simplification | Session performance investigation | experiment/http-simplification, 659ce38 plus uncommitted shared receive routine | Benchmark evidence only | Local; no push/deploy |
| /tmp/pagi-shared-receive-20260923/before | Same | Frozen lib/bin snapshot of 659ce38, not a branch | Read-only baseline | None |
| Installed IO::Async::Loop::EV 0.05 | Same | CPAN install | Read-only | No install/downgrade |
| /tmp/pagi-ev-timer-20260923/pristine/lib | Same | Pristine adapter 0.04, not a Server branch | Process-local server override for adapter comparison | None |

Phase 1: before/after/after/before, 20 seconds measured per sample, single response vs 64-chunk stream, stream with observer, SSE and WebSocket. Both sides installed 0.05.
Phase 2: current Server held fixed; 0.04/0.05/0.05/0.04, 10 seconds per sample, GET with observer, stream with observer, SSE and WebSocket. WebSocket client always installed 0.05.
Both: one EV worker, production, pure-Perl Futures, 25 HTTP clients / 20 persistent WS clients. Sequential measurements, no profiling or tests alongside load. Capture source/dependency hashes, complete client/server output, server process CPU and peak RSS. Retain every sample.

The copied harness adds only /usr/bin/time measurement and a separate server environment; workload/apps/preflight/client logic are unchanged. Main and Tools runtime sources remain untouched.
