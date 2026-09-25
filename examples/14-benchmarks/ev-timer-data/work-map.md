# EV zero-delay timer experiment

User-approved scope: test the single idle-to-timer substitution in isolation.
No installed dependencies, PAGI runtime, release files, pushes or tags change.

| Repository / source | Ticket | Branch / base | Owned changes | Deployment / push |
| --- | --- | --- | --- | --- |
| PAGI-Server /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/experiment-http-simplification | Session performance investigation | experiment/http-simplification / 659ce38 | Diagnostic evidence only | Local only; none |
| PAGI-Server /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server | Same | main / 4625b00 with existing release preparation | Read-only comparison | None |
| IO-Async-Loop-EV temporary source under /tmp/pagi-ev-timer-20260923 | Same; no upstream ticket | No git branch; CPAN 0.04 | One-line candidate change and regression probe; pristine source retained | PERL5LIB override for named child processes only; no installation or publication |

Hypothesis: EV idle scheduling causes the busy-I/O callback backlog; a one-shot
zero-delay EV timer delivers deferred work under load while preserving relevant
scheduling and cancellation behavior. Test existing adapter suite and standalone
probes first; then measure PAGI callback delay and selected workloads. Distinguish
starvation recovery from exact round ordering and from throughput improvement.
