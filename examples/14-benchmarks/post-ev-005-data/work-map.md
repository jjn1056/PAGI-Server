# Performance investigation after upstream EV 0.05

User scope: verify installed adapter fixes busy-I/O starvation, then resume
release/main performance investigation. No additional runtime change authorized
by this investigation step; propose any further simplification from evidence.

| Repository/source | Ticket | Branch/base | Owned changes | Deployment/push |
| --- | --- | --- | --- | --- |
| /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server | Session performance investigation | main / 4625b00, existing release preparation | Read-only benchmark target | None |
| /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/experiment-http-simplification | Same | experiment/http-simplification / 659ce38 | Investigation evidence only | Local, no push |
| Installed PAGI::Server 0.002013 | Same | Released CPAN source, no branch | Read-only benchmark target | None |
| Installed IO::Async::Loop::EV 0.05 | Same | User-installed CPAN release | Read-only verification; shared across all measurements | No patch/override/install |

Preserve all existing uncommitted release preparation and research artifacts.
Do not touch unrelated branches/PR13. Measurements are sequential; no concurrent
test suites or benchmark loads. Profile only previously supported GET/stream
paths; retain the known NYTProf/AsyncAwait POST limitation rather than changing
the workload to make profiling succeed.
