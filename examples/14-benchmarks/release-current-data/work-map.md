# Combined release/current comparison

User requested one table showing the released Server against all current performance changes together. Fresh paired measurements; no runtime edits.

| Source | Ticket | Branch/base | Owned work | Push/deploy |
| --- | --- | --- | --- | --- |
| /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/experiment-http-simplification | Session performance investigation | experiment/http-simplification / 659ce38 plus shared receive routine | Evidence only | Local, none |
| Installed PAGI::Server 0.002013 | Same | CPAN release, no branch | Read-only baseline | None |
| Installed IO::Async::Loop::EV 0.05 | Same | CPAN dependency | Same unchanged dependency for both versions | None |

Current includes all existing scalar-storage/send-tail changes and the shared HTTP receive routine. No additional Server branches or code changes. Both sides use Perl 5.42.2@default, production, pure-Perl Futures, EV 0.05, one worker, 25 HTTP clients / 20 persistent WebSocket clients. Each workload is release/current/current/release, 15 seconds measured per sample plus existing warmup/preflight. Workloads: GET, small POST with observer, single 64 KiB send, 64-chunk stream with/without observer, SSE, WebSocket. Sequential, no concurrent tests/profiles.
