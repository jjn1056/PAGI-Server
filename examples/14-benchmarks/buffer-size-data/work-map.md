# Buffer size experiment

Repository: /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server
Ticket: session performance investigation; PR #23.
Branch: experiment/http-simplification; base 5c960b27522214c99da3e9b11e628f55ade3f3ac; base branch main.
Owned changes: compare existing 8192-byte read/write lengths with 65536 only. No autoflush, read_all/write_all, logging, drain callback or other runtime change.
Deployment boundary: source snapshots on isolated AWS i-077635273be935c19. Stop after results.
Push target: origin/experiment/http-simplification, but no push until the experiment is evaluated.
Local checkout stays unchanged during user manual testing. No new git branch/worktree.

Use installed CPAN release, before, buffers64. Server CPU0, clients CPUs2-3, driver CPU1. Same Perl/dependencies/EV/Future settings as prior AWS runs. Rotated orders; primary cases three rounds, controls two; preserve every result and host telemetry. Test 1MiB upload, 64KiB single/chunked download, GET at 500 connections, mixed bulk/small traffic, and small GET/POST/SSE/WebSocket controls. Compare median per-run rates and p95/p99, not pooled percentiles. Bulk gains must not come at substantial small-request latency cost.
