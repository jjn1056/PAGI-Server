# Listener experiment work map

Repository: /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/experiment-http-simplification
Ticket: session performance investigation; user requests testing PR #13 listener changes for p95/p99 variability.
Branch: experiment/http-simplification; base 2e42135; canonical runtime cdf4a7c.
Source: PR #13 listener commit 7ce3c0be04037fc154aec05c993b935dd7340b53 only. Do not merge the PR or its other optimizations.
Owned changes: private batching Listener, the three existing creation sites, focused regression coverage and benchmark evidence. No other repositories or branches.
Deployment boundary: isolated AWS instance i-077635273be935c19 only; stop after results. No push requested; no PR mutation.

Probe: compare installed release, canonical checkpoint and listener-only candidate. Record p95/p99/max and throughput, repeat in rotated orders. Include steady keep-alive, connection churn and a separate connection-storm first-response distribution; a few hundred slow connects disappear from all-request p99 in a long keep-alive run. Preserve normal POST/stream/SSE/WS controls. No load-generator or dependency upgrades during measurements.

Correctness: reproduce stock one-accept behavior and check bounded drain on real sockets; preserve nonblocking sockets, error delivery, TLS routing, inherited/Unix sockets and multiworker behavior. Focused checks first; full suite only for a viable keeper. Record review concerns and stop expanding scope if adoption needs architectural changes.

User steering: 64 is arbitrary and may deserve a tunable setting. First compare 64; if batching is useful, probe a small batch-size range before recommending an option/default. No public setting added during the first comparison.

Adaptation: original PR batching loop continued after callback pause/close/removal. Real-socket regressions fail against it; candidate adds a post-callback handle/readiness/loop guard. Version aligned with checkpoint; POD avoids presenting historical accept rates as universal.

Measurement setup correction: first smoke used 500 connections and a 3-second deadline; release timed out. Preserve that attempt, reduce only smoke storm to 50 clients; timed storm remains 500 with 30-second background load and 27-second safety bound.

Batch-size follow-up: after primary runs (never concurrently), compare release,
saved, 16, 64, 256 on the same 500-new-connection/100-existing-client workload.
Three rotated rounds, 12-second background windows. This is separate evidence
from the primary 30-second windows; do not pool background percentiles. Only
ACCEPT_BATCH changes between listener variants. No public tuning API yet.
