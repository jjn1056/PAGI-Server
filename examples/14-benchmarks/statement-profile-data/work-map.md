# Statement profiling work map

- Repository: /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/experiment-http-simplification
- Branch/base: experiment/http-simplification at 07a03b8 plus the already-reviewed, uncommitted normal HTTP completion cleanup. Exact source hashes captured separately.
- Ticket: session performance research, no external ticket.
- Owned work: GET statement profiles, analysis and evidence only. No runtime or test edits.
- Comparison: installed release 0.002013 and current checkout; both EV 0.05 and PP Futures.
- Deployment boundary / push target: local research only, no merge, tag or push.
- Root Server main release preparation, PAGI spec and PAGI-Tools remain outside scope.

Smoke: 100 measured GETs, 200 warmup, 3 preflight, 5 clients, each version. Both passed.
Main: 2000 measured GETs, 200 warmup, 3 preflight, 25 clients; release/current/current/release. Statement timings locate work and are not unprofiled throughput evidence. Analyze exact counts and source ownership before suggesting any change. Do not retry the previously crashing POST profile or reopen the parked byte-counter experiment.
