# Request timing experiment — 2026-09-25

Repository: /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server
Ticket: PR #23 performance investigation; user approved trying conditional request timing.
Branch: experiment/http-simplification; base 040731c; PR base main 4625b00.
Owned changes: one conditional request timestamp, access-log regression coverage, benchmark evidence.
Deployment boundary: disposable source snapshots on dedicated AWS i-077635273be935c19 in us-east-1; no Larp or Tools changes. Stop instance after results.
Push target: origin/experiment/http-simplification; no push, merge or release in this experiment.

Question: does avoiding gettimeofday and its array when access logging is disabled improve real workloads without altering logged durations?
Probe: confirm timestamp only serves access logging, test two requests with logging on/off, then compare release 0.002013, exact saved 040731c, and candidate using three rotated AWS rounds. Hold read/write sizes, loops, workers, affinity and load fixed between saved/candidate. GET25, POST observer, streaming, SSE and WebSocket controls; GET500 checks concurrency. No autoflush or callback changes.
Candidate remains experimental until evaluated; preserve the exact patch and all samples. User prefers simpler code and no benchmark-specific hacks. No new branch/worktree.

Outcome: neutral; candidate runtime and test restored to 040731c after exact-patch check. Local 6 files/46 tests PASS; AWS logging 1 file/14 PASS; 18 smoke + 48 timed runs complete. All source/dependency manifests remained unchanged during runs. AWS stop verified 2026-09-25. Evidence-only commit on existing branch; no push.
