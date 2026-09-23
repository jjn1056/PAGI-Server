# NYTProf investigation work map

- Repository: /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server
- Ticket: user-approved release-to-main performance investigation; no issue number.
- Evidence branch: experiment/http-simplification, HEAD 148a01f; forked from main 4625b00.
- Worktree: /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/experiment-http-simplification
- Compared implementations: installed 0.002013; root main 4625b00 with existing version-only lib changes; experiment only if evidence warrants.
- Owned changes: investigation scripts, saved profile summaries and report. No runtime changes.
- Deployment boundary: local profiling only; root main/release preparation and unrelated PR13 untouched.
- Push target: none. No merge, push, release or tag.
- Method: one-worker GET, POST with completion callback, 64-send stream; equal fixed request counts and identical apps/env; subroutine CPU profiles first, line profiles only where needed; separate unprofiled throughput.
- Deliverable: evidence-backed account of additional work and at most two justified next candidates, with profiler limitations stated.
