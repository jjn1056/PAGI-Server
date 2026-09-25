# Read-only buffer follow-up

Repository: /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server
Branch: experiment/http-simplification; ticket PR #23 performance investigation.
Git base 0b519a1; executable runtime matches 5c960b2.
Owned changes: compare only read_len/write_len defaults on AWS snapshots. No local runtime changes while user tests. No new branch/worktree.
Deployment: isolated AWS i-077635273be935c19; stop after results.
Push target: origin/experiment/http-simplification; no push during experiment.

User approved separating larger reads from writes and requested a final findings table. Rerun release, before, both64 and read64 together for 1MiB POST, single-send64KiB download, mixed bulk/small traffic, GET500 and GET25. Three rotated rounds for primary workloads, two GET25. Eight read64 smoke workloads check uploads/downloads/small traffic/SSE/WS. Reuse unchanged main comparison harness/dependencies/CPU placement. No per-loop workaround or new public option.
