# PR13 byte-accounting review

- Owned repository: /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/experiment-http-simplification
- Task: review and test PR13's byte accounting; no external ticket.
- Branch/base: experiment/http-simplification at b9a03bb; runtime remains b995443.
- Read-only source: PR13 commit b8dd6ee, preceding autoflush changes, installed IO::Async 0.805.
- Scratch snapshot: /tmp/pagi-pr13-counter-review-20260923/original, exported files only, not another branch.
- Owned changes: review evidence only until an implementation decision.
- Deployment boundary: no merge/cherry-pick/dependency replacement/push/deployment.
- Push target: none for this research. Existing branch only for saved evidence.
- Tools, spec, and Server main release prep remain untouched.

- Diagnostic scope extension: candidate/ is a temporary copy of the saved runtime with only PR13 byte accounting adapted at 28 write sites; not a branch or shippable candidate. Original close/reset limitations deliberately retained to keep the timing experiment narrow. Native benchmark compares this copy against the unchanged saved branch.
