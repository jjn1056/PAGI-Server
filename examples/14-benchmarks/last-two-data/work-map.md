# Two independent bounded performance experiments

Repository: /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/experiment-http-simplification
Ticket: session performance investigation, user approved trying both remaining ideas.
Branch: experiment/http-simplification; base efbed54; retained runtime 874b120.
Owned changes: independent header-scan and shared-HTTP-send prototypes, correctness checks, AWS comparison and ledger. Only retain justified changes.
Deployment: existing isolated instance i-077635273be935c19, stop afterwards. No push requested. No other repository or branch in scope.

Header probe: combine only Content-Length/Date/Upgrade discovery into one case-insensitive pass; retain validation, stripping, order, duplicate fields and input immutability.
Send probe: normal wrapper around one shared async routine; private per-send hash holds existing captured state/options. Preserve weak ownership by shifting before weakening the invocant; no extra Future chain, public API, cache, flags or fast path. Retain refusal publisher/rollback unchanged.

Each prototype starts from the same base, snapshots stay under /tmp and on the existing host; no branches. Existing focused behavior tests first. Compare release/base/headers/send in balanced repeated orders, with source hashes and native metrics. Run full suite for a keeper after results; discarded probes do not merit another 17-minute full-suite run. Keep candidate patches and logs either way.
