# HTTP/1 header encoder experiment

- Repository: /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/experiment-http-simplification
- Ticket: session performance investigation, no external ticket.
- Branch/base: experiment/http-simplification at 07a03b8 plus existing uncommitted normal HTTP completion cleanup and research documents.
- Owned changes: checked public serializers sharing private encoding with already-validated ordinary HTTP/1 starts/trailers; focused boundary tests; ten-header benchmark app; evidence and ledger.
- Baseline: /tmp/pagi-header-encoder-20260924/before (plain lib/bin snapshot, not a branch).
- Excluded: HTTP/2, manual WebSocket accept, refusal-dispatch validation, header-scan consolidation, validation flags/caches, spec edits.
- Deployment/push: local experiment only, no commit, merge, tag, dependencies or push requested.
- Root Server main release preparation, PAGI-Tools and PAGI spec are untouched.

Verification: establish boundary tests on baseline; preserve direct serializer checks and fail-before-mutation/retry on app sends; test injection, validation, HEAD, framing, errors and refusal callers. Benchmark release/baseline/candidate sequentially with GET, ten headers, POST/observer and streaming/observer. Source hashes identify variants; report native timing as provisional when noisy.
