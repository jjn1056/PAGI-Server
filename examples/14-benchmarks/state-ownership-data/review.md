# Independent review

Reviewer: state_owner_review; read-only, no edits or tests run by reviewer.

Verdict: no blocking regression established.

Ordinary HTTP has one scalar. Its reference does not retain its connection.
Detaching before keep-alive reset preserves saved sends; examined ordinary
HTTP reuse paths pass through this reset. Rollbacks update the same state
readers observe. Refusals preserve their private state and existing translated
publication, including during receive/file suspension. Completion ordering and
reentrant cancellation remain unchanged.

Simplicity: modest improvement in ownership with a maintenance tradeoff: the
scalar-detachment invariant. No further isolation mechanism was found necessary.

Pre-existing issue, not fixed here: a saved SSE send permits repeat sse.close
after reuse and overwrites h1_seq. Baseline already emits an extra terminator
and completes the wrong scope. Candidate additionally corrupts the new HTTP
validator state, but reviewer found no previously intact valid execution newly
broken. Do not expand this experiment into general stale-SSE-send handling.

Not judged: benchmarks, full suite, or general correctness when applications
return with unfinished sends. Those last lifetime problems are outside this
bounded equivalence experiment; the existing reentrant cancellation test is run.
