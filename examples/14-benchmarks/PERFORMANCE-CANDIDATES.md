# Deferred performance candidates

The development stream is `experiment/http-simplification`. Prefer removing
work and simplifying ownership over adding caches, flags or parallel state.
These are hypotheses, not promised improvements. Compare any candidate with
both the saved baseline and installed release; keep individual samples.

## Combine response-header scans — deferred

HTTP response start separately scans for Content-Length, Date and Upgrade.
Consider collecting these facts in one pass, without caching or changing the
header representation. Preserve field order, duplicates, case handling,
application header immutability, stripping warnings and framing behavior.
Start with the existing scans; avoid making a general header framework.
Measure a bounded candidate before claiming a gain. Not implemented.

## Share the HTTP send implementation — deferred

Consider a named shared async routine, following the saved receive change.
PR #13's a0d11f81 is a research reference, not a patch to merge. Moving closure
variables into a state hash may introduce extra indirection, so a shorter
factory alone is not proof of improvement. Preserve weak connection ownership,
cancellation, backpressure, HEAD/file/fh/trailers, failure rollback, refusal
state publication and completion before handler return. Avoid adding a
synchronous fast path or another dispatch layer. Not implemented.

## Byte counter — parked

The temporary PR #13 adaptation improved chunked streaming/SSE modestly but
regressed single-body and WebSocket measurements and carried accounting edge
cases. It is outside the saved runtime. See COUNTER-QUIET-2026-09-23.md and
PR13-COUNTER-REVIEW-2026-09-23.md. Do not layer further accounting onto this
experiment without a better correctness/simplicity argument.

## Callback caching — deprioritized

Connection-bound callback reuse is possible, but adds cached state. Prefer the
smaller removal/consolidation opportunities above. No runtime change planned.

## Tried: redundant normal HTTP completion — retained as cleanup

Removed the normal application-return _mark_complete call: successful final
output already completes the scope through _h1_end_scope_output. Keep all
terminal notification behavior and other protocol/error paths. The bounded
experiment and its evidence are recorded in COMPLETION-CLEANUP-2026-09-24.md.

Correctness: 15 files / 253 tests pass. Native comparisons do not demonstrate a performance benefit; the streaming recheck reversed the first round's direction amid substantial sample variation. Keep this as ownership cleanup only; no additional optimization is bundled.

## Statement-profile follow-up — 2026-09-24

The focused GET line profiles found approximately level scope/object creation,
identical Future allocation counts, and one completion call per request. Added
work is chiefly terminal scheduling plus lifecycle adapters and state
publication. No substantial, clearly removable operation was demonstrated.
Header scans and shared-send hoisting remain deferred, without an estimated
win. See STATEMENT-PROFILE-2026-09-24.md. No runtime changes in this pass.

## Header validation boundary — implemented experiment, timing inconclusive

The approved checked-public-wrapper/shared-private-encoder split is implemented
for ordinary HTTP/1 starts and trailers, without flags or caches. Public
serializers retain rejection behavior. Focused checks pass (15 files / 201
tests); independent review found no blockers. A ten-header benchmark was added.

Native timing is mixed: the ten-header mean improved, GET/POST declined, and
streaming varied. The reversed-order POST recheck also has a lower candidate
mean, but its candidate samples span roughly 13%. Load snapshots include paging,
and the user reported heavy fan activity. No sample was excluded and load is
not proven to explain the results. Leave the change uncommitted; pause benchmarks
until a quieter comparison. No general performance benefit established.
See HEADER-ENCODER-2026-09-24.md and its saved evidence. The earlier research is
preserved in HEADER-BOUNDARY-RESEARCH-2026-09-24.md.

## AWS CPAN/current baseline — 2026-09-24

Provisioned an isolated c7a.xlarge and compared installed CPAN 0.002013 directly
with the complete current working tree, as requested. No new runtime changes.
The repeated CPAN POST baseline spread was 2.49%; the comparison observed
GET -14.81%, ten headers -8.32%, POST observer -12.14%, streaming -2.48%,
SSE -0.21%, and WebSocket -1.49% versus release. These Linux figures are not
combined with earlier Mac results. WebSocket may be client-limited.

The larger HTTP differences warrant confidence that a gap remains in this
configuration, but this does not attribute that gap to an individual experiment
or settle the header-encoder comparison against its immediate predecessor.
See AWS-BENCHMARK-2026-09-24.md for validation, individual samples, source
identities, resource observations and instance access/lifecycle instructions.

## AWS retained-checkpoint audit — 2026-09-24

Sixty timed runs compare CPAN, pre-performance 4625b00, saved b995443, completion
cleanup and current header encoding on the same restarted AWS instance. The
saved checkpoint improves all four workloads by 3.4–6.3%, consistently across
three rounds. Completion cleanup is effectively neutral (mean +0.1–0.9%, mixed
round directions), preserving its ownership-cleanup rationale. Header encoding
adds +5.1% for ten-header responses, positive in all three rounds; POST/streaming
effects are small and mixed, and the earlier Mac POST penalty did not repeat
consistently. This supersedes the header experiment's inconclusive Mac-only
performance status above.

Keep the retained changes. Most of the remaining release gap predates this
performance work. No old prototype was revived, no runtime edit was made, and
no further profiling was performed. See AWS-CHECKPOINTS-2026-09-24.md for the
complete release-inclusive table, samples, host checks and limits.

## Canonical checkpoint and focused AWS profile — 2026-09-25

The retained work is committed as 874b120 on experiment/http-simplification.
Fresh targeted checks pass (15 files / 201 tests); independent review found no
blockers. Twelve CPU profiles compare CPAN 0.002013 with that exact commit for
GET and buffered POST. Future allocation counts are identical; each version
constructs one connection state and marks completion once per request. Current
adds one deferred terminal delivery and three state-publisher calls per request,
plus lifecycle checks. No large safely removable operation was established.

A possible bounded simplification is consolidating send-state ownership instead
of copying lexical state through a publisher; it has no demonstrated native
speedup and must preserve refusal delegation, rollback and terminal semantics.
Do not remove callback deferral or add a cache/flag on the strength of the
profile. The split-body POST preflight triggers an AsyncAwait panic under
NYTProf; final POST profiles cover buffered requests only. The uninstrumented
benchmark harness remains unchanged. No runtime edits were made. See
AWS-FOCUSED-PROFILE-2026-09-25.md for evidence and limits. AWS is stopped.

## Tried: HTTP send-state ownership — rejected, 2026-09-25

The scalar-alias candidate removes ordinary HTTP's default state-publisher
callback, while retaining existing refusal translation. Correctness checks and
the full enabled suite passed (180 files / 1,272 tests), but it needs a new
keep-alive scalar-detachment invariant to preserve saved sends. AWS means versus
the saved checkpoint: GET +1.25%, headers -0.02%, POST +0.09%, streaming -1.13%,
SSE +0.05%, WebSocket +0.04%. GET's direction was mixed across rounds; streaming
was slightly lower in all three. No persuasive broad benefit justifies the
lifetime tradeoff. Restore the saved runtime; do not add more mechanisms.

All 18 smoke and 48 timed runs passed. Source/dependency hashes stayed unchanged.
The rejected implementation and regression test survive as an evidence patch,
not active runtime/test changes. See STATE-OWNERSHIP-2026-09-25.md and
state-ownership-data/. AWS was stopped after downloading results.
