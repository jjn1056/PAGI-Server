# Independent final runtime review

Reviewed candidate `ac30cc8` against `87e5714`, requirements in task-1-brief.md, implementation report, cumulative runtime diff, affected surrounding lifetime/teardown/refusal code, and PAGI::Spec::Www callback/resumption contract. Benchmark harness commit c00219c was independently reviewed elsewhere and is not reassessed here. Read-only review: no tests, benchmarks, or load were launched while the controller's full suite ran.

## Findings

No substantive correctness or scope findings in the three runtime simplifications. Ready for benchmark evaluation once the controller's full suite passes; this is not a merge recommendation or a performance claim.

- ConnectionState private field accesses remain confined to ConnectionState. Scalar replacement consistently preserves reads, transitions, idempotence, late registration, and deferred delivery. Observer isolation is unchanged.
- Hoisted receive body retains weak connection capture and all per-call parked/body/result locals. Request state remains intentionally scope-shared. Wrapper guards, Future tracking, and pruning are preserved. No new strong connection capture, retained call-local state, or cancellation ownership was introduced.
- HTTP/1 send's common tail preserves terminal predicate, state publication, and mark-before-wake order. HEAD body flushes headers and reaches the tail; HEAD trailers still advance then discard. Ordinary body, file/fh success, trailers, and delegated SSE/WebSocket refusals reach the tail. Start/fullflush do not become terminal merely by reaching it.
- Validation and file/fh/trailer errors unwind before the tail; pre-existing rollback and publication remain intact. Missing/closed connection guards intentionally bypass the tail; disconnect teardown owns the terminal state before settling drain waiters. File helper outcomes still reach the same idempotent terminal helper as before.
- The change does alter readiness ordering: a pending receiver can resume before the terminal send Future becomes ready, and can cancel it reentrantly. The spec explicitly permits receive resumption inside the send before its Future resolves. Terminal facts precede that wake and callbacks remain deferred. The added regression test correctly accepts either done/cancelled readiness and checks shared guarantees instead of requiring the previous implementation order. Its old/new passing results are implementation-report evidence, not reruns by this reviewer.

## Remaining gates

Controller must confirm the full-suite result and run comparable baseline/candidate benchmarks before drawing an integration or performance conclusion. Existing focused pass counts are recorded by the implementer; this review makes no independent test-pass claim. No additional production machinery, hooks, knobs, H2 send refactor, or observer fast path was added.
