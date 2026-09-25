# Focused GET statement profiling — 2026-09-24

The new statement profiles do not identify a substantial, clearly removable
piece of request lifecycle work. Object construction is approximately level
with release in these samples. The new work is concentrated in terminal
notification scheduling, abort-hook construction, receive lifecycle callbacks,
and send-state publication/checking. This supports a distributed lifecycle
cost explanation, not a new demonstrated runtime defect. It does not allocate
the native throughput regression precisely among those operations.

No runtime or test code changed during this investigation. Current includes
the previously tested, uncommitted normal HTTP completion cleanup. Exact hashes
are recorded in [statement-profile-data](statement-profile-data/).

## Method and limits

- Installed PAGI::Server 0.002013 versus experiment/http-simplification at
  07a03b8 plus the existing completion cleanup; no new branch.
- Both use Perl 5.42.2@default, EV adapter 0.05, pure-Perl Futures,
  LIBEV_FLAGS=8, one production worker and the same GET benchmark app.
- Statement smoke tests: 100 measured GETs, 200 warmup, 3 preflight, 5 clients
  for each version. Both completed and yielded readable profiles.
- Main: release/current/current/release; 2,000 measured GETs plus 200 warmup
  and 3 preflight requests per fresh process, 25 clients. All four completed:
  8,812 successful requests including warmup/preflight. Runtime source hashes
  remained unchanged. Smoke results are excluded from the comparisons below.
- NYTProf: stmts=1:calls=0:slowops=0:compress=1:start=init. The prior crashing
  POST workload was not retried. No streaming-buffer experiment was reopened.
- These are instrumented elapsed-time profiles, not native performance or CPU
  measurements. Startup is captured too; the compared request routines have
  exactly 2,203 invocations per profile, except where explicitly stated.
  Differences of tiny routines are especially vulnerable to instrumentation.

Context7 located the upstream NYTProf project but returned only installation
material for the requested extraction API. The extractor was therefore checked
against the installed NYTProf Data/FileInfo source and POD. FileInfo's
line_time_data and sub_call_lines supply statement records and separate called-
subroutine records. The installed NYTProf interpretation notes explain that
line times can include call/return attribution artifacts and need not sum to
subroutine exclusive time. Several statements on one source line may count
more than once per invocation. For example, the one-line response-start marker
has twice as many statement executions as actual subroutine calls; it is NOT
a duplicate lifecycle call.

## Stable operation counts

Counts per GET, except total Future allocations. Both samples agree.

| Operation | Release | Current |
| --- | ---: | ---: |
| ConnectionState construction | 1 | 1 |
| TransportState construction | 1 | 1 |
| Complete transition call | 1 | 1 |
| Response-start marker call | 1 | 1 |
| Abort-hook construction | 0 | 1 |
| Terminal delivery / loop scheduling | 0 | 1 |
| HTTP state publication callback | 0 | 3 |
| Clean-output predicate | 0 | 2 |
| Future::PP::new, total per profile | 8,878 | 8,878 |

The single current completion call comes from _h1_end_scope_output, whereas
release calls it from the handler-return path. This verifies the earlier
cleanup eliminated the repeated normal HTTP call. There is no increase in
Future allocations in this workload.

## Timing signals

Mean exclusive profiled microseconds per request across two samples. These
numbers locate work; they are not native savings estimates. Do not sum them
with statement times or infer a predicted throughput gain.

| Routine | Release | Current |
| --- | ---: | ---: |
| Scope factory | 11.10 | 11.06 |
| Connection-state constructor | 4.72 | 4.43 |
| Transport-state constructor | 3.88 | 3.36 |
| Receive factory | 3.01 | 4.47 |
| Send factory | 4.88 | 6.35 |
| Abort-hook factory | 0.00 | 2.16 |
| Complete transition | 2.17 | 3.29 |
| Terminal delivery helper | 0.00 | 3.20 |
| Loop scheduling | 0.00 | 1.66 |
| EV scheduling adapter | 0.00 | 5.11 |
| Clean-output predicate | 0.00 | 2.51 |
| Send-state publication callback | 0.00 | 2.21 |

## What the lines and source reveal

Scope creation and ConnectionState/TransportState constructors show no new
large regression here. The scope literal and ordinary object construction are
also prominent in release. The fresh abort hook is additional, but remains a
small part of this instrumented request; it is not evidence for adding a cache.

The receive factory adds a per-scope disconnect cap and two callbacks. They
implement bounded repeated receives after disconnect and clean-end detection.
A GET does not exercise body reading, so these profiles cannot judge the cost
of the shared receive coroutine. Eliminating the helpers would require placing
the same state and decisions elsewhere; this pass did not identify a smaller
replacement. No receive opt-out is proposed.

The send factory's default publication callback is newly allocated and called
at initialization and after each of this GET's two events. Closure-local
sequence state is published to the scope owner so app-return handling sees the
outcome. SSE/WebSocket refusal owners translate that state, and file/fh failure
paths republish rollback. Removing the publisher is therefore an ownership
change, not a safe deletion. The async closure's source line itself is also
executed by sends; its line time must not be mistaken for pure construction
cost. These results do not establish shared-send hoisting as the next win.

The clearest new group is terminal delivery. Release walks completion callbacks
inline. Current marks terminal facts immediately, creates the delivery closure,
and schedules it through loop->later. That preserves callbacks outside the
application's send stack while exposing completion before a parked receive
resumes. The EV adapter method is still named watch_idle, but installed 0.05
uses the fixed zero-delay timer; this is not the old starvation bug.

The delivery closure also resolves the optional end Future, invokes the two
callback families, clears callback references and releases the abort hook.
Those cleanup responsibilities still exist for the GET with no observers.
Dropping deferred delivery outright changes the contract. Skipping delivery
for selected scopes or adding a batching scheduler would be new conditional
machinery, outside the requested simplification approach.

## Recommendation

No further runtime change is justified by this pass alone. The added lifecycle
behavior explains concrete extra work, but there is no demonstrated large
unnecessary operation to remove. Keep the header-scan and shared-send ideas
in the ledger as unproven possibilities, not the next promised speedup. Avoid
another tiny deletion/benchmark cycle based solely on profiler microseconds.

If a larger performance effort is resumed, it should have an explicit native
workload target and a simpler ownership design to test. This investigation
does not recommend weakening the contract, adding callback caches, disabling
receive work or reviving the parked buffer counter.

## Evidence

The data directory preserves drivers, extractor, work map, metadata, hashes,
raw load-client output, subroutine profiles, relevant per-line/source records
and the summary. Raw binary profiles and complete server logs remain in
/tmp/pagi-statement-profile-20260924. Root Server main release preparation and
other repositories were untouched. No full suite was rerun because runtime
code was unchanged. Research evidence is left uncommitted and unpushed.
