# PR13 byte-accounting review and diagnostic comparison

PR13 already contains an implementation of constant-time outbound byte
measurement, separate from its custom Listener. This was missed in the earlier
write-buffer research. Commit `b8dd6ee` routes writes through `_stream_write`,
adds their byte lengths to a connection counter, and subtracts successful writes
through one callback shared by that connection. The callback is passed to every
write, so it still prevents IO::Async from combining adjacent writers.

The original integration test passes. The design is a useful starting point,
but it is not ready for an unreviewed cherry-pick into the current server.
Close handling needs reconciliation, and this session's benchmarks do not
establish a performance improvement. The user reported likely concurrent host
activity after seeing the variable results; further benchmarking is paused.

## Scope and exact sources

- Existing branch: `experiment/http-simplification`, base b9a03bb; saved runtime
  remains b995443. No runtime files were changed in the working branch.
- PR13 implementation: b8dd6ee, exported under `/tmp` for inspection and tests.
  This export is not another development branch.
- Diagnostic candidate: copy of current runtime with only the counter adapted
  from b8dd6ee. Twenty-eight byte-string write sites now call `_stream_write`.
  The old counter getter, shared callback, and close-time reset are retained.
- No PR13 autoflush, buffer-length, Listener, dispatch or other optimization
  was included. The known close/rejection limitations are deliberately retained
  in this disposable candidate; it is not shippable.
- All sources stayed unchanged during the benchmark, checked by hashes.

PR13's historical +9% large-response result was measured on top of its other
performance changes, including autoflush. It is not an estimate for this port.
Its original note about reduced combining was correct, but the claim that it
matters only with permanent backpressure depends on that older scheduling.
Current deferred writes can build a queue during an ordinary fast-client burst.

## Tests and review findings

The original `t/54-outbound-byte-accounting.t` passed against the exported
b8dd6ee library: one file, five tests. It observes a real TCP backlog, checks
counter equality against the queue, and drains the response to zero. It does
not exercise the close/reset and rejected-write cases below.

A deterministic probe uses the actual Connection implementation and installed
IO::Async::Stream with a public custom writer to force EAGAIN and 8 KiB partial
progress. The same observations reproduced with the original implementation
and the temporary adaptation:

| Observation | Queue bytes | Raw counter | Getter |
| --- | ---: | ---: | ---: |
| Normal partial write from 64 KiB | 57,344 | 57,344 | 57,344 |
| `_close`, before queued output drains | 65,536 | 0 | 0 |
| One partial write after `_close` | 57,344 | -8,192 | 0 |
| Full drain after `_close` | 0 | -65,536 | 0 |
| 100-byte write rejected by closing stream | 65,536 | 65,636 | 65,636 |
| Forced write error and close | 0 | 0 | 0 |

The accounting works during ordinary enqueue, would-block, partial progress,
and full drain. The close reset is different: `_close` normally requests
`close_when_empty`, which keeps queued data for flushing. The comment saying
that bytes are abandoned is incorrect for that path. Later callbacks subtract
from an already-zero counter; the getter's clamp hides the negative number.

This demonstrates a broken exact-accounting invariant, not a proven PAGI spec
violation after terminal scope state or a loss of response bytes. A proper port
must decide whether the counter tracks the still-draining stream or deliberately
stops measuring at scope closure, and implement that consistently. Clamping is
not a substitute for that decision.

The rejected-write case calls the internal write helper after the stream enters
closing state. It exposes an assumption: incrementing happens before the stream
accepts the write. It is a boundary probe, not evidence that ordinary conforming
application sends reach that situation; the current send guards need reviewing
before deciding whether a new guard is necessary.

The probe's passing assertions confirm these observations, including the bad
counter values. They do not mean the candidate passes a correctness gate. No
full current Server suite, TLS suite, or HTTP/2 integration suite was run against
this diagnostic adaptation.

## Unprofiled comparison

Twenty sequential runs used one worker, 25 HTTP clients or 20 persistent WebSocket
clients, EV 0.05 and pure-Perl Futures. Each workload ran baseline, candidate,
candidate, baseline; each run had a one-second warmup and ten-second measurement.
The existing apps, content preflights and WebSocket echo/close checks were used.
All measured responses succeeded: 213,185 HTTP responses and 171,613 WS echoes.

Rates are requests/second, except WebSocket echoes/second. Every sample is retained.

| Workload | Baseline samples | Counter samples | Baseline mean | Counter mean | Mean difference |
| --- | --- | --- | ---: | ---: | ---: |
| GET | 3,357.9; 2,606.4 | 2,922.7; 2,834.7 | 2,982.1 | 2,878.7 | -3.5% |
| POST with observer | 2,449.5; 2,011.3 | 2,489.6; 1,265.5 | 2,230.4 | 1,877.6 | -15.8% |
| 64-chunk stream | 211.3; 263.4 | 258.0; 236.0 | 237.3 | 247.0 | +4.1% |
| SSE burst | 116.6; 71.4 | 128.4; 65.5 | 94.0 | 97.0 | +3.2% |
| WebSocket echo | 3,435.0; 4,727.5 | 4,260.7; 4,732.1 | 4,081.3 | 4,496.4 | +10.2% |

These mean differences are arithmetic descriptions, not reliable measured gains
or regressions. Within-variant variation is large; POST and SSE each have a
candidate sample approximately half the other's rate. Streaming samples overlap.
A single aggregate host-load snapshot after the run cannot establish conditions
throughout it. The user's report of concurrent work makes repeating on a quieter
host the appropriate next measurement, rather than selecting favorable samples.

## Recommendation

Keep PR13's centralized write/accounting idea available. Do not merge the old
PR wholesale or promote this temporary adaptation. Review and resolve the small
counter-lifecycle contract first; preserve current send/close semantics. If we
pursue a corrected port, validate it against current closure, backpressure, TLS,
HTTP/2 and protocol tests, then benchmark with the machine quiet. Avoid adding
other optimizations before isolating the counter's actual effect.

No upstream request is needed for this evaluation. Current benchmark results
neither prove the counter is worthwhile nor rule it out.

## Evidence

[pr13-counter-review-data](pr13-counter-review-data/) contains the source map,
original and adapted edge observations, candidate probe TAP, preparation script,
exact diagnostic patch and hashes, benchmark driver, raw client samples and
summary. Full source exports and server logs remain under
`/tmp/pagi-pr13-counter-review-20260923` and are not committed.

The scripts identify the current checkout and original commit explicitly.
`prepare.py` creates a disposable candidate; `bench.py` refuses to reuse its
existing runs directory. Benchmarking is currently paused; do not treat this
artifact as a request to run it automatically.
