# Write-buffer measurement: research before another optimization

The repeated scans are real, but there is no existing public queued-byte getter
in IO::Async::Stream 0.805. Adding per-write callbacks to maintain a counter has
an important cost: it prevents the stream from combining adjacent queued writes.
A counter integrated with the stream's own queue is the cleaner direction. This
research does not implement or benchmark a Server optimization.

The saved performance branch remains the base: `experiment/http-simplification`
at 92d39ed, runtime checkpoint b995443. No dependency or Server runtime changed.

## Evidence

The prior profiles showed 129 `_get_write_buffer_size` calls per 64-chunk HTTP
response. Source inspection explains the shape: a check before each body write,
after-write watermark observation, and a drain measurement. The current helper
walks the entire queued-writer list and sums the lengths of materialized strings.

A controlled probe using the installed stream and the actual Server measurement
helper queues fixed 1 KiB strings without flushing between them. Before/after
measurement visits N-squared queue entries: the pre-write scans visit
0 + 1 + ... + (N-1), and post-write scans visit 1 + 2 + ... + N.

| Chunks queued | Queue entries visited by before/after measurements |
| --- | ---: |
| 1 | 1 |
| 8 | 64 |
| 64 | 4,096 |
| 256 | 65,536 |

This count excludes the final drain check and other stream-library calls.
It describes an unflushed burst, not every streaming app. Writes that yield to
I/O frequently can keep a much shorter queue.

## Why callback counting is not an obvious improvement

The stream's `_flush_one_write` combines adjacent plain-string writers when
write lengths match and callback boundaries permit it. Supplying `on_write`
on either neighbor prevents combining. An `on_flush` boundary on the earlier
writer also prevents combining across that boundary.

The probe uses the default 8 KiB write length and a public custom `writer`
callback that consumes bytes deterministically. It first returns EAGAIN, then
removes the allowed prefix on each successful invocation. All bytes, remaining
buffer measurements, and counters are checked. Results for 64 queued 1 KiB
strings:

| Approach | Successful low-level writer calls | Per-write callbacks |
| --- | ---: | ---: |
| Plain writes | 8 | 0 |
| Counter via `on_write` on every write | 64 | 64 |
| Counter via `on_flush` on every write | 64 | 64 |
| Counter at the custom writer layer | 8 | 0 |

These are writer invocations in a deterministic probe, not measured real socket
syscalls or a throughput benchmark. The normal writer invokes syswrite, which
is why preserving this combining behavior matters. The probe retains diagnostic
scans in all variants so it can verify counters; it does not measure the speed
of any counter implementation.

`on_flush` is also insufficient for exact partial-progress accounting. After one
8 KiB write from a 64 KiB queued item, the real queue contains 56 KiB; a counter
updated only by on_flush still reports 64 KiB because that callback has not run.
`on_write` reports each successful write's byte count, so it avoids that accuracy
problem while retaining the combining problem above.

## What a stream-owned counter must account for

A public getter would let Server replace its private-queue scan with one call,
without changing HTTP/SSE/WebSocket backpressure decisions. An incremental
implementation must count actual materialized bytes when they enter the queue,
subtract actual partial progress, and clear discarded bytes on close_now.
Combining adjacent writers must not change the total.

General IO::Async streams can queue Futures and generators as well as strings,
and can encode strings. Their eventual bytes are not known until materialized.
This is tractable where the stream owns those transitions; a Server-side counter
would otherwise have to duplicate that knowledge or explicitly specialize the
stream to the narrower Server use case.

The audited Server write sites currently write materialized byte strings,
including the file-response loops. A specialized Server stream is therefore a
possible alternative, not an impossible one. It would need to own all enqueue
paths, preserve void/scalar write context, handle autoflush and rejected writes,
remain accurate during callbacks, and reset on teardown. Counting only body
writes would miss response headers, trailers, protocol frames and other output.

A custom `writer` is a documented hook and preserves combining in this probe,
but it is not a drop-in counter. IO::Async::SSL installs its own writer to handle
SSL's read/write readiness requirements; replacing it with ordinary syswrite
would lose that behavior. Decorating the active writer requires appropriate
setup ownership. Overriding the private `_syswrite` method instead would create
another dependency on implementation details. Neither integration was tested
here, and the counter probe is not a TLS validation.

## Recommendation

Do not add per-write counting callbacks or an approximate cached queue size to
Connection.pm. They trade away combining or accuracy. Likewise, do not change
autoflush just to keep the queue short; that changes batching and scheduling.

The cleanest long-term boundary is a public queued-byte getter backed by
accounting inside IO::Async::Stream. An upstream proposal is worth discussing,
but no message or issue has been sent and no dependency has been modified.
A public getter implemented by scanning would remove private access but would
not by itself improve performance; the constant-time accounting is the relevant
part of the proposal.

If keeping the change local is preferred, evaluate a small specialized Server
stream explicitly, including TLS and close/error handling. That is a larger
integration decision than the previous receive simplification. Stop here for
discussion rather than quietly adding hooks across the Server's write sites.

This opportunity predates the release/current regression. It can be parked
without discarding the saved performance improvements, while investigation of
the smaller per-request lifecycle overhead continues.

## Sources and reproduction

- [IO::Async::Stream documentation](https://metacpan.org/pod/IO::Async::Stream):
  public write callbacks, custom writer contract, encoding and deferred data.
- [IO::Async::Stream 0.805 source](https://metacpan.org/release/PEVANS/IO-Async-0.805/source/lib/IO/Async/Stream.pm):
  `_flush_one_write`, `write`, `_syswrite`, and `close_now`.
- Installed `IO/Async/SSL.pm`: `sslwrite` and `SSL_upgrade`'s writer configuration.
- Server `Connection.pm`: `_get_write_buffer_size`, `_notify_transport_write`,
  `_check_drain_waiters`, and all calls to the underlying stream's write method.

Context7 was consulted for the documented API; the installed module source and
POD were used to check implementation behavior and resolve generated-documentation
ambiguities. For example, autoflush is a stream configuration parameter, not a
supported per-write parameter in this installed version.

[write-buffer-research-data](write-buffer-research-data/) contains the probe,
its TAP output, JSON observations, source identities and work map. It uses a
socketpair for stream handles but a deterministic custom writer; it does not
perform network load testing. Run from the experiment checkout:

```sh
perlbrew exec --with perl-5.42.2@default perl -Ilib \
  examples/14-benchmarks/write-buffer-research-data/probe.pl /tmp/write-buffer-results.json
```

No full Server suite was repeated because no runtime code changed.
