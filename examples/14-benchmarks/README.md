# Repeatable server benchmarks

These are raw PAGI apps for comparing server versions. They deliberately omit
routing, JSON processing, databases, and logging from the measured request path.
They are examples, not production endpoints. Run them on loopback.

| App | Workload | Variants |
| --- | --- | --- |
| `get.pl` | Tiny fixed response, no receive | `?observe=1` registers one completion callback |
| `headers.pl` | Same tiny body with ten application response headers | Runner case: `headers` |
| `post.pl` | Read the entire request body; respond with byte count and newline | `?observe=1` |
| `stream.pl` | 64 KiB, sent as 64 chunks of 1 KiB | `?single=1` sends the same bytes once; either mode accepts `observe=1` |
| `sse.pl` | 100 ordered events, each containing 128 bytes of data, then clean close | `?paced=1` waits 10 ms between events |
| `websocket.pl` | Echo binary or text messages without changing their contents | Driver uses persistent connections and 128-byte binary messages |

The completion callback increments a worker-local counter. It does not log or
change the response. This variant measures the cost of registering and delivering
an observer; it does not simulate expensive application cleanup.

Bodies are prebuilt where possible. The HTTP streaming comparison keeps payload
bytes constant, but the multi-send response also uses chunked transfer framing.
It is a comparison of these actual serving paths, not a pure function-cost test.
Multiple sends do not necessarily imply multiple packets or suspended Futures.
SSE sends are bursts unless pacing is requested. No slow-reader/backpressure or
HTTP/2 claims should be inferred from these workloads.

## Manual use

From the repository root, with the intended Perl environment active:

```sh
LIBEV_FLAGS=8 PERL_FUTURE_NO_XS=1 perl -Ilib bin/pagi-server \
  --loop EV --workers 16 --env production examples/14-benchmarks/get.pl
hey -z 30s -c 500 http://127.0.0.1:5000/
hey -z 30s -c 500 'http://127.0.0.1:5000/?observe=1'
```

Start the corresponding app for the following commands:

```sh
# Small POST (runner uses a fixed 1 KiB body).
curl --data 'hello' http://127.0.0.1:5000/

# Same 64 KiB in one send or 64 sends.
hey -z 30s -c 500 'http://127.0.0.1:5000/?single=1'
hey -z 30s -c 500 http://127.0.0.1:5000/

# Accept selects the server's SSE scope.
curl -N -H 'Accept: text/event-stream' 'http://127.0.0.1:5000/?paced=1'
hey -z 30s -c 50 -H 'Accept: text/event-stream' http://127.0.0.1:5000/

# WebSocket: seconds, then number of persistent connections.
perl examples/14-benchmarks/ws-client.pl ws://127.0.0.1:5000/ 30 20
```

To run the installed release instead, omit `-Ilib` and use the installed
`pagi-server` executable. Check its startup banner and the module paths in the
recorded metadata; an inherited development `PERL5LIB` can contaminate a comparison.

## Repeatable release/main comparison

Requirements: Python 3.9+, `hey`, and the server's Perl dependencies. The optional
WebSocket driver also needs `Net::Async::WebSocket::Client` (already used by the
server's integration tests). No PAGI-Tools dependency is involved.

```sh
perl examples/14-benchmarks/check-apps.pl
python3 examples/14-benchmarks/check-harness.py

python3 examples/14-benchmarks/run.py \
  --output /tmp/pagi-baseline-w1 --seconds 10 --workers 1 --concurrency 50

python3 examples/14-benchmarks/run.py \
  --output /tmp/pagi-baseline-w16 --seconds 10 --workers 16 --concurrency 500
```

For this project's Perl environment, prefix those commands with
`perlbrew exec --with perl-5.42.2@default`.

Use a fresh output directory per invocation; the runner rejects existing results.
The summary rejects overlapping cases from separate experiments or mismatched loads.

Each case runs **release, main, main, release**. `release` means the installed
`pagi-server` on PATH, launched with the same Perl as main. `main` means the current
checkout, including uncommitted changes, not necessarily the Git branch name.
The runner sets `LIBEV_FLAGS=8` and `PERL_FUTURE_NO_XS=1` for both. It preserves
perlbrew/local::lib paths and records them. It selects its own temporary ports,
starts production EV servers, and stops only the process groups it created.
Do not run both comparisons concurrently: they would compete for the same machine.

The `headers` case checks all ten response fields during preflight and can be
selected with `--cases headers` for a focused header-processing comparison.

Select fewer cases or run a smoke check:

```sh
python3 examples/14-benchmarks/run.py --output /tmp/pagi-smoke \
  --seconds 1 --concurrency 2 --ws-connections 2 --order release main

python3 examples/14-benchmarks/run.py --output /tmp/pagi-post \
  --cases post post-observe --seconds 30 --workers 16 --concurrency 500
```

The runner checks exact response contents before load: HTTP bodies, ordered SSE
IDs/data, and a POST delivered in two writes separated by 20 ms. The split POST
is a receive-wait probe, not a throughput measurement; OS buffering may still
coalesce the writes. Warmup runs for one second, excluded from reported results.
During HTTP/SSE load, `hey` checks completion/status, not every response payload.
Any reported request error or non-200 status rejects the sample.

WebSocket measurements verify every echo. Connections are established before a
one-second warmup; setup and closing are outside the message-rate interval. Each
connection has one outstanding message. Reported RTT includes client work and
loopback transport. `close_reply_p50_ms` measures the peer Close reply, after which
the client closes its transport; it is not server terminal-notification latency.
The single-process Perl client can itself become the bottleneck, especially with
16 server workers. This is a latency/regression probe, not a capacity claim.
`--ws-connections` is independent of HTTP `--concurrency` and defaults to 20.

## Compare two checkouts

The default comparison remains installed release versus current checkout. To
compare an unchanged checkout with an experiment branch, use `--baseline-repo`:

```sh
python3 examples/14-benchmarks/run.py \
  --baseline-repo /path/to/unchanged/PAGI-Server \
  --output /tmp/pagi-experiment-w1 --seconds 10 --workers 1 --concurrency 50
```

The candidate is this runner's checkout by default (override with `--repo`).
Both servers use the same example files from this runner's directory. Samples
are labeled **baseline/candidate**, and default order is baseline → candidate →
candidate → baseline. If overriding `--order`, use those labels. Metadata records
both module paths, revisions, dirty status and library hashes; version banners
alone do not identify a checkout. Summarize this comparison separately from the
older installed-release measurements.

## Results and interpretation

A live summary is available while a run is in progress:

```sh
python3 examples/14-benchmarks/summarize.py \
  /tmp/pagi-baseline-w1/results.jsonl /tmp/pagi-baseline-w16/results.jsonl
```

Pass only paths that exist. A case is fully sampled when the table shows 2/2.

Output contains metadata (Perl/module paths, source commit, dirty status, app
hashes, settings), server logs, raw client reports, and `results.jsonl`.
HTTP measurements report requests/sec and mean/p50/p99 request latency. Streaming
also reports payload MiB/sec. SSE reports streams/sec and derived events/sec
(100 per completed stream); total-response latency is not per-event latency.
WebSocket reports messages/sec and p50/p99 RTT, plus setup and close-reply timing.

Compare versions **within each workload**, including individual samples and
variation. A callback-on/off difference measured at different times can be noise.
Ten-second runs are a screening baseline; repeat longer runs before attributing
small differences or claiming an optimization. Client and server share a host,
and background activity, worker scheduling, framing, and connection reuse all
influence the result. These examples intentionally leave those limits visible.

The initial release/main comparison is recorded in [BASELINE-2026-09-22.md](BASELINE-2026-09-22.md).

The simplification experiment is recorded in [SIMPLIFICATION-2026-09-23.md](SIMPLIFICATION-2026-09-23.md), comparing unchanged main with the isolated test branch.

The follow-up [NYTProf investigation](PROFILING-2026-09-23.md) records request-path call counts and a sustained-I/O delay in deferred terminal callbacks.

### Canonical performance checkpoint and focused AWS profile

The shared-send runtime checkpoint is `cdf4a7c` on
`experiment/http-simplification`: all previously retained changes plus the
shared HTTP send coroutine. The listener experiment below builds on this
checkpoint, alongside the installed CPAN release. The header-scan and
scalar-alias ownership experiments remain rejected.

The earlier checkpoint `874b120` is the subject of the
[AWS-FOCUSED-PROFILE-2026-09-25.md](AWS-FOCUSED-PROFILE-2026-09-25.md)
release/current CPU profile, with call counts, native benchmark reference,
and the buffered-POST profiling limitation.

The subsequent [send-state ownership experiment](STATE-OWNERSHIP-2026-09-25.md)
was tested and rejected: small mixed results did not justify its extra lifetime
rule.

The next [header-scan and shared-send comparison](HTTP-SEND-2026-09-25.md)
found repeatable 4.5–5.1% ordinary HTTP gains from sharing the send coroutine,
with streaming effectively flat. Shared send is retained after the full enabled
suite passed (180 files / 1,273 tests); the header-scan candidate was discarded.
The report compares both independent candidates with the saved checkpoint and
CPAN release.

The latest retained change is [bounded listener batching](LISTENER-2026-09-25.md).
At 500 new connections under established traffic, first-response p99 falls from
3,561 ms at the saved checkpoint to 234 ms (release: 3,264 ms). The full enabled
suite passes: 182 files / 1,282 tests. A separate size sweep supports 64 as a
starting default; a public tuning option remains deferred. The report preserves
release comparisons, streaming/SSE tradeoffs, raw results and the isolated patch.
