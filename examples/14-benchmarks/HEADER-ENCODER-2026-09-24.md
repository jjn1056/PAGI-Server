# HTTP/1 header encoder experiment — 2026-09-24

Status: implemented and correctness-reviewed, uncommitted on
`experiment/http-simplification`. Performance remains inconclusive. Benchmarking
is paused after the user reported heavy fan activity; the POST recheck had
already finished when host load was inspected. No further benchmark was started.
Do not claim a general speedup or attribute the slower samples to load as fact.

## Change and boundary

Public response-start/trailer serializers still check names and values, using
the existing EventValidator primitives. They share private encoders with the
ordinary HTTP/1 send path, which already validates application events before
sequence changes, stripping, HEAD handling or output. That internal path now
avoids repeated byte checks and the redundant trailer-pair copy. Encoding is
synchronous; no await allows application mutation between validation and encoding.
Synthetic error and SSE serializers retain the checked entry points. There are
no validation flags, caches, spec changes or HTTP/2 changes.

The baseline is an exact pre-experiment lib/bin snapshot at
`/tmp/pagi-header-encoder-20260924/before`. It includes the earlier uncommitted
normal HTTP completion cleanup. Only Connection.pm and Protocol/HTTP1.pm differ
between this baseline and candidate; `header-encoder-data/runtime.patch` records
that delta independently of earlier changes. Source hashes were checked after
every run and again when saving this report.

## Native results

Perl 5.42.2, pure-Perl Future, IO::Async::Loop::EV 0.05, LIBEV_FLAGS=8;
one worker, 25 clients. These are diagnostic measurements, not the user's
original 16-worker/500-client comparison. Release is installed Server 0.002013;
source hashes identify the current variants independently of version strings.

Initial round: 15 measured seconds/sample, one-second warmup. Each case ran
release, baseline, candidate, candidate, baseline, release. Means in requests/sec:

| Case | Release | Baseline before this change | Candidate | vs baseline |
|---|---:|---:|---:|---:|
| get | 3,360.1 | 3,161.1 | 3,114.8 | -1.46% |
| headers | 2,946.1 | 2,811.6 | 2,947.1 | +4.82% |
| post-observe | 3,215.2 | 3,018.5 | 2,922.9 | -3.17% |
| stream-observe | 256.6 | 245.8 | 260.7 | +6.09% |

All 24 runs passed their response checks, totaling 848,132 measured responses.
The ten-header case is promising, but the small GET decline and POST decline
make the overall result mixed. Streaming's higher mean is strongly affected by
one weak baseline sample; it is not an established gain from this header change.

POST recheck: 20 seconds/sample, reversed interior order:
release, candidate, baseline, baseline, candidate, release.

| Case | Release | Baseline before this change | Candidate | vs baseline |
|---|---:|---:|---:|---:|
| POST observer recheck | 3,298.3 | 3,128.8 | 2,996.9 | -4.22% |

All six recheck runs passed, totaling 377,053 measured responses. Candidate
samples were 2,813.0 and 3,180.8 requests/sec; baseline was 3,098.4 and 3,159.2;
release was 3,200.6 and 3,395.9. The candidate mean is lower in both rounds;
this adverse result must not be discarded. However, the roughly 13% candidate
sample spread prevents a confident attribution to the code on these data alone.

Initial pre-run CPU idle ranged 70.9–93.1%; one sample showed 165,515 swapouts
in its sampling interval. Recheck pre-run idle ranged 82.34–94.24%, with another
77,223 swapouts in one interval. These brief pre-run samples are not continuous
load or thermal measurements. A later host sample, after all runs completed,
showed 88–91% idle, background VM/UI activity and swapins. Thermal status could
not be read. Fan activity does not establish thermal throttling or prove that
background load caused a particular result. No samples were excluded.

Individual rates, CPU/request, peak RSS, load snapshots, client output and source
identities are preserved under `header-encoder-data/`. Original server logs and
baseline source snapshot remain under `/tmp/pagi-header-encoder-20260924`.
The archived drivers reference that snapshot and the repository's existing
quiet-rerun harness; the evidence directory alone is not a standalone runner.

## Correctness and next decision

- Focused regressions: 15 files / 201 tests pass, covering byte validation,
  injection, mandatory validation, HEAD/trailers, framing, refusal, completion,
  callbacks and cancellation. Full suite was not rerun in this experiment.
- New tests pass on the baseline and candidate. A temporary negative control
  that disables value checking fails as expected; production source was untouched.
- Example checks and four harness tests pass. A reusable ten-header example
  is now selectable with `--cases headers` in the normal benchmark runner.
- Independent review found no blocking correctness or simplicity issues.
- Instrumented send-factory probes confirm one name/value check per app header
  or trailer in the candidate, versus two and three respectively before this
  change. These counts demonstrate removed work, not elapsed-time improvement.

Leave the candidate uncommitted for a quieter release/baseline/candidate
comparison. Do not add compensating optimizations or label it a performance win.
If the slowdown repeats under controlled conditions, reconsider this delta
without removing the earlier completion cleanup. Nothing was committed or pushed.
