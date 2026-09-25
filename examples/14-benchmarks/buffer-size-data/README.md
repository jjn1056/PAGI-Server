# Buffer-size comparison evidence

See [the report](../BUFFER-SIZES-2026-09-25.md) for results and limitations.
`before` is 5c960b2, `release` is installed CPAN 0.002013, and `buffers64`
changes only read_len/write_len from 8192 to 65536. Source snapshots are
reconstructible from that base and `candidate.patch`; they are not duplicated
here. No candidate runtime change was applied to the local branch.

Rebuild summaries from the repository root:

```sh
python3 examples/14-benchmarks/buffer-size-data/summarize.py examples/14-benchmarks/buffer-size-data smoke
python3 examples/14-benchmarks/buffer-size-data/summarize.py examples/14-benchmarks/buffer-size-data timed
```

`compare.py` and `buffer-harness.py` preserve the exact remote paths and commands.
Adapt those paths before a new run. The harness regenerates `post-payload.bin`
for each POST case: 1048576 bytes of `x` for post-large, 1024 for post-observe.
That mutable payload file is intentionally not included; copying the last one
would misrepresent earlier commands. Metadata records case settings and script,
app, source and dependency hashes. Per-run commands are in results.jsonl.

`mixed` records its small-response background separately from bulk measurements.
Their measurement windows differ; do not add their rates or pool percentiles.
All raw log whitespace is preserved. The initial missing-fixture test failure
is retained separately from the corrected passing run. The two HTTP/2 tests
were skipped; no dependencies were changed during this experiment.
