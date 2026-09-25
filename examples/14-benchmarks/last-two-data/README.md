# Header-scan and shared-send evidence

See [the experiment report](../HTTP-SEND-2026-09-25.md) for the decision and
interpretation. Both patches apply independently to `efbed54`; they must not
be applied on top of each other. The shared-send patch is the retained runtime
candidate. The header patch is rejected research, not an additional change.

- `work-map.md`: repository, branch, ownership and deployment boundary.
- `identities.json`: SHA-256 for every archived runtime and launcher file.
- `header.patch`, `send.patch`: independently reproduce the measured sources.
- `make-send.py`: mechanical transformation used to create the send candidate.
- `send-lifetime.t`: ownership checks used in local and Linux verification.
- `*-tests.log`, `send-smoke.log`, `full-suite.log`: local verification.
- `send-negative-control.log`: attempted control that did not fail.
- `send-strong-owner-control.log`: weakening-removal control that did fail.
- `remote-tests.log`: Linux lifetime checks.
- `review.md`: independent review, including its scope and limitations.
- `smoke/`, `timed/`: raw results, module/source identities, client/server logs,
  per-process resource counters, vmstat and pidstat output.
- `compare.py`, `linux-harness.py`: the exact remote comparison driver and harness.
- `summarize.py`, `summary.json`: calculations and individual sample values.

Recreate the summary from this directory's parent:

```sh
python3 last-two-data/summarize.py last-two-data
```

The comparison driver uses the recorded AWS directory layout. Benchmark app
hashes and loaded dependency paths are recorded in each run's metadata. The
host was stopped after downloading and validating the results. Logs and client
output are preserved verbatim, including their original whitespace.
