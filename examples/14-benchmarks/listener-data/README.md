# Listener comparison evidence

See [the report](../LISTENER-2026-09-25.md) for methodology, interpretation and
limits. `before` means runtime checkpoint cdf4a7c (tree base 2e42135), `release`
is installed CPAN 0.002013, and `listener` is the retained batch-64 candidate.

`candidate.patch` applies to 2e42135 and includes runtime changes and regressions.
`identities.json` records source hashes; per-run metadata records loaded modules,
commands and dependency hashes. The work map and review record the scope.

Rebuild summaries without starting a server, from the repository root:

```sh
python3 examples/14-benchmarks/listener-data/summarize.py examples/14-benchmarks/listener-data
python3 examples/14-benchmarks/listener-data/summarize-sweep.py examples/14-benchmarks/listener-data
```

The scripts rewrite their respective JSON summaries. Raw results live in
`smoke/`, `timed/` and `batch-sweep/`. The benchmark drivers preserve the exact
AWS commands and paths; adapt environment paths before attempting a new run.
Do not pool the 30-second main and 12-second sweep background percentiles.

`smoke-short-timeout/` preserves the initial release timeout. Completed smoke
uses 50 new clients and a five-second background window; the measured storm
uses 500. Static planned settings in smoke metadata can describe the primary
workload; the actual per-run commands and row settings identify the smoke load.

Test logs include intentional red regressions and setup mistakes: a nonexistent
focused-test filename and an initial EMFILE mock applied at the wrong socket
class. Corrected logs and `full-suite.log` record the final passing checks.
Raw client/test logs are preserved verbatim, including their whitespace.

No source snapshots or credentials are included. Source identities plus the
base commit and patch reproduce the candidate. AWS was confirmed stopped.
