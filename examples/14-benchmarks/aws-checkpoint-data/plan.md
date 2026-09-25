# AWS checkpoint comparison — 2026-09-24

Approved task: remeasure retained changes before attempting another optimization.
Use installed CPAN 0.002013 as reference; compare the server before performance
work, saved optimizations, completion cleanup and current header encoder.

## Work map

- Repository: /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/experiment-http-simplification.
- Ticket: session performance investigation; no external ticket.
- Branch: experiment/http-simplification at 07a03b8 plus existing uncommitted runtime and benchmark work.
- Owned changes: measurement driver, checkpoint identities, results and documentation only. No runtime edits, commits, branch changes or pushes.
- Deployment: existing isolated EC2 i-077635273be935c19, us-east-1, c7a.xlarge. Stop after evidence downloaded. Larp and other repositories excluded; no changes to their earlier work mapping.
- Push target: none.

## Checkpoints

release: installed CPAN PAGI-Server 0.002013.
pre: immutable git archive of 4625b006aa136844ab80f9163bcffc25db748cb3, lib/bin. Identical runtime to c00219c before the first optimization.
saved: immutable git archive b99544311ebc6454d3f280adbb90b32d299340a9, lib/bin. Identical runtime to 07a03b8.
cleanup: exact /tmp/pagi-header-encoder-20260924/before snapshot, lib/bin.
current: current lib/bin snapshot, byte-identical to the previous AWS comparison.

## Execution

1. Confirm checkpoint diffs and SHA256 manifests locally; copy snapshots without changing local branches. Record immutable git IDs where available.
2. Reuse installed Perl/dependencies, existing Linux harness, apps, CPU affinity and monitoring. No package changes. Verify SSH host alias and source hashes.
3. Smoke all 20 checkpoint/workload combinations. Check response content and protocol outcomes before timing.
4. Compare GET, ten headers, POST observer, stream observer: 15 measured seconds + warmup, one worker, 25 clients. Three rounds with fixed orders: release/pre/saved/cleanup/current; current/cleanup/saved/pre/release; pre/current/release/cleanup/saved. Keep all samples and report ranges. Do not claim small effects beyond observed variation.
5. Recheck loaded and complete snapshot hashes after every run. Capture vmstat and pidstat (all process names, to include renamed server workers), GNU time and client results.
6. Report release and each checkpoint, both adjacent and cumulative differences. Record strengths/limits and whether another experiment is justified; no automatic code changes. Download evidence, stop instance, save on existing branch uncommitted.
