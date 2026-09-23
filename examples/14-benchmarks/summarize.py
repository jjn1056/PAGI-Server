#!/usr/bin/env python3
"""Summarize run.py JSONL samples; latency columns are means of run percentiles."""
import argparse
from collections import defaultdict
import json
from pathlib import Path
from statistics import mean

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('results', nargs='+', type=Path)
args = parser.parse_args()
groups = defaultdict(lambda: defaultdict(list))
identities = {}
for path in args.results:
    for line in path.read_text().splitlines():
        if line.strip():
            row = json.loads(line)
            key = (row['workers'], row['case'])
            identity = (row.get('run_id', str(path.resolve())), row['seconds'],
                        row.get('connections') if row['case'] == 'websocket' else row['concurrency'])
            if key in identities and identities[key] != identity:
                parser.error(f'incompatible experiments for {key}; summarize these runs separately')
            identities[key] = identity
            groups[key][row['variant']].append(row)
variants_seen = {variant for variants in groups.values() for variant in variants}
labels = ('baseline', 'candidate') if variants_seen <= {'baseline', 'candidate'} else ('release', 'main')
if not variants_seen <= set(labels):
    parser.error('summarize installed-release and checkout comparisons separately')
left, right = labels
for workers in sorted({key[0] for key in groups}):
    print(f'\n## {workers} worker(s)\n')
    print(f'| Case | Samples {left}/{right} | {left.title()} rate | {right.title()} rate | Change | {left.title()} p50/p99 ms | {right.title()} p50/p99 ms |')
    print('| --- | ---: | ---: | ---: | ---: | ---: | ---: |')
    for (count, case), variants in groups.items():
        if count != workers:
            continue
        release, main = variants[left], variants[right]
        if not release or not main:
            print(f'| {case} | {len(release)}/{len(main)} | pending | pending | — | — | — |')
            continue
        rate = 'messages_per_second' if case == 'websocket' else 'rps'
        r, m = mean(x[rate] for x in release), mean(x[rate] for x in main)
        def latency(rows):
            if case == 'websocket':
                p50, p99 = mean(x['p50_ms'] for x in rows), mean(x['p99_ms'] for x in rows)
            else:
                p50, p99 = 1000 * mean(x['p50_s'] for x in rows), 1000 * mean(x['p99_s'] for x in rows)
            return f'{p50:.2f}/{p99:.2f}'
        print(f'| {case} | {len(release)}/{len(main)} | {r:,.1f} | {m:,.1f} | {100*(m/r-1):+.1f}% | {latency(release)} | {latency(main)} |')
    print('\nRates: HTTP requests/sec, SSE completed streams/sec, WebSocket echoes/sec.')
    print('Each latency cell averages per-run percentiles; it is not a pooled percentile.')
