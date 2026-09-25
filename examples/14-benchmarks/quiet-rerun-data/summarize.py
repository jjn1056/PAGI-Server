import json, re, statistics
from pathlib import Path

HERE = Path(__file__).resolve().parent
all_rows, summary = [], {}
for phase in ('receive','adapter'):
    file = HERE/phase/'results.jsonl'
    if not file.exists():
        continue
    rows = [json.loads(line) for line in file.read_text().splitlines() if line]
    for row in rows:
        path = HERE/phase/(row['stamp']+'.process-time.txt')
        if not path.exists() or not path.stat().st_size:
            continue  # run_one records its row just before server shutdown
        timing = path.read_text()
        cpu = re.search(r'([\d.]+) real\s+([\d.]+) user\s+([\d.]+) sys', timing)
        row.update(phase=phase, process_cpu_s=float(cpu[2])+float(cpu[3]),
            peak_rss_mib=int(re.search(r'(\d+)\s+maximum resident set size', timing)[1])/1048576)
        if row['case']=='websocket':
            row.update(rate=row['messages_per_second'], p50_ms=row['p50_ms'], p99_ms=row['p99_ms'])
        else:
            row.update(rate=row['rps'], p50_ms=1000*row['p50_s'], p99_ms=1000*row['p99_s'])
        all_rows.append(row)
    summary[phase] = {}
    for case in dict.fromkeys(r['case'] for r in all_rows if r['phase']==phase):
        group = {}
        for variant in ('baseline','candidate'):
            samples = [r for r in all_rows if r['phase']==phase and r['case']==case and r['variant']==variant]
            if samples:
                group[variant] = {key:statistics.mean(r[key] for r in samples)
                    for key in ('rate','p50_ms','p99_ms','process_cpu_s','peak_rss_mib')}
                group[variant]['samples'] = [{key:r[key] for key in ('rate','peak_rss_mib','p99_ms')} for r in samples]
        if len(group)==2:
            group['rate_delta_pct'] = 100*(group['candidate']['rate']/group['baseline']['rate']-1)
        summary[phase][case] = group
(HERE/'summary.json').write_text(json.dumps(dict(runs=all_rows,summary=summary),indent=2)+'\n')
for phase,cases in summary.items():
    print(phase)
    for case,group in cases.items():
        if 'rate_delta_pct' not in group:
            print(case, 'in progress'); continue
        b,a=group['baseline'],group['candidate']
        print(f"{case}: {b['rate']:.1f} -> {a['rate']:.1f} ({group['rate_delta_pct']:+.2f}%), "
              f"RSS {b['peak_rss_mib']:.2f} -> {a['peak_rss_mib']:.2f} MiB, "
              f"samples {len(b['samples'])}/{len(a['samples'])}")
