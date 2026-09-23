import hashlib, json, re, statistics
from pathlib import Path

HERE = Path(__file__).resolve().parent
rows = []
hashes = {}
for result_path in sorted([*(HERE/'runs').rglob('result.json'), *(HERE/'confirm').rglob('result.json')]):
    row = json.loads(result_path.read_text())
    meta = json.loads((result_path.parent/'metadata.json').read_text())
    label = 'before' if row['variant'] == 'main' else 'after'
    assert 'Future/PP.pm' in meta['loaded']['inc'] and 'Future/XS.pm' not in meta['loaded']['inc']
    assert row['responses'] == meta['requests']
    if label in hashes:
        assert hashes[label] == meta['server_hashes']
    hashes[label] = meta['server_hashes']
    timing = (result_path.parent/'process-time.txt').read_text()
    cpu = re.search(r'([\d.]+) real\s+([\d.]+) user\s+([\d.]+) sys', timing)
    row.update(label=label, phase=result_path.relative_to(HERE).parts[0], sample=str(result_path.relative_to(HERE)),
        process_cpu_s=float(cpu[2])+float(cpu[3]),
        peak_rss_mib=int(re.search(r'(\d+)\s+maximum resident set size', timing)[1])/1048576)
    rows.append(row)
summary = {}
for phase, case in sorted({(r['phase'], r['case']) for r in rows}):
    key = case if phase == 'runs' else case+'-confirmation'
    summary[key] = {}
    for label in ('before', 'after'):
        subset = [r for r in rows if r['case'] == case and r['label'] == label and r['phase'] == phase]
        if subset:
            summary[key][label] = {field:statistics.mean(r[field] for r in subset)
                for field in ('rps', 'process_cpu_s', 'peak_rss_mib')}
            summary[key][label]['rps_samples'] = [r['rps'] for r in subset]
    if len(summary[key]) == 2:
        summary[key]['delta_pct'] = 100*(summary[key]['after']['rps']/summary[key]['before']['rps']-1)
for sources in hashes.values():
    for path, digest in sources.items():
        assert hashlib.sha256(Path(path).read_bytes()).hexdigest() == digest, path
(HERE/'results.json').write_text(json.dumps(dict(runs=rows, summary=summary), indent=2)+'\n')
print(json.dumps(summary, indent=2))
