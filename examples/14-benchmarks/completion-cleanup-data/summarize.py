import json,re,statistics
from pathlib import Path
p=Path(__file__).resolve().parent
rows=[json.loads(x) for x in (p/'runs/results.jsonl').read_text().splitlines()]
resources=[json.loads(x) for x in (p/'runs/resources.jsonl').read_text().splitlines()]
assert len(rows)==len(resources)==18
out={}
for case in ['get','post-observe','stream-observe']:
 out[case]={}
 for variant in ['release','baseline','candidate']:
  rr=[r for r in rows if r['case']==case and r['variant']==variant]
  rs=[r for r in resources if r['case']==case and r['variant']==variant]
  assert len(rr)==len(rs)==2
  out[case][variant]=dict(samples=[r['rps'] for r in rr],rps=statistics.mean(r['rps'] for r in rr),cpu_us=statistics.mean(r['cpu_us_per_request'] for r in rs),rss_mib=statistics.mean(r['rss_mib'] for r in rs))
 for variant in ['baseline','candidate']:
  out[case][variant]['vs_release_percent']=100*(out[case][variant]['rps']/out[case]['release']['rps']-1)
 out[case]['candidate']['vs_baseline_percent']=100*(out[case]['candidate']['rps']/out[case]['baseline']['rps']-1)
summary=dict(workloads=out,total_responses=sum(r['responses'] for r in rows),preflight_cpu_idle=[],preflight_swapouts=[])
for f in sorted((p/'runs').glob('*-load.txt')):
 s=f.read_text()
 summary['preflight_cpu_idle'].append(float(re.findall(r'([\d.]+)% idle',s)[-1]))
 summary['preflight_swapouts'].append(int(re.findall(r'(\d+)\((\d+)\) swapouts',s)[-1][1]))
(p/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps(summary,indent=2))
