import json, statistics, sys
from pathlib import Path
root=Path(sys.argv[1]);file=root/'timed/results.jsonl'
rows=[json.loads(x) for x in file.read_text().splitlines()] if file.exists() else []
summary={}
for case in dict.fromkeys(r['case'] for r in rows):
 summary[case]={}
 print(case)
 for label in ['release','before','listener']:
  items=[x for x in rows if x['case']==case and x['variant']==label]
  if not items:continue
  metrics=['rps','p95_s','p99_s','max_s'] if case!='websocket' else ['messages_per_second','p99_ms','connect_p50_ms']
  stats={}
  for metric in metrics:
   values=[r[metric] for r in items if r.get(metric) is not None]
   if values:stats[metric]={'samples':values,'median':statistics.median(values),'mean':statistics.mean(values),'min':min(values),'max':max(values)}
  if case=='storm':
   for metric in ['rps','p95_s','p99_s']:
    values=[x['background'][metric] for x in items]
    stats['background_'+metric]={'samples':values,'median':statistics.median(values),'min':min(values),'max':max(values)}
  summary[case][label]={'n':len(items),'metrics':stats}
  if case=='websocket':
   print(f" {label} n={len(items)} messages/s={stats['messages_per_second']['median']:.1f} p99={stats['p99_ms']['median']:.3f}ms")
  else:
   lat=stats.get('p99_s');p95=stats.get('p95_s')
   print(f" {label} n={len(items)} rps={stats['rps']['median']:.1f} p95={1000*p95['median']:.2f}ms p99={1000*lat['median']:.2f}ms range={1000*lat['min']:.2f}..{1000*lat['max']:.2f}ms max={1000*stats['max_s']['max']:.2f}ms")
   if case=='storm': print(f"   background rps={stats['background_rps']['median']:.1f}, p99={1000*stats['background_p99_s']['median']:.2f}ms")
(root/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
