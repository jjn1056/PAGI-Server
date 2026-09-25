import json, statistics, sys
from pathlib import Path
root=Path(sys.argv[1]); phase=sys.argv[2] if len(sys.argv)>2 else 'timed'
file=root/phase/'results.jsonl'; rows=[json.loads(x) for x in file.read_text().splitlines()] if file.exists() else []
summary={}
for case in dict.fromkeys(r['case'] for r in rows):
 summary[case]={}
 for label in ['release','before','buffers64','read64']:
  items=[r for r in rows if r['case']==case and r['variant']==label]
  if not items:continue
  stats={}
  for metric in ['rps','p95_s','p99_s','max_s','messages_per_second','p99_ms']:
   values=[r[metric] for r in items if r.get(metric) is not None]
   if values:stats[metric]={'samples':values,'median':statistics.median(values),'min':min(values),'max':max(values)}
  if case in ('mixed','mixed-upload'):
   for metric in ['rps','p95_s','p99_s']:
    values=[r['background'][metric] for r in items if r['background'].get(metric) is not None]
    if values:stats['background_'+metric]={'samples':values,'median':statistics.median(values),'min':min(values),'max':max(values)}
  summary[case][label]={'n':len(items),'metrics':stats}
 print(case)
 for label,s in summary[case].items():
  m=s['metrics'];rate=m.get('rps',m.get('messages_per_second'))['median']
  lat=1000*m['p99_s']['median'] if 'p99_s' in m else m.get('p99_ms',{}).get('median')
  print(f" {label}: n={s['n']} rate={rate:.2f} p99_ms={lat}")
  if 'background_p99_s' in m: print(f"   small-response p99_ms={1000*m['background_p99_s']['median']:.2f}")
(root/(phase+'-summary.json')).write_text(json.dumps(summary,indent=2)+'\n')
print('Completed rows:',len(rows))
