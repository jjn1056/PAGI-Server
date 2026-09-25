#!/usr/bin/env python3
import json,statistics,sys
from pathlib import Path
for directory in sys.argv[1:]:
 p=Path(directory); rows=[json.loads(s) for s in (p/'results.jsonl').read_text().splitlines()]
 print(p.name)
 summary={}
 for case in dict.fromkeys(r['case'] for r in rows):
  summary[case]={}
  for variant in dict.fromkeys(r['variant'] for r in rows if r['case']==case):
   rr=[r for r in rows if r['case']==case and r['variant']==variant]
   key='messages_per_second' if case=='websocket' else 'rps'
   rates=[r[key] for r in rr];mean=statistics.mean(rates)
   summary[case][variant]={'samples':rates,'mean':mean,'range_percent':100*(max(rates)-min(rates))/mean}
  if 'main' in summary[case]:summary[case]['delta_percent']=100*(summary[case]['main']['mean']/summary[case]['release']['mean']-1)
 print(json.dumps(summary,indent=2))
 (p/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
