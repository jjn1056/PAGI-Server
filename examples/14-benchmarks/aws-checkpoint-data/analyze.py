#!/usr/bin/env python3
import json,statistics,re,sys
from pathlib import Path
p=Path(sys.argv[1]);rows=[json.loads(s) for s in (p/'results.jsonl').read_text().splitlines()]
labels=['release','pre','saved','cleanup','current'];report={}
for case in dict.fromkeys(r['case'] for r in rows):
 report[case]={}
 for label in labels:
  rr=[r for r in rows if r['case']==case and r['variant']==label]
  rates=[r['rps'] for r in rr]
  if not rates:continue
  report[case][label]={'n':len(rr),'samples':rates,'mean':statistics.mean(rates),'range_percent':100*(max(rates)-min(rates))/statistics.mean(rates),'p50_s':statistics.mean(r['p50_s'] for r in rr),'p99_s':statistics.mean(r['p99_s'] for r in rr)}
 for a,b in zip(labels,labels[1:]):
  if a in report[case] and b in report[case]:
   report[case][b]['vs_previous_percent']=100*(report[case][b]['mean']/report[case][a]['mean']-1)
   report[case][b]['paired_percent']=[100*(y/x-1) for x,y in zip(report[case][a]['samples'],report[case][b]['samples'])]
 for label in labels[1:]:
  if label in report[case]:report[case][label]['vs_release_percent']=100*(report[case][label]['mean']/report[case]['release']['mean']-1)
summary={'runs':len(rows),'responses':sum(r['responses'] for r in rows),'workloads':report}
vm=[x.split() for x in (p/'vmstat.txt').read_text().splitlines() if re.match(r'^\s*\d',x)][1:]
if vm:summary['host_samples']={'n':len(vm),'max_swap_in':max(int(x[6]) for x in vm),'max_swap_out':max(int(x[7]) for x in vm),'max_iowait_percent':max(int(x[15]) for x in vm),'max_steal_percent':max(int(x[16]) for x in vm)}
print(json.dumps(summary,indent=2));(p/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
