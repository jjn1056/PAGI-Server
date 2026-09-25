#!/usr/bin/env python3
import json,statistics,sys
from pathlib import Path
root=Path(sys.argv[1]); file=root/'timed/results.jsonl'
rows=[json.loads(s) for s in file.read_text().splitlines()] if file.exists() else []
summary={}
print('case | release | before | candidate | candidate/before | candidate/release | n')
for case in dict.fromkeys(r['case'] for r in rows):
 metric='messages_per_second' if case=='websocket' else 'rps'
 values={label:[r[metric] for r in rows if r['case']==case and r['variant']==label] for label in ['release','before','candidate']}
 if not all(values.values()):continue
 means={label:statistics.mean(a) for label,a in values.items()}
 summary[case]={'samples':values,'means':means,'candidate_vs_before_percent':100*(means['candidate']/means['before']-1),'candidate_vs_release_percent':100*(means['candidate']/means['release']-1),'paired_percent':[100*(c/b-1) for c,b in zip(values['candidate'],values['before'])]}
 d=summary[case]
 print(f"{case} | {means['release']:.2f} | {means['before']:.2f} | {means['candidate']:.2f} | {d['candidate_vs_before_percent']:+.2f}% | {d['candidate_vs_release_percent']:+.2f}% | {[len(a) for a in values.values()]}")
(root/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
