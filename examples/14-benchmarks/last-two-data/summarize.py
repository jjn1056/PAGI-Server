#!/usr/bin/env python3
import json,statistics,sys
from pathlib import Path
root=Path(sys.argv[1]);file=root/'timed/results.jsonl'
rows=[json.loads(s) for s in file.read_text().splitlines()] if file.exists() else []
summary={}
print('case | release | saved | headers | shared send | changes vs saved | n')
for case in dict.fromkeys(r['case'] for r in rows):
 metric='messages_per_second' if case=='websocket' else 'rps'
 values={label:[r[metric] for r in rows if r['case']==case and r['variant']==label] for label in ['release','before','headers','send']}
 if not all(values.values()):continue
 means={label:statistics.mean(a) for label,a in values.items()}
 changes={k:100*(means[k]/means['before']-1) for k in ['headers','send']}
 summary[case]={'samples':values,'means':means,'changes_vs_saved':changes,'paired_percent':{k:[100*(c/b-1) for c,b in zip(values[k],values['before'])] for k in ['headers','send']}}
 print(f"{case} | {means['release']:.2f} | {means['before']:.2f} | {means['headers']:.2f} | {means['send']:.2f} | {changes['headers']:+.2f}%, {changes['send']:+.2f}% | {[len(a) for a in values.values()]}")
(root/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
