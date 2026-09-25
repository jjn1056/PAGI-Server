from pathlib import Path
from statistics import mean
import json,re
P=Path(__file__).resolve().parent
D=P/'settled-runs'
rows=[json.loads(x) for x in (D/'results.jsonl').read_text().splitlines()] if (D/'results.jsonl').exists() else []
resources={r['stamp']:r for r in [json.loads(x) for x in (D/'resources.jsonl').read_text().splitlines()]} if (D/'resources.jsonl').exists() else {}
summary=[]
for case in dict.fromkeys(r['case'] for r in rows):
 groups={v:[r for r in rows if r['case']==case and r['variant']==v and r['stamp'] in resources] for v in ('baseline','candidate')}
 if any(len(g)!=2 for g in groups.values()):continue
 record={'case':case,'variants':{}}
 for variant,group in groups.items():
  rr=[resources[r['stamp']] for r in group]
  rates=[r.get('rps',r.get('messages_per_second')) for r in group]
  cpu=[r['cpu_us_per_request'] for r in rr]
  record['variants'][variant]={'rates':rates,'mean_rate':mean(rates),'cpu_us_per_request':cpu,'mean_cpu_us_per_request':mean(cpu) if all(x is not None for x in cpu) else None,'peak_rss_mib':[r['rss_mib'] for r in rr],'mean_peak_rss_mib':mean(r['rss_mib'] for r in rr),'cpu_s':[r['cpu_s'] for r in rr]}
 a,b=(record['variants'][v] for v in ('baseline','candidate'))
 record['rate_delta_pct']=100*(b['mean_rate']/a['mean_rate']-1)
 record['cpu_delta_pct']=100*(b['mean_cpu_us_per_request']/a['mean_cpu_us_per_request']-1) if a['mean_cpu_us_per_request'] is not None else None
 summary.append(record)
 print(case, 'rates',a['rates'],'=>',b['rates'],'delta%',round(record['rate_delta_pct'],2),'CPU%',round(record['cpu_delta_pct'],2) if record['cpu_delta_pct'] is not None else '-', 'RSS',round(a['mean_peak_rss_mib'],2),round(b['mean_peak_rss_mib'],2))
loads=[]
for f in sorted(D.glob('*-load.txt')):
 s=f.read_text()
 idle=re.findall(r'([\d.]+)% idle',s)
 swaps=re.findall(r'\((\d+)\) swapins,.*?\((\d+)\) swapouts',s)
 loads.append({'file':f.name,'last_cpu_idle_pct':float(idle[-1]),'last_swapins':int(swaps[-1][0]),'last_swapouts':int(swaps[-1][1])})
result={'completed_runs':len(rows),'http_responses':sum(r.get('responses',0) for r in rows),'ws_echoes':sum(r.get('messages',0) for r in rows),'cases':summary,'load_snapshots':loads}
(P/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
print('Completed',len(rows),'HTTP responses',result['http_responses'],'WS echoes',result['ws_echoes'])
if loads:print('Pre-run CPU idle range:',min(x['last_cpu_idle_pct'] for x in loads),max(x['last_cpu_idle_pct'] for x in loads),'snapshots with swap-outs',sum(x['last_swapouts']>0 for x in loads),'of',len(loads))
