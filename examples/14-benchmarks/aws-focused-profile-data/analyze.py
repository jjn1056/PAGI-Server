#!/usr/bin/env python3
"""Summarize paired CPU profiles. Profile times are not native CPU estimates."""
import hashlib, json, statistics, sys
from pathlib import Path
root=Path(sys.argv[1])
roles={
 'scope_factory':['PAGI::Server::Connection::_create_scope'],
 'connection_state_new':['PAGI::Server::ConnectionState::new'],
 'receive_factory':['PAGI::Server::Connection::_create_receive'],
 'send_factory':['PAGI::Server::Connection::_create_send'],
 'completion_mark':['PAGI::Server::ConnectionState::_mark_complete'],
 'terminal_delivery':['PAGI::Server::ConnectionState::_deliver_terminal'],
 'loop_later':['IO::Async::Loop::later'],
 'loop_watch_idle':['IO::Async::Loop::EV::watch_idle'],
 'clean_predicate':['PAGI::Server::EventValidator::scope_send_clean'],
 'abort_hook_factory':['PAGI::Server::Connection::_h1_abort_hook'],
 'future_new':['Future::PP::new'],
}
result={'warning':'CPU times include substantial profiling overhead, startup and teardown. Do not infer native gains or sum inclusive and exclusive times.','cases':{}}
for case in ['get','post-observe']:
 out={}
 for variant in ['release','current']:
  runs=[]
  for n in [1,2]:
   path=root/f'sub-round-{n}'/f'{variant}-{case}'
   meta=json.loads((path/'metadata.json').read_text())
   subs=json.loads((path/'subs.json').read_text())
   counts=meta['requests']+meta['warmup_requests']+meta['preflight_requests']
   assert counts==5203
   by_name={r['name']:r for r in subs}
   names=dict(roles)
   names['state_publication']=[r['name'] for r in subs if 'Connection.pm:6110]' in r['name']]
   names['terminal_callback']=[r['name'] for r in subs if 'ConnectionState.pm:748]' in r['name']]
   row={'requests':counts,'roles':{}}
   for name,keys in names.items():
    matches=[by_name[k] for k in keys if k in by_name]
    row['roles'][name]={'calls':sum(r['calls'] for r in matches),
     'exclusive_cpu_us_per_request':sum(r['exclusive_s'] for r in matches)*1e6/counts}
   runs.append(row)
  out[variant]={'runs':runs,'mean_roles':{role:{
   'calls_per_request':statistics.mean(r['roles'][role]['calls']/r['requests'] for r in runs),
   'exclusive_cpu_us_per_request':statistics.mean(r['roles'][role]['exclusive_cpu_us_per_request'] for r in runs)} for role in names}}
 result['cases'][case]=out
samples=[]
for line in (root/'vmstat.txt').read_text().splitlines():
 a=line.split()
 if len(a)==18 and all(x.isdigit() for x in a):samples.append(list(map(int,a)))
samples=samples[1:] # first row is the since-boot average
result['telemetry']={'interval_samples':len(samples),'max':{k:max(r[i] for r in samples) for k,i in [('swap_in',6),('swap_out',7),('io_wait_percent',15),('steal_percent',16)]}}
result['completed_profile_runs']=len(list(root.glob('sub-round-*/*/result.json')))+len(list(root.glob('lines/*/result.json')))
assert result['completed_profile_runs']==12
for path in list(root.glob('sub-round-*/*/result.json'))+list(root.glob('lines/*/result.json')):
 d=json.loads(path.read_text());assert d['responses']==(2000 if d['mode']=='line' else 5000)
(root/'summary.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result['telemetry']))
for case, variants in result['cases'].items():
 print(case)
 for role in roles.keys()|{'state_publication','terminal_callback'}:
  print(role,[(v,round(d['mean_roles'][role]['calls_per_request'],4),round(d['mean_roles'][role]['exclusive_cpu_us_per_request'],3)) for v,d in variants.items()])
