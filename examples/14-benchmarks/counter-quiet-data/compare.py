import importlib.util,json,os,subprocess,hashlib,shutil,re,contextlib,io,argparse
from pathlib import Path
from types import SimpleNamespace
ROOT=Path('/Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/experiment-http-simplification')
OUT=Path(__file__).resolve().parent
CAND=Path('/tmp/pagi-pr13-counter-review-20260923/candidate').resolve()
spec=importlib.util.spec_from_file_location('bench',ROOT/'examples/14-benchmarks/quiet-rerun-data/harness.py')
b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)
b.HERE=ROOT/'examples/14-benchmarks'
parser=argparse.ArgumentParser();parser.add_argument('--cases',nargs='+',default=['stream','sse','get','post-observe','single','stream-observe','websocket']);parser.add_argument('--output',type=Path,default=OUT/'runs');opts=parser.parse_args()
output=opts.output;output.mkdir(exist_ok=False)
env=dict(os.environ,LIBEV_FLAGS='8',PERL_FUTURE_NO_XS='1');env.pop('PAGI_FUTURE_XS',None);env.pop('NYTPROF',None)
probe='use PAGI::Server; use IO::Async::Loop::EV; use Future; use JSON::PP; print encode_json({version=>"$PAGI::Server::VERSION",server=>$INC{"PAGI/Server.pm"},adapter=>"$IO::Async::Loop::EV::VERSION",future=>ref(Future->new)});'
identities={}
for label,source in [('baseline',ROOT),('candidate',CAND)]:
 identities[label]=dict(loaded=json.loads(subprocess.check_output(['perl','-I'+str(source/'lib'),'-e',probe],env=env,text=True)),sha256={str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in sorted((source/'lib').rglob('*.pm'))})
 assert identities[label]['loaded']['adapter']=='0.05'
 assert identities[label]['loaded']['future']=='Future'
 prior=json.loads(Path('/tmp/pagi-pr13-counter-review-20260923/runs/identities.json').read_text())[label]
 assert prior==identities[label],label
normalized=[{str(Path(f).relative_to(source)):h for f,h in identities[label]['sha256'].items()} for label,source in [('baseline',ROOT),('candidate',CAND)]]
assert [f for f in normalized[0] if normalized[0][f]!=normalized[1][f]]==['lib/PAGI/Server/Connection.pm']
(output/'identities.json').write_text(json.dumps(identities,indent=2)+'\n')
args=SimpleNamespace(repo=CAND,baseline_repo=ROOT,output=output,run_id='pr13-counter-quiet',workers=1,concurrency=25,seconds=20,ws_connections=20)
for case in opts.cases:
 for n,variant in enumerate(('baseline','candidate','candidate','baseline')):
  load=subprocess.check_output(['top','-l','2','-s','1','-n','0'],text=True)
  (output/f'{case}-{n}-load.txt').write_text(load)
  with contextlib.redirect_stdout(io.StringIO()):
   b.run_one(args,variant,case,env,shutil.which('perl'),shutil.which('pagi-server'))
  row=json.loads((output/'results.jsonl').read_text().splitlines()[-1])
  timing=(output/(row['stamp']+'.process-time.txt')).read_text()
  m=re.search(r'([\d.]+) real\s+([\d.]+) user\s+([\d.]+) sys',timing);assert m,timing
  warmfile=output/(row['stamp']+'.warmup.txt')
  if warmfile.exists():
   total=row['responses']+b.parse_hey(warmfile.read_text())['responses']+(4 if case=='post-observe' else 3)
   cpu_unit='us/request including warmup/preflight'
  else:
   # WS output has measured messages only. Do not invent a count for warmup.
   total=None;cpu_unit='not normalized: warmup echo count unavailable'
  cpu=float(m[2])+float(m[3]);rss=int(re.search(r'(\d+)\s+maximum resident set size',timing)[1])/1048576
  extra=dict(case=case,variant=variant,stamp=row['stamp'],cpu_s=cpu,server_total_requests=total,cpu_us_per_request=cpu*1e6/total if total else None,rss_mib=rss,load_file=f'{case}-{n}-load.txt')
  with (output/'resources.jsonl').open('a') as f:f.write(json.dumps(extra)+'\n')
  print(json.dumps(dict(case=case,variant=variant,rate=row.get('rps',row.get('messages_per_second')),cpu_s=cpu,cpu_us_per_request=extra['cpu_us_per_request'],rss_mib=round(rss,2))),flush=True)
  for ident in identities.values():
   for f,h in ident['sha256'].items():assert hashlib.sha256(Path(f).read_bytes()).hexdigest()==h,f
print(f'All {4*len(opts.cases)} runs passed with unchanged source hashes.',flush=True)
