import importlib.util,json,os,subprocess,hashlib,shutil,re,contextlib,io
from pathlib import Path
from types import SimpleNamespace
ROOT=Path('/Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/experiment-http-simplification')
OUT=Path(__file__).resolve().parent
BEFORE=OUT/'before'
spec=importlib.util.spec_from_file_location('bench',ROOT/'examples/14-benchmarks/quiet-rerun-data/harness.py')
b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)
b.HERE=ROOT/'examples/14-benchmarks'
output=OUT/'stream-recheck';output.mkdir(exist_ok=False)
env=dict(os.environ,LIBEV_FLAGS='8',PERL_FUTURE_NO_XS='1');env.pop('PAGI_FUTURE_XS',None);env.pop('NYTPROF',None)
probe='use PAGI::Server; use IO::Async::Loop::EV; use Future; use JSON::PP; print encode_json({version=>"$PAGI::Server::VERSION",server=>$INC{"PAGI/Server.pm"},connection=>$INC{"PAGI/Server/Connection.pm"},adapter=>"$IO::Async::Loop::EV::VERSION",future=>ref(Future->new)});'
identities={}
for label,source in [('release',None),('baseline',BEFORE),('candidate',ROOT)]:
 loaded=json.loads(subprocess.check_output(['perl']+(['-I'+str(source/'lib')] if source else [])+['-e',probe],env=env,text=True))
 assert loaded['adapter']=='0.05';assert loaded['future']=='Future'
 files=sorted((source/'lib').rglob('*.pm')) if source else [Path(loaded['server']),Path(loaded['connection'])]
 identities[label]=dict(loaded=loaded,sha256={str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in files})
normalized=[{str(Path(f).relative_to(source)):h for f,h in identities[label]['sha256'].items()} for label,source in [('baseline',BEFORE),('candidate',ROOT)]]
assert [f for f in normalized[0] if normalized[0][f]!=normalized[1][f]]==['lib/PAGI/Server/Connection.pm']
(output/'identities.json').write_text(json.dumps(identities,indent=2)+'\n')
args=SimpleNamespace(repo=ROOT,baseline_repo=None,output=output,run_id='completion-cleanup',workers=1,concurrency=25,seconds=20,ws_connections=20)
for case in ['stream-observe']:
 for n,variant in enumerate(('release','candidate','baseline','baseline','candidate','release')):
  load=subprocess.check_output(['top','-l','2','-s','1','-n','0'],text=True)
  (output/f'{case}-{n}-load.txt').write_text(load)
  args.baseline_repo=BEFORE if variant=='baseline' else None
  with contextlib.redirect_stdout(io.StringIO()):
   b.run_one(args,variant,case,env,shutil.which('perl'),shutil.which('pagi-server'))
  row=json.loads((output/'results.jsonl').read_text().splitlines()[-1])
  timing=(output/(row['stamp']+'.process-time.txt')).read_text()
  m=re.search(r'([\d.]+) real\s+([\d.]+) user\s+([\d.]+) sys',timing);assert m,timing
  warmfile=output/(row['stamp']+'.warmup.txt')
  total=row['responses']+b.parse_hey(warmfile.read_text())['responses']+(4 if case=='post-observe' else 3)
  cpu=float(m[2])+float(m[3]);rss=int(re.search(r'(\d+)\s+maximum resident set size',timing)[1])/1048576
  extra=dict(case=case,variant=variant,stamp=row['stamp'],cpu_s=cpu,server_total_requests=total,cpu_us_per_request=cpu*1e6/total,rss_mib=rss,load_file=f'{case}-{n}-load.txt')
  with (output/'resources.jsonl').open('a') as f:f.write(json.dumps(extra)+'\n')
  print(json.dumps(dict(case=case,variant=variant,rate=row['rps'],cpu_us_per_request=extra['cpu_us_per_request'],rss_mib=round(rss,2))),flush=True)
  for ident in identities.values():
   for f,h in ident['sha256'].items():assert hashlib.sha256(Path(f).read_bytes()).hexdigest()==h,f
print('All 6 runs passed with unchanged source hashes.',flush=True)
