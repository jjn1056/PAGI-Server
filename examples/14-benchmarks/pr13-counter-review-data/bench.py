import importlib.util,json,os,subprocess,hashlib,shutil
from pathlib import Path
from types import SimpleNamespace
ROOT=Path('/Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/experiment-http-simplification')
OUT=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('bench',ROOT/'examples/14-benchmarks/run.py')
b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)
output=OUT/'runs';output.mkdir(exist_ok=False)
env=dict(os.environ,LIBEV_FLAGS='8',PERL_FUTURE_NO_XS='1');env.pop('PAGI_FUTURE_XS',None);env.pop('NYTPROF',None)
probe='use PAGI::Server; use IO::Async::Loop::EV; use Future; use JSON::PP; print encode_json({version=>"$PAGI::Server::VERSION",server=>$INC{"PAGI/Server.pm"},adapter=>"$IO::Async::Loop::EV::VERSION",future=>ref(Future->new)});'
identities={}
for label,source in [('baseline',ROOT),('candidate',OUT/'candidate')]:
 identities[label]=dict(loaded=json.loads(subprocess.check_output(['perl','-I'+str(source/'lib'),'-e',probe],env=env,text=True)),sha256={str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in sorted((source/'lib').rglob('*.pm'))})
 assert identities[label]['loaded']['adapter']=='0.05'
 assert identities[label]['loaded']['future']=='Future'
(output/'identities.json').write_text(json.dumps(identities,indent=2)+'\n')
args=SimpleNamespace(repo=OUT/'candidate',baseline_repo=ROOT,output=output,run_id='pr13-counter-only',workers=1,concurrency=25,seconds=10,ws_connections=20)
for case in ('get','post-observe','stream','sse','websocket'):
 for variant in ('baseline','candidate','candidate','baseline'):
  b.run_one(args,variant,case,env,shutil.which('perl'),shutil.which('pagi-server'))
  for ident in identities.values():
   for f,h in ident['sha256'].items():assert hashlib.sha256(Path(f).read_bytes()).hexdigest()==h,f
print('All twenty runs completed with unchanged sources.',flush=True)
