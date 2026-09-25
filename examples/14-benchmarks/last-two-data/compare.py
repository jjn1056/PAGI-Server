#!/usr/bin/env python3
import argparse, hashlib, importlib.util, json, os, shutil, subprocess
from pathlib import Path
from types import SimpleNamespace
root=Path('/home/ubuntu/pagi-benchmark')
exp=root/'last-two-20260925'
spec=importlib.util.spec_from_file_location('harness',root/'linux-harness.py')
b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)
b.HERE=root/'work/examples/14-benchmarks'
p=argparse.ArgumentParser();p.add_argument('--smoke',action='store_true');opts=p.parse_args()
out=exp/('smoke' if opts.smoke else 'timed');out.mkdir(exist_ok=False)
env=dict(os.environ,PERL_FUTURE_NO_XS='1',LIBEV_FLAGS='4',GOMAXPROCS='2')
for k in ['PAGI_FUTURE_XS','NYTPROF','PERL5OPT','PERL5LIB']:env.pop(k,None)
sources={'release':None,**{k:exp/k for k in ['before','headers','send']}}
hashes={label:{str(f.relative_to(path)):hashlib.sha256(f.read_bytes()).hexdigest() for d in ['lib','bin'] for f in (path/d).rglob('*') if f.is_file()} for label,path in sources.items() if path}
expected=json.loads((exp/'identities.json').read_text());assert hashes==expected
perl,installed=shutil.which('perl'),shutil.which('pagi-server')
probe='use JSON::PP; use PAGI::Server; use IO::Async::Loop::EV; use Future; use EV; print encode_json({version=>$PAGI::Server::VERSION,loop_ev=>"$IO::Async::Loop::EV::VERSION",perl=>"$^V",future=>ref(Future->new),backend=>EV::backend(),loaded=>\\%INC});'
meta={'base':'efbed54','runtime_base':'874b120','sources':hashes,'variants':{},'env':{k:env.get(k) for k in ['LIBEV_FLAGS','PERL_FUTURE_NO_XS','GOMAXPROCS']},'server_cpu':0,'client_cpus':[2,3],'driver_cpus':sorted(os.sched_getaffinity(0))}
for label,path in sources.items():
 meta['variants'][label]=json.loads(subprocess.check_output([perl,*(['-I'+str(path/'lib')] if path else []),'-e',probe],env=env,text=True))
 assert meta['variants'][label]['future']=='Future'
 assert meta['variants'][label]['backend']==4
 assert meta['variants'][label]['loop_ev']=='0.05'
loaded={f:hashlib.sha256(Path(f).read_bytes()).hexdigest() for variant in meta['variants'].values() for f in variant['loaded'].values() if Path(f).is_file()}
meta['loaded_sha256']=loaded
meta['scripts_sha256']={str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in [Path(__file__),root/'linux-harness.py',*b.HERE.glob('*.pl')]}
cases=['get','headers','post-observe','stream-observe','sse','websocket']
orders=[['release','before','headers','send'],['send','headers','before','release'],['before','release','send','headers']]
meta.update(cases=cases,orders=orders,seconds=2 if opts.smoke else 10,smoke=opts.smoke)
(out/'metadata.json').write_text(json.dumps(meta,indent=2)+'\n')
args=SimpleNamespace(repo=root/'work',baseline_repo=None,output=out,run_id=out.name,workers=1,concurrency=25,seconds=meta['seconds'],ws_connections=20)
with (out/'vmstat.txt').open('w') as vm,(out/'pidstat.txt').open('w') as pid:
 monitors=[subprocess.Popen(['vmstat','-w','1'],stdout=vm),subprocess.Popen(['pidstat','-h','-u','-r','-w','1'],stdout=pid)]
 try:
  for case in cases:
   use_orders=orders[:1] if opts.smoke else orders[:2] if case in ['sse','websocket'] else orders
   for order in use_orders:
    for label in order:
     args.repo=sources[label] or root/'work'
     b.run_one(args,label,case,env,perl,installed)
     for path,digest in loaded.items():assert hashlib.sha256(Path(path).read_bytes()).hexdigest()==digest,path
  for label,path in sources.items():
   if path:
    for f,digest in hashes[label].items():assert hashlib.sha256((path/f).read_bytes()).hexdigest()==digest,f
 finally:
  for m in monitors:m.terminate()
  for m in monitors:m.wait(timeout=5)
print('Completed with unchanged source and dependency hashes.',flush=True)
