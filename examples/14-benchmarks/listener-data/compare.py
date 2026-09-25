import argparse, hashlib, importlib.util, json, os, shutil, subprocess
from pathlib import Path
from types import SimpleNamespace
root=Path('/home/ubuntu/pagi-benchmark');exp=root/'listener-20260925'
spec=importlib.util.spec_from_file_location('harness',exp/'listener-harness.py')
b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)
b.HERE=root/'work/examples/14-benchmarks'
p=argparse.ArgumentParser();p.add_argument('--smoke',action='store_true');opt=p.parse_args()
out=exp/('smoke' if opt.smoke else 'timed');out.mkdir(exist_ok=False)
env=dict(os.environ,PERL_FUTURE_NO_XS='1',LIBEV_FLAGS='4',GOMAXPROCS='2')
for k in ['PAGI_FUTURE_XS','NYTPROF','PERL5OPT','PERL5LIB']:env.pop(k,None)
sources={'release':None,'before':exp/'before','listener':exp/'listener'}
hashes={label:{str(f.relative_to(path)):hashlib.sha256(f.read_bytes()).hexdigest() for d in ['lib','bin'] for f in (path/d).rglob('*') if f.is_file()} for label,path in sources.items() if path}
assert hashes==json.loads((exp/'identities.json').read_text())
perl,installed=shutil.which('perl'),shutil.which('pagi-server')
probe='use JSON::PP; use PAGI::Server; use IO::Async::Loop::EV; use Future; use EV; print encode_json({version=>$PAGI::Server::VERSION,loop_ev=>"$IO::Async::Loop::EV::VERSION",perl=>"$^V",future=>ref(Future->new),backend=>EV::backend(),loaded=>\\%INC});'
meta={'base':'2e42135','runtime_base':'cdf4a7c','pr_source':'7ce3c0be04037fc154aec05c993b935dd7340b53','sources':hashes,'variants':{},'env':{k:env.get(k) for k in ['LIBEV_FLAGS','PERL_FUTURE_NO_XS','GOMAXPROCS']},'server_cpu':0,'client_cpus':[2,3],'driver_cpus':sorted(os.sched_getaffinity(0))}
for label,path in sources.items():
 meta['variants'][label]=json.loads(subprocess.check_output([perl,*(['-I'+str(path/'lib')] if path else []),'-e',probe],env=env,text=True))
 assert meta['variants'][label]['future']=='Future'
 assert meta['variants'][label]['backend']==4
 assert meta['variants'][label]['loop_ev']=='0.05'
loaded={f:hashlib.sha256(Path(f).read_bytes()).hexdigest() for variant in meta['variants'].values() for f in variant['loaded'].values() if Path(f).is_file()}
meta['loaded_sha256']=loaded
meta['scripts_sha256']={str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in [Path(__file__),exp/'listener-harness.py',*b.HERE.glob('*.pl')]}
cases=['storm','get-500','churn','get','post-observe','stream-observe','sse','websocket']
orders=[['release','before','listener'],['listener','before','release'],['before','release','listener'],['listener','release','before'],['before','listener','release']]
meta.update(cases=cases,orders=orders,smoke=opt.smoke,settings={'storm':{'rounds':5,'background_seconds':30,'concurrency':500,'background_concurrency':100},'get-500':{'rounds':3,'seconds':20,'concurrency':500},'churn':{'rounds':3,'seconds':15,'concurrency':100},'controls':{'rounds':2,'seconds':10,'concurrency':25,'ws_connections':20}})
meta['somaxconn']=subprocess.check_output(['sysctl','-n','net.core.somaxconn'],text=True).strip()
(out/'metadata.json').write_text(json.dumps(meta,indent=2)+'\n')
args=SimpleNamespace(repo=root/'work',baseline_repo=None,output=out,run_id=out.name,workers=1,concurrency=25,seconds=10,ws_connections=20,smoke=opt.smoke)
with (out/'vmstat.txt').open('w') as vm,(out/'pidstat.txt').open('w') as pid:
 monitors=[subprocess.Popen(['vmstat','-w','1'],stdout=vm),subprocess.Popen(['pidstat','-h','-u','-r','-w','1'],stdout=pid)]
 try:
  for case in cases:
   rounds=1 if opt.smoke else 5 if case=='storm' else 3 if case in ['get-500','churn'] else 2
   args.seconds=(5 if case=='storm' else 2) if opt.smoke else 30 if case=='storm' else 20 if case=='get-500' else 15 if case=='churn' else 10
   args.concurrency=50 if opt.smoke and case=='storm' else 500 if case in ['storm','get-500'] else 100 if case=='churn' else 25
   for order in orders[:rounds]:
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
