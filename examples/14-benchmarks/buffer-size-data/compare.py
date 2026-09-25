import argparse, hashlib, importlib.util, json, os, shutil, subprocess
from pathlib import Path
from types import SimpleNamespace
root=Path('/home/ubuntu/pagi-benchmark');exp=root/'buffers-20260925'
spec=importlib.util.spec_from_file_location('harness',exp/'buffer-harness.py')
b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)
b.HERE=exp/'apps'
p=argparse.ArgumentParser();p.add_argument('--smoke',action='store_true');opt=p.parse_args()
out=exp/('smoke' if opt.smoke else 'timed');out.mkdir(exist_ok=False)
env=dict(os.environ,PERL_FUTURE_NO_XS='1',LIBEV_FLAGS='4',GOMAXPROCS='2')
for k in ['PAGI_FUTURE_XS','NYTPROF','PERL5OPT','PERL5LIB']:env.pop(k,None)
sources={'release':None,'before':exp/'before','buffers64':exp/'buffers64'}
hashes={label:{str(f.relative_to(path)):hashlib.sha256(f.read_bytes()).hexdigest() for d in ['lib','bin'] for f in (path/d).rglob('*') if f.is_file()} for label,path in sources.items() if path}
assert hashes==json.loads((exp/'identities.json').read_text())
perl,installed=shutil.which('perl'),shutil.which('pagi-server')
probe='use JSON::PP; use PAGI::Server; use IO::Async::Loop::EV; use Future; use EV; print encode_json({version=>$PAGI::Server::VERSION,loop_ev=>"$IO::Async::Loop::EV::VERSION",perl=>"$^V",future=>ref(Future->new),backend=>EV::backend(),loaded=>\\%INC});'
meta={'base':'5c960b2','runtime_base':'5c960b2','change':'read_len and write_len 8192 -> 65536 only','sources':hashes,'variants':{},'env':{k:env.get(k) for k in ['LIBEV_FLAGS','PERL_FUTURE_NO_XS','GOMAXPROCS']},'server_cpu':0,'client_cpus':[2,3],'driver_cpus':sorted(os.sched_getaffinity(0))}
for label,path in sources.items():
 meta['variants'][label]=json.loads(subprocess.check_output([perl,*(['-I'+str(path/'lib')] if path else []),'-e',probe],env=env,text=True))
 assert meta['variants'][label]['future']=='Future'
 assert meta['variants'][label]['backend']==4
 assert meta['variants'][label]['loop_ev']=='0.05'
loaded={f:hashlib.sha256(Path(f).read_bytes()).hexdigest() for variant in meta['variants'].values() for f in variant['loaded'].values() if Path(f).is_file()}
meta['loaded_sha256']=loaded
meta['scripts_sha256']={str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in [Path(__file__),exp/'buffer-harness.py',*b.HERE.glob('*.pl')]}
cases=['post-large','single','stream-observe','get-500','mixed','get','post-observe','sse','websocket']
orders=[['release','before','buffers64'],['buffers64','before','release'],['before','release','buffers64']]
meta.update(cases=cases,orders=orders,smoke=opt.smoke,settings={'primary_rounds':3,'control_rounds':2,'seconds':10,'get_500_seconds':15,'upload_bytes':1048576,'mixed_background_clients':100,'mixed_background_seconds_extra':2,'mixed_background_lead_seconds':0.5})
meta['somaxconn']=subprocess.check_output(['sysctl','-n','net.core.somaxconn'],text=True).strip()
(out/'metadata.json').write_text(json.dumps(meta,indent=2)+'\n')
args=SimpleNamespace(repo=root/'work',baseline_repo=None,output=out,run_id=out.name,workers=1,concurrency=25,seconds=10,ws_connections=20,smoke=opt.smoke)
with (out/'vmstat.txt').open('w') as vm,(out/'pidstat.txt').open('w') as pid:
 monitors=[subprocess.Popen(['vmstat','-w','1'],stdout=vm),subprocess.Popen(['pidstat','-h','-u','-r','-w','1'],stdout=pid)]
 try:
  for case in cases:
   rounds=1 if opt.smoke else 2 if case in ['get','post-observe','sse','websocket'] else 3
   args.seconds=2 if opt.smoke else 15 if case=='get-500' else 10
   args.concurrency=500 if case=='get-500' else 25
   b.POST_BYTES=1048576 if case=='post-large' else 1024
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
