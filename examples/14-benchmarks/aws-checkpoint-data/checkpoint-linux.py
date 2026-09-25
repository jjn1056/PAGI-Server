#!/usr/bin/env python3
"""Compare fixed historical checkpoints using the existing Linux harness."""
import argparse,hashlib,importlib.util,json,os,subprocess,shutil,time
from pathlib import Path
from types import SimpleNamespace
root=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('harness',root/'linux-harness.py')
b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)
b.HERE=root/'work/examples/14-benchmarks'
a=argparse.ArgumentParser();a.add_argument('--mode',choices=['smoke','compare'],required=True);opts=a.parse_args()
out=root/('checkpoint-'+opts.mode+'-'+str(time.time_ns()));out.mkdir()
env=dict(os.environ,PERL_FUTURE_NO_XS='1',LIBEV_FLAGS='4',GOMAXPROCS='2')
for k in ('PAGI_FUTURE_XS','NYTPROF','PERL5OPT','PERL5LIB'):env.pop(k,None)
perl=shutil.which('perl');installed=shutil.which('pagi-server')
assert perl and installed and shutil.which('hey')
identities=json.loads((root/'checkpoint-identities.json').read_text())
sources={'release':None,**{k:root/'checkpoints'/k for k in ('pre','saved','cleanup','current')}}
manifest=json.loads((root/'manifest.json').read_text())
def verify():
 for name,digest in manifest.items():assert hashlib.sha256((root/name).read_bytes()).hexdigest()==digest,name
 for label,identity in identities.items():
  for name,digest in identity['sha256'].items():assert hashlib.sha256((sources[label]/name).read_bytes()).hexdigest()==digest,(label,name)
verify()
probe='use JSON::PP; use PAGI::Server; use IO::Async; use IO::Async::Loop::EV; use Future; use Future::AsyncAwait; use EV; use HTTP::Parser::XS; use Protocol::WebSocket; my %v; for my $m (qw(PAGI::Server IO::Async IO::Async::Loop::EV Future Future::AsyncAwait EV HTTP::Parser::XS Protocol::WebSocket)) { no strict "refs"; $v{$m}="".${"${m}::VERSION"} } print encode_json({versions=>\\%v,perl=>"$^V",loaded=>\\%INC,future=>ref(Future->new),ev_backend=>EV::backend(),epoll=>EV::BACKEND_EPOLL()});'
orders=[['release','pre','saved','cleanup','current'],['current','cleanup','saved','pre','release'],['pre','current','release','cleanup','saved']]
cases=['get','headers','post-observe','stream-observe']
meta={'mode':opts.mode,'orders':orders,'cases':cases,'env':{k:env.get(k) for k in ('LIBEV_FLAGS','PERL_FUTURE_NO_XS','GOMAXPROCS','PERL5LIB')},'uname':list(os.uname()),'affinity':{'server':'0','client':'2,3','driver':sorted(os.sched_getaffinity(0))},'lscpu':subprocess.check_output(['lscpu'],text=True),'identities':identities,'manifest':manifest,'harness_sha256':hashlib.sha256((root/'linux-harness.py').read_bytes()).hexdigest(),'driver_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'variants':{}}
for label,source in sources.items():
 extra=['-I'+str(source/'lib')] if source else []
 result=json.loads(subprocess.check_output([perl,*extra,'-e',probe],env=env,text=True))
 assert result['future']=='Future';assert result['versions']['IO::Async::Loop::EV']=='0.05';assert result['ev_backend']==4
 if source:assert result['loaded']['PAGI/Server.pm']==str(source/'lib/PAGI/Server.pm'),label
 meta['variants'][label]=result
meta['loaded_sha256']={p:hashlib.sha256(Path(p).read_bytes()).hexdigest() for v in meta['variants'].values() for p in v['loaded'].values() if Path(p).is_file()}
(out/'metadata.json').write_text(json.dumps(meta,indent=2)+'\n')
seconds=2 if opts.mode=='smoke' else 15
args=SimpleNamespace(repo=root/'work',baseline_repo=None,output=out,run_id=out.name,workers=1,concurrency=25,seconds=seconds,ws_connections=20)
if opts.mode=='smoke':orders=orders[:1]
print('OUTPUT '+str(out),flush=True)
with (out/'vmstat.txt').open('w') as vm, (out/'pidstat.txt').open('w') as pid:
 monitors=[subprocess.Popen(['vmstat','-w','1'],stdout=vm),subprocess.Popen(['pidstat','-h','-u','-r','-w','1'],stdout=pid)]
 try:
  for case in cases:
   for order in orders:
    for variant in order:
     args.repo=sources[variant] or root/'work'
     b.run_one(args,variant,case,env,perl,installed)
     verify()
     for name,digest in meta['loaded_sha256'].items():assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==digest,name
     assert hashlib.sha256(Path(__file__).read_bytes()).hexdigest()==meta['driver_sha256']
     assert hashlib.sha256((root/'linux-harness.py').read_bytes()).hexdigest()==meta['harness_sha256']
 finally:
  for m in monitors:m.terminate()
  for m in monitors:m.wait(timeout=5)
print('Completed with unchanged source/dependency/harness hashes.',flush=True)
