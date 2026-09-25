#!/usr/bin/env python3
"""CPAN/current comparison on a dedicated Linux benchmark VM.
Run under taskset -c 1. Server=CPU0; clients=CPU2,3. No runtime patches.
"""
import argparse,hashlib,importlib.util,json,os,subprocess,shutil,time
from pathlib import Path
from types import SimpleNamespace
root=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('harness',root/'linux-harness.py')
b=importlib.util.module_from_spec(spec);spec.loader.exec_module(b)
b.HERE=root/'work/examples/14-benchmarks'
a=argparse.ArgumentParser();a.add_argument('--mode',choices=['smoke','stability','compare'],required=True);opts=a.parse_args()
out=root/(opts.mode+'-'+str(time.time_ns()));out.mkdir()
env=dict(os.environ,PERL_FUTURE_NO_XS='1',LIBEV_FLAGS='4',GOMAXPROCS='2')
for k in ('PAGI_FUTURE_XS','NYTPROF','PERL5OPT','PERL5LIB'):env.pop(k,None)
perl=shutil.which('perl');installed=shutil.which('pagi-server')
assert perl and installed and shutil.which('hey')
manifest=json.loads((root/'manifest.json').read_text())
def verify():
 for name,digest in manifest.items():assert hashlib.sha256((root/name).read_bytes()).hexdigest()==digest,name
verify()
probe=r'use JSON::PP; use PAGI::Server; use IO::Async; use IO::Async::Loop::EV; use Future; use Future::AsyncAwait; use EV; use HTTP::Parser::XS; use Protocol::WebSocket; my %v; for my $m (qw(PAGI::Server IO::Async IO::Async::Loop::EV Future Future::AsyncAwait EV HTTP::Parser::XS Protocol::WebSocket)) { no strict "refs"; $v{$m}="".${"${m}::VERSION"} } print encode_json({versions=>\%v,perl=>"$^V",loaded=>\%INC,future=>ref(Future->new),ev_backend=>EV::backend(),epoll=>EV::BACKEND_EPOLL()});'
meta={'mode':opts.mode,'env':{k:env.get(k) for k in ('LIBEV_FLAGS','PERL_FUTURE_NO_XS','GOMAXPROCS','PERL5LIB')},'uname':list(os.uname()),'affinity':{'server':'0','client':'2,3','driver':sorted(os.sched_getaffinity(0))},'lscpu':subprocess.check_output(['lscpu'],text=True),'manifest':manifest,'harness_sha256':hashlib.sha256((root/'linux-harness.py').read_bytes()).hexdigest()}
for label,extra in [('release',[]),('current',['-I'+str(root/'work/lib')])]:
 result=json.loads(subprocess.check_output([perl,*extra,'-e',probe],env=env,text=True));assert result['future']=='Future';assert result['versions']['IO::Async::Loop::EV']=='0.05';meta[label]=result
meta['loaded_sha256']={p:hashlib.sha256(Path(p).read_bytes()).hexdigest() for v in ('release','current') for p in meta[v]['loaded'].values() if Path(p).is_file()}
(out/'metadata.json').write_text(json.dumps(meta,indent=2)+'\n')
seconds=2 if opts.mode=='smoke' else 15
args=SimpleNamespace(repo=root/'work',baseline_repo=None,output=out,run_id=out.name,workers=1,concurrency=25,seconds=seconds,ws_connections=20)
cases=['get','headers','post-observe','stream-observe','sse','websocket']
if opts.mode=='stability':cases=['post-observe']
order=['release','main'] if opts.mode=='smoke' else (['release']*4 if opts.mode=='stability' else ['release','main','main','release'])
print('OUTPUT '+str(out),flush=True)
with (out/'vmstat.txt').open('w') as vm, (out/'pidstat.txt').open('w') as pid:
 monitors=[subprocess.Popen(['vmstat','-w','1'],stdout=vm),subprocess.Popen(['pidstat','-h','-u','-r','-w','-C','perl|hey','1'],stdout=pid)]
 try:
  for case in cases:
   for variant in order:
    b.run_one(args,variant,case,env,perl,installed)
    verify()
    for name,digest in meta['loaded_sha256'].items():assert hashlib.sha256(Path(name).read_bytes()).hexdigest()==digest,name
 finally:
  for m in monitors:m.terminate()
  for m in monitors:m.wait(timeout=5)
print('Completed with unchanged source/dependency hashes.',flush=True)
