import argparse, hashlib, http.client, importlib.util, json, os
from pathlib import Path
import shutil, socket, subprocess, time

ROOT=Path('/home/ubuntu/pagi-benchmark')
EXP=ROOT/'canonical-874b120'
HERE=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('bench',EXP/'examples/14-benchmarks/run.py')
bench=importlib.util.module_from_spec(spec);spec.loader.exec_module(bench)

def run(args,variant,case):
    out=args.output/f'{variant}-{case}'
    out.mkdir(parents=True,exist_ok=False)
    perl=shutil.which('perl')
    source={'release':None,'current':EXP}[variant]
    env=dict(os.environ,LIBEV_FLAGS='4',PERL_FUTURE_NO_XS='1',GOMAXPROCS='2')
    env.pop('PAGI_FUTURE_XS',None)
    env.pop('NYTPROF',None)
    if args.mode!='off':
        env['NYTPROF']=f'file={out}/nytprof.out:stmts={int(args.mode=="line")}:calls=0:slowops=0:compress=1:start=init:clock=2'
    with socket.socket() as sock:
        sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
    command=[perl]+(['-d:NYTProf'] if args.mode!='off' else [])
    command+=['-I'+str(source/'lib'),str(source/'bin/pagi-server')] if source else [shutil.which('pagi-server')]
    base,app,path=bench.case_config(case)
    appfile=args.app or EXP/'examples/14-benchmarks'/f'{app}.pl'
    command+=['--loop','EV','--workers','1','--env','production','--port',str(port),str(appfile)]
    command=['taskset','-c','0','/usr/bin/time','-f','user_s=%U system_s=%S maxrss_kib=%M major_faults=%F','-o',str(out/'process-time.txt'),*command]
    metadata=dict(checkpoint='874b1201c58ea97b1eaf67e50fe9c4449ae7e2ba' if source else 'CPAN 0.002013', cpu_clock='CLOCK_PROCESS_CPUTIME_ID=2',command=command,mode=args.mode,requests=args.requests,warmup_requests=200,
        preflight_requests=3, split_post_preflight=False,concurrency=args.concurrency,
        app_sha256=hashlib.sha256(appfile.read_bytes()).hexdigest(),
        env={k:env.get(k) for k in ['NYTPROF','LIBEV_FLAGS','PERL_FUTURE_NO_XS','PERL5LIB','PERL5OPT','PAGI_FUTURE_XS']})
    probe='use PAGI::Server; use Future; use JSON::PP; print encode_json({version=>$PAGI::Server::VERSION,inc=>\\%INC,future_class=>ref(Future->new)});'
    metadata['loaded']=json.loads(subprocess.check_output([perl]+(['-I'+str(source/'lib')] if source else [])+['-e',probe],env={**env,'NYTPROF':'start=no'},text=True))
    libdir=Path(metadata['loaded']['inc']['PAGI/Server.pm']).parent
    metadata['server_hashes']={str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in [libdir/'Server.pm',*sorted((libdir/'Server').rglob('*.pm'))]}
    (out/'metadata.json').write_text(json.dumps(metadata,indent=2)+'\n')
    with (out/'server.log').open('w') as log:
        proc=subprocess.Popen(command,cwd=out,env=env,stdout=log,stderr=log,start_new_session=True)
        try:
            for _ in range(150):
                if proc.poll() is not None: raise RuntimeError(f'Server exited: {out}')
                try:
                    with socket.create_connection(('127.0.0.1',port),timeout=.1): break
                except OSError: time.sleep(.1)
            else: raise RuntimeError('Server did not listen')
            # NYTProf aborts when AsyncAwait resumes the split-body preflight.
            # Profile the buffered POST hot path; split-body correctness is
            # already covered by the uninstrumented benchmark and test suite.
            conn=http.client.HTTPConnection('127.0.0.1',port,timeout=10)
            try:
                for _ in range(3):
                    conn.request('POST' if base=='post' else 'GET',path,
                                 body=b'x'*1024 if base=='post' else None)
                    bench.check_response(base,conn.getresponse())
            finally:
                conn.close()
            client=['taskset','-c','2,3',shutil.which('hey'),'-c',str(args.concurrency)]
            if base=='post':client+=['-m','POST','-d','x'*1024]
            url=f'http://127.0.0.1:{port}{path}'
            for label,count in [('warmup',200),('measured',args.requests)]:
                result=subprocess.run(client+['-n',str(count),url],env=env,capture_output=True,text=True,timeout=240)
                (out/(label+'.txt')).write_text(result.stdout+result.stderr)
                result.check_returncode();metrics=bench.parse_hey(result.stdout)
                if metrics['responses']!=count:raise RuntimeError('Unexpected request count')
            row=dict(variant=variant,case=case,mode=args.mode,**metrics)
        finally:
            if proc.poll() is None:
                bench.stop_server(proc)
    if args.mode!='off':
        result=subprocess.run([perl,str(HERE/'extract.pl'),str(out/'nytprof.out')],capture_output=True,text=True)
        (out/'extract.stderr').write_text(result.stderr)
        result.check_returncode()
        rows=json.loads(result.stdout)
        if not rows:raise RuntimeError('Empty profile')
        (out/'subs.json').write_text(result.stdout)
        row['profile_subroutines']=len(rows)
        if args.mode=='line':
            result=subprocess.run([perl,str(HERE/'extract-lines.pl'),str(out/'nytprof.out')],capture_output=True,text=True)
            (out/'extract-lines.stderr').write_text(result.stderr)
            result.check_returncode()
            (out/'lines.json').write_text(result.stdout)
    (out/'result.json').write_text(json.dumps(row,indent=2)+'\n')
    print(json.dumps(row),flush=True)

if __name__=='__main__':
    p=argparse.ArgumentParser()
    p.add_argument('--output',type=Path,required=True)
    p.add_argument('--mode',choices=['off','sub','line'],default='sub')
    p.add_argument('--requests',type=int,default=5000)
    p.add_argument('--concurrency',type=int,default=25)
    p.add_argument('--app',type=Path)
    p.add_argument('--cases',nargs='+',default=['get','post-observe','stream'])
    p.add_argument('--variants',nargs='+',default=['release','current'])
    args=p.parse_args()
    args.output=args.output.resolve()
    for case in args.cases:
        for variant in args.variants:run(args,variant,case)
