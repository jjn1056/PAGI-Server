import argparse, hashlib, importlib.util, json, os, shutil, subprocess
from pathlib import Path
from types import SimpleNamespace

HERE=Path(__file__).resolve().parent
EXP=Path('/Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/experiment-http-simplification')
spec=importlib.util.spec_from_file_location('profile_harness',EXP/'examples/14-benchmarks/profiling-data/profile.py')
harness=importlib.util.module_from_spec(spec);spec.loader.exec_module(harness)
p=argparse.ArgumentParser();p.add_argument('phase',choices=['bench','profile','confirm']);args=p.parse_args()
probe='use IO::Async::Loop::EV; use JSON::PP; print encode_json({version=>"$IO::Async::Loop::EV::VERSION",path=>$INC{"IO/Async/Loop/EV.pm"}})'
adapter=json.loads(subprocess.check_output([shutil.which('perl'),'-e',probe],text=True))
assert adapter['version']=='0.05',adapter
assert '/tmp/' not in adapter['path'] and '/private/tmp/' not in adapter['path'],adapter
adapter['sha256']=hashlib.sha256(Path(adapter['path']).read_bytes()).hexdigest()
(HERE/'adapter.json').write_text(json.dumps(adapter,indent=2)+'\n')
if args.phase=='bench':
    cases=['get','post-observe','stream']
    order=['release','main','experiment','experiment','main','release']
elif args.phase=='profile':
    cases=['get','stream'];order=['release','main','experiment']
else:
    cases=['get'];order=['release','main','main','release']
for case in cases:
    for index,variant in enumerate(order):
        out=HERE/args.phase/f'{case}-{index}-{variant}'
        requests=30000 if args.phase=='confirm' else ((5000 if case=='stream' else 10000) if args.phase=='bench' else (500 if case=='stream' else 2000))
        options=SimpleNamespace(output=out,mode='sub' if args.phase=='profile' else 'off',requests=requests,
            concurrency=25,watch_idle=False,app=None)
        print(f'START {args.phase} {case} {index} {variant} requests={requests}',flush=True)
        harness.run(options,variant,case)
        assert hashlib.sha256(Path(adapter['path']).read_bytes()).hexdigest()==adapter['sha256']
