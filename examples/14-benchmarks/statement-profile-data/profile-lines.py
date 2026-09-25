import hashlib, importlib.util, json, os, subprocess
from pathlib import Path
from types import SimpleNamespace
ROOT=Path('/Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/experiment-http-simplification')
OUT=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('profile',ROOT/'examples/14-benchmarks/profiling-data/profile.py')
p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)
snapshot={str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in sorted((ROOT/'lib').rglob('*.pm'))}
(OUT/'current-hashes.json').write_text(json.dumps(snapshot,indent=2)+'\n')
for i,variant in enumerate(['release','experiment','experiment','release']):
    args=SimpleNamespace(output=OUT/f'run-{i}',mode='line',requests=2000,concurrency=25,watch_idle=False,app=None)
    p.run(args,variant,'get')
    profile=args.output/f'{variant}-get'
    with (profile/'lines.json').open('w') as out:
        subprocess.run(['perl',str(OUT/'extract-lines.pl'),str(profile/'nytprof.out')],stdout=out,check=True)
    for f,h in snapshot.items():assert hashlib.sha256(Path(f).read_bytes()).hexdigest()==h,f
    meta=json.loads((profile/'metadata.json').read_text())
    for f,h in meta['server_hashes'].items():assert hashlib.sha256(Path(f).read_bytes()).hexdigest()==h,f
print('Four statement profiles completed; tracked runtime hashes unchanged.',flush=True)
