import hashlib, importlib.util, json, os, subprocess
from pathlib import Path
from types import SimpleNamespace
ROOT=Path('/Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/experiment-http-simplification')
OUT=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('profile',ROOT/'examples/14-benchmarks/profiling-data/profile.py')
p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()
assert head.startswith('b995443'),head
snapshot={str(f):hashlib.sha256(f.read_bytes()).hexdigest() for f in sorted((ROOT/'lib').rglob('*.pm'))}
probe='use IO::Async::Loop::EV; use EV; use Future; use JSON::PP; print encode_json({adapter=>"$IO::Async::Loop::EV::VERSION",ev=>"$EV::VERSION",future=>ref(Future->new),adapter_file=>$INC{"IO/Async/Loop/EV.pm"}});'
env=dict(os.environ,PERL_FUTURE_NO_XS='1');env.pop('PAGI_FUTURE_XS',None)
identity=json.loads(subprocess.check_output(['perl','-e',probe],env=env,text=True))
assert identity['adapter']=='0.05',identity
identity['adapter_sha256']=hashlib.sha256(Path(identity['adapter_file']).read_bytes()).hexdigest()
(OUT/'checkpoint.json').write_text(json.dumps(dict(head=head,source_hashes=snapshot,dependencies=identity),indent=2)+'\n')
for case,n in [('get',2000),('stream',500)]:
    for i,variant in enumerate(['release','experiment','experiment','release']):
        args=SimpleNamespace(output=OUT/f'{case}-{i}',mode='sub',requests=n,concurrency=25,watch_idle=False,app=None)
        p.run(args,variant,case)
        for f,h in snapshot.items():assert hashlib.sha256(Path(f).read_bytes()).hexdigest()==h,f
        meta=json.loads((args.output/f'{variant}-{case}'/'metadata.json').read_text())
        for f,h in meta['server_hashes'].items():assert hashlib.sha256(Path(f).read_bytes()).hexdigest()==h,f
print('All eight profiles completed; source hashes unchanged.',flush=True)
