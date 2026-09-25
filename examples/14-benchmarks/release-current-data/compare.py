import hashlib, importlib.util, json, os, shutil, subprocess
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
EXP = Path('/Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/experiment-http-simplification')
spec = importlib.util.spec_from_file_location('bench', HERE/'harness.py')
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)
bench.HERE = EXP/'examples/14-benchmarks'
perl, installed = shutil.which('perl'), shutil.which('pagi-server')
env = dict(os.environ, LIBEV_FLAGS='8', PERL_FUTURE_NO_XS='1')
for key in ('PAGI_FUTURE_XS','NYTPROF'):
    env.pop(key,None)
probe = r'''use PAGI::Server; use IO::Async::Loop::EV; use Future; use JSON::PP;
print encode_json({server_version=>$PAGI::Server::VERSION, adapter_version=>"$IO::Async::Loop::EV::VERSION", inc=>\%INC});'''
metadata = dict(identities={}, apps={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in bench.HERE.glob('*.pl')},
    settings=dict(workers=1, concurrency=25, ws_connections=20, seconds=15,
        order=['release','current','current','release']),
    env={key:env.get(key) for key in ('LIBEV_FLAGS','PERL_FUTURE_NO_XS','PERL5LIB','PERL5OPT')})
for label,source in (('release',None),('current',EXP)):
    loaded = json.loads(subprocess.check_output([perl]+(['-I'+str(source/'lib')] if source else [])+['-e',probe],env=env,text=True))
    assert loaded['adapter_version']=='0.05'
    assert loaded['server_version']=='0.002013',loaded
    assert 'Future/PP.pm' in loaded['inc'] and 'Future/XS.pm' not in loaded['inc']
    libdir = Path(loaded['inc']['PAGI/Server.pm']).parent
    files = [libdir/'Server.pm',*sorted((libdir/'Server').rglob('*.pm')),
        Path(loaded['inc']['IO/Async/Loop/EV.pm']), source/'bin/pagi-server' if source else Path(installed)]
    metadata['identities'][label] = dict(loaded=loaded,
        hashes={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in files})
(HERE/'metadata.json').write_text(json.dumps(metadata,indent=2)+'\n')
output = HERE/'comparison'; output.mkdir(exist_ok=False)
args = SimpleNamespace(repo=EXP, baseline_repo=None, output=output,
    workers=1, concurrency=25, ws_connections=20, seconds=15, run_id='release-current')
for case in ('get','post-observe','single','stream','stream-observe','sse','websocket'):
    for variant in ('release','current','current','release'):
        print(f'START {case} {variant}',flush=True)
        bench.run_one(args,variant,case,env,perl,installed)
        for path,digest in metadata['identities'][variant]['hashes'].items():
            assert hashlib.sha256(Path(path).read_bytes()).hexdigest()==digest,path
for path,digest in metadata['apps'].items():
    assert hashlib.sha256(Path(path).read_bytes()).hexdigest()==digest,path
print('All 28 samples complete; runtime sources and adapter unchanged.',flush=True)
