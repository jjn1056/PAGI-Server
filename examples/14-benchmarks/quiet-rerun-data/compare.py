import hashlib, importlib.util, json, os, shutil, subprocess
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
EXP = Path('/Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/experiment-http-simplification')
BEFORE = Path('/tmp/pagi-shared-receive-20260923/before')
OLD_ADAPTER = Path('/tmp/pagi-ev-timer-20260923/pristine/lib')
spec = importlib.util.spec_from_file_location('bench', HERE/'harness.py')
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)
bench.HERE = EXP/'examples/14-benchmarks'
perl = shutil.which('perl')
env = dict(os.environ, LIBEV_FLAGS='8', PERL_FUTURE_NO_XS='1')
for key in ('PAGI_FUTURE_XS', 'NYTPROF'):
    env.pop(key, None)
probe = r'''use PAGI::Server; use IO::Async::Loop::EV; use Future; use JSON::PP;
print encode_json({adapter_version=>"$IO::Async::Loop::EV::VERSION", inc=>\%INC, server_version=>$PAGI::Server::VERSION});'''

def identity(source, server_env):
    loaded = json.loads(subprocess.check_output([perl, '-I'+str(source/'lib'), '-e', probe], env=server_env, text=True))
    assert 'Future/PP.pm' in loaded['inc'] and 'Future/XS.pm' not in loaded['inc']
    adapter_path = Path(loaded['inc']['IO/Async/Loop/EV.pm'])
    sources = [*sorted((source/'lib').rglob('*.pm')), source/'bin/pagi-server', adapter_path]
    return dict(loaded=loaded, source=str(source),
        env={key:server_env.get(key) for key in ('LIBEV_FLAGS','PERL_FUTURE_NO_XS','PERL5LIB','PERL5OPT')},
        hashes={str(p):hashlib.sha256(p.read_bytes()).hexdigest() for p in sources})

client_identity = identity(EXP, env)
assert client_identity['loaded']['adapter_version'] == '0.05'
old_env = dict(env, PERL5LIB=str(OLD_ADAPTER)+(os.pathsep+env['PERL5LIB'] if env.get('PERL5LIB') else ''))
metadata = dict(client=client_identity, apps={str(p):hashlib.sha256(p.read_bytes()).hexdigest()
    for p in bench.HERE.glob('*.pl')}, phases={})

for phase, cases, seconds in (
    ('receive', ('single','stream','stream-observe','sse','websocket'), 20),
    ('adapter', ('get-observe','stream-observe','sse','websocket'), 10),
):
    output = HERE/phase
    output.mkdir(exist_ok=False)
    args = SimpleNamespace(repo=EXP, baseline_repo=BEFORE if phase=='receive' else EXP,
        output=output, workers=1, concurrency=25, ws_connections=20, seconds=seconds,
        run_id=phase)
    identities = {label:identity(args.baseline_repo if label=='baseline' else EXP,
        old_env if phase=='adapter' and label=='baseline' else env)
        for label in ('baseline','candidate')}
    assert identities['candidate']['loaded']['adapter_version'] == '0.05'
    assert identities['baseline']['loaded']['adapter_version'] == ('0.04' if phase=='adapter' else '0.05')
    metadata['phases'][phase] = dict(identities=identities, cases=cases, seconds=seconds,
        baseline='before receive refactor' if phase=='receive' else 'adapter 0.04',
        candidate='after receive refactor' if phase=='receive' else 'adapter 0.05')
    (HERE/'metadata.json').write_text(json.dumps(metadata,indent=2)+'\n')
    for case in cases:
        for variant in ('baseline','candidate','candidate','baseline'):
            print(f'START {phase} {case} {variant}',flush=True)
            bench.run_one(args,variant,case,env,perl,shutil.which('pagi-server'),
                server_env=old_env if phase=='adapter' and variant=='baseline' else env)
            for path,digest in identities[variant]['hashes'].items():
                assert hashlib.sha256(Path(path).read_bytes()).hexdigest()==digest,path

for path,digest in metadata['apps'].items():
    assert hashlib.sha256(Path(path).read_bytes()).hexdigest()==digest,path
assert identity(EXP,env)['hashes'] == client_identity['hashes']
print('All samples complete; source and installed-adapter hashes unchanged.',flush=True)
