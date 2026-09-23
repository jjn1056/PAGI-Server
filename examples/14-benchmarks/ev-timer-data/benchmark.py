import argparse, hashlib, importlib.util, json, os, shutil, subprocess
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
EXP = Path('/Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/experiment-http-simplification')
spec = importlib.util.spec_from_file_location('profile_harness', EXP/'examples/14-benchmarks/profiling-data/profile.py')
harness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(harness)

p = argparse.ArgumentParser()
p.add_argument('--completion', action='store_true')
args = p.parse_args()
original_lib = os.environ.get('PERL5LIB', '')
cases = ['get'] if args.completion else ['get', 'post-observe', 'stream']
order = ['pristine', 'candidate'] if args.completion else ['pristine', 'candidate', 'candidate', 'pristine']
for case in cases:
    for index, adapter in enumerate(order):
        lib = HERE/adapter/'lib'
        os.environ['PERL5LIB'] = str(lib) + (os.pathsep+original_lib if original_lib else '')
        loaded = subprocess.check_output([shutil.which('perl'), '-MIO::Async::Loop::EV', '-e', 'print $INC{"IO/Async/Loop/EV.pm"}'], text=True)
        assert Path(loaded).resolve() == lib/'IO/Async/Loop/EV.pm', loaded
        out = HERE/('completion' if args.completion else 'bench')/f'{case}-{index}-{adapter}'
        out.mkdir(parents=True)
        (out/'adapter.json').write_text(json.dumps(dict(adapter=adapter, path=loaded,
            sha256=hashlib.sha256(Path(loaded).read_bytes()).hexdigest()), indent=2)+'\n')
        print(f'START {case} {index} {adapter}', flush=True)
        options = SimpleNamespace(output=out, mode='off', requests=10000, concurrency=25,
            watch_idle=False, app=(EXP/'examples/14-benchmarks/profiling-data/completion-get.pl') if args.completion else None)
        harness.run(options, 'main', case)
        log=(out/f'main-{case}'/'server.log').read_text()
        if args.completion:
            for line in log.splitlines():
                if line.startswith('COMPLETION ') or line.startswith('PROGRESS requests=10000 '):
                    print(line, flush=True)
        print((out/f'main-{case}'/'process-time.txt').read_text(), flush=True)
