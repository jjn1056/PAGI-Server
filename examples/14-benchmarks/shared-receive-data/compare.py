import hashlib, importlib.util, json, shutil, subprocess, sys
from pathlib import Path
from types import SimpleNamespace

HERE = Path(__file__).resolve().parent
EXP = Path('/Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/experiment-http-simplification')
spec = importlib.util.spec_from_file_location('profile_harness', EXP/'examples/14-benchmarks/profiling-data/profile.py')
harness = importlib.util.module_from_spec(spec)
spec.loader.exec_module(harness)
# Use the exact existing experiment before this change as the baseline. The
# harness's "main" label here means this snapshot, not the repository's main.
harness.ROOT = HERE/'before'
probe = 'use IO::Async::Loop::EV; use JSON::PP; print encode_json({version=>"$IO::Async::Loop::EV::VERSION",path=>$INC{"IO/Async/Loop/EV.pm"}})'
adapter = json.loads(subprocess.check_output([shutil.which('perl'), '-e', probe], text=True))
assert adapter['version'] == '0.05', adapter
adapter['sha256'] = hashlib.sha256(Path(adapter['path']).read_bytes()).hexdigest()
(HERE/'adapter.json').write_text(json.dumps(adapter, indent=2)+'\n')

confirm = '--confirm-stream' in sys.argv
cases = ('stream',) if confirm else ('get', 'post-observe', 'stream')
order = ('experiment', 'main', 'main', 'experiment') if confirm else ('main', 'experiment', 'experiment', 'main')
for case in cases:
    for index, variant in enumerate(order):
        label = 'before' if variant == 'main' else 'after'
        print(f'START {case} {index} {label}', flush=True)
        requests = 5000 if case == 'stream' else 30000
        _, app, _ = harness.bench.case_config(case)
        options = SimpleNamespace(output=HERE/('confirm' if confirm else 'runs')/f'{case}-{index}-{label}',
            mode='off', requests=requests, concurrency=25, watch_idle=False,
            app=EXP/'examples/14-benchmarks'/f'{app}.pl')
        harness.run(options, variant, case)
        assert hashlib.sha256(Path(adapter['path']).read_bytes()).hexdigest() == adapter['sha256']
