import json, os, subprocess
from pathlib import Path

root = Path(__file__).resolve().parent
repo = Path('/Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/experiment-http-simplification')
env = dict(os.environ, IO_ASYNC_LOOP='EV', LIBEV_FLAGS='8', PERL_FUTURE_NO_XS='1')
env['PERL5LIB'] = str(root/'candidate/lib') + (os.pathsep+env['PERL5LIB'] if env.get('PERL5LIB') else '')
probe = subprocess.check_output(['perl', '-MIO::Async::Loop', '-e', 'my $l=IO::Async::Loop->new; print ref($l), "\n", $INC{"IO/Async/Loop/EV.pm"}, "\n"'], env=env, text=True)
assert probe.splitlines() == ['IO::Async::Loop::EV', str(root/'candidate/lib/IO/Async/Loop/EV.pm')], probe
tests = [
    't/37-connection-state.t',
    't/69-connection-state-protocol-scopes.t',
    't/84-terminal-callback-deferral.t',
    't/85-terminal-callback-graceful-shutdown.t',
    't/http-terminal-send-reentrant-cancel.t',
    't/sse-close.t',
    't/sse-decline.t',
    't/ws-close-parity.t',
    't/ws-close-no-duplicate-frame.t',
    't/http2/30-connection-state.t',
    't/http2/25-sse-decline.t',
    't/http2/ws-close-no-duplicate-frame-h2.t',
]
command = ['prove', '-l', '-j4', *tests]
(root/'server-checks-command.json').write_text(json.dumps(dict(command=command, cwd=str(repo), loaded=probe,
    env={k:env[k] for k in ['PERL5LIB','IO_ASYNC_LOOP','LIBEV_FLAGS','PERL_FUTURE_NO_XS']}), indent=2)+'\n')
with (root/'server-checks.log').open('w') as log:
    proc = subprocess.run(command, cwd=repo, env=env, stdout=log, stderr=subprocess.STDOUT, timeout=240)
print((root/'server-checks.log').read_text())
raise SystemExit(proc.returncode)
