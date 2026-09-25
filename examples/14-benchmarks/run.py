#!/usr/bin/env python3
"""Controlled local release/main comparison. Python stdlib + hey; optional Perl WS client."""
import argparse
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import time

HERE = Path(__file__).resolve().parent
CASES = ['get', 'get-observe', 'headers', 'post', 'post-observe', 'single', 'single-observe',
         'stream', 'stream-observe', 'sse', 'websocket']

def case_config(case):
    base = case.removesuffix('-observe')
    app = 'stream' if base == 'single' else base
    query = []
    if base == 'single':
        query.append('single=1')
    if case.endswith('-observe'):
        query.append('observe=1')
    path = '/' + ('?' + '&'.join(query) if query else '')
    return base, app, path

def check_response(base, response):
    body = response.read()
    if response.status != 200:
        raise RuntimeError(f'HTTP {response.status}: {body[:200]!r}')
    expected = {'get': b'Hello from PAGI', 'headers': b'Hello from PAGI', 'post': b'1024\n',
                'single': b'x' * 65536, 'stream': b'x' * 65536}
    if base in expected and body != expected[base]:
        raise RuntimeError(f'Incorrect {base} response ({len(body)} bytes)')
    if base == 'headers':
        expected_headers = {
            'content-type': 'text/plain', 'cache-control': 'private, no-cache',
            'vary': 'accept-encoding', 'etag': '"benchmark-v1"',
            'content-language': 'en', 'x-content-type-options': 'nosniff',
            'referrer-policy': 'same-origin',
            'permissions-policy': 'camera=(), microphone=()',
            'link': '</assets/app.css>; rel=preload; as=style',
            'x-benchmark-case': 'ten-headers',
        }
        for name, value in expected_headers.items():
            if response.getheader(name) != value:
                raise RuntimeError(f'Incorrect benchmark header: {name}')
    if base == 'sse':
        if response.getheader('content-type', '').split(';')[0] != 'text/event-stream':
            raise RuntimeError('Incorrect SSE content type')
        blocks = [b for b in body.replace(b'\r\n', b'\n').split(b'\n\n') if b.strip()]
        expected_blocks = [{'id': str(i), 'event': 'tick', 'data': 'x' * 128} for i in range(1, 101)]
        actual = [dict((key, value.lstrip(' '))
                       for key, value in (line.split(':', 1) for line in block.decode().splitlines())) for block in blocks]
        if actual != expected_blocks:
            raise RuntimeError('Missing, unordered, or incorrect SSE events')

def preflight(port, base, path):
    conn = http.client.HTTPConnection('127.0.0.1', port, timeout=10)
    try:
        # Repeated requests also check reuse/reconnect after a completed response.
        for _ in range(3):
            headers = {'Accept': 'text/event-stream'} if base == 'sse' else {}
            conn.request('POST' if base == 'post' else 'GET', path,
                         body=b'x' * 1024 if base == 'post' else None, headers=headers)
            check_response(base, conn.getresponse())
        if base == 'post':
            conn.putrequest('POST', path)
            conn.putheader('Content-Length', '1024')
            conn.endheaders()
            conn.send(b'x' * 512)
            time.sleep(0.02)
            conn.send(b'x' * 512)
            check_response(base, conn.getresponse())
    finally:
        conn.close()

def parse_hey(output):
    if 'Error distribution:' in output:
        raise RuntimeError('Load generator reported errors; inspect raw output')
    codes = re.findall(r'^\s*\[(\d+)\]\s+(\d+) responses', output, re.M)
    if not codes or any(code != '200' for code, _ in codes):
        raise RuntimeError(f'Unexpected status distribution: {codes}')
    patterns = {'rps': r'Requests/sec:\s+([\d.]+)', 'average_s': r'Average:\s+([\d.]+)',
                'p50_s': r'50% in ([\d.]+)', 'p99_s': r'99% in ([\d.]+)'}
    result = {}
    for key, pattern in patterns.items():
        match = re.search(pattern, output)
        if match:
            result[key] = float(match.group(1))
        elif key in ('p50_s', 'p99_s'):
            result[key] = None  # hey can omit percentiles for small samples.
        else:
            raise RuntimeError(f'Load report missing {key}; inspect raw output')
    result['responses'] = sum(int(count) for _, count in codes)
    return result

def stop_server(proc, grace=12, term=5):
    """Bounded cleanup of this launch's process group, including orphaned workers."""
    for sig, timeout in ((signal.SIGINT, grace), (signal.SIGTERM, term), (signal.SIGKILL, 2)):
        try:
            os.killpg(proc.pid, sig)
        except ProcessLookupError:
            break
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            proc.poll()  # Reap the master if it has exited.
            try:
                os.killpg(proc.pid, 0)
            except ProcessLookupError:
                break
            except PermissionError:
                # macOS can report EPERM while a killed process group exits.
                # Keep polling within the deadline; do not assume it is gone.
                pass
            time.sleep(0.05)
        else:
            continue
        break
    else:
        raise RuntimeError(f'Benchmark process group {proc.pid} did not disappear after SIGKILL')
    proc.wait(timeout=2)

def run_one(args, variant, case, env, perl, installed):
    base, app, path = case_config(case)
    with socket.socket() as reservation:
        reservation.bind(('127.0.0.1', 0))
        port = reservation.getsockname()[1]
    stamp = f'{time.time_ns()}-{variant}-{case}-w{args.workers}'
    is_baseline = variant in ('release', 'baseline')
    source = args.baseline_repo if is_baseline else args.repo
    prefix = ([perl, installed] if source is None else
              [perl, '-I' + str(source / 'lib'), str(source / 'bin/pagi-server')])
    command = prefix + ['--loop', 'EV', '--workers', str(args.workers), '--env', 'production',
                        '--port', str(port), str(HERE / (app + '.pl'))]
    with (args.output / (stamp + '.server.log')).open('w') as log:
        proc = subprocess.Popen(command, cwd=args.repo, env=env, stdout=log, stderr=log, start_new_session=True)
        try:
            for _ in range(100):
                if proc.poll() is not None:
                    raise RuntimeError(f'Server exited: {stamp}')
                try:
                    with socket.create_connection(('127.0.0.1', port), timeout=0.1):
                        break
                except OSError:
                    time.sleep(0.1)
            else:
                raise RuntimeError('Server did not listen')
            if base == 'websocket':
                client = [perl, str(HERE / 'ws-client.pl'), f'ws://127.0.0.1:{port}/', str(args.seconds), str(args.ws_connections)]
                result = subprocess.run(client, env=env, capture_output=True, text=True, timeout=args.seconds + 40)
                (args.output / (stamp + '.client.txt')).write_text(result.stdout + result.stderr)
                result.check_returncode()
                metrics = json.loads(result.stdout)
            else:
                preflight(port, base, path)
                client = [shutil.which('hey'), '-c', str(args.concurrency)]
                if base == 'post':
                    client += ['-m', 'POST', '-d', 'x' * 1024]
                if base == 'sse':
                    client += ['-H', 'Accept: text/event-stream']
                url = f'http://127.0.0.1:{port}{path}'
                warm = subprocess.run(client + ['-z', '1s', url], env=env, capture_output=True, text=True, timeout=20)
                (args.output / (stamp + '.warmup.txt')).write_text(warm.stdout + warm.stderr)
                warm.check_returncode()
                parse_hey(warm.stdout)
                result = subprocess.run(client + ['-z', f'{args.seconds}s', url], env=env,
                                        capture_output=True, text=True, timeout=args.seconds + 30)
                (args.output / (stamp + '.client.txt')).write_text(result.stdout + result.stderr)
                result.check_returncode()
                metrics = parse_hey(result.stdout)
                if base == 'sse':
                    metrics['events_per_second'] = metrics['rps'] * 100
                if base in ('single', 'stream'):
                    metrics['mib_per_second'] = metrics['rps'] / 16
            row = dict(run_id=args.run_id, variant=variant, case=case, workers=args.workers, concurrency=args.concurrency,
                       seconds=args.seconds, stamp=stamp, command=command, **metrics)
            with (args.output / 'results.jsonl').open('a') as out:
                out.write(json.dumps(row) + '\n')
            print(json.dumps(row), flush=True)
        finally:
            stop_server(proc)
            time.sleep(0.3)

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--repo', type=Path, default=HERE.parents[1])
    parser.add_argument('--baseline-repo', type=Path, help='compare another checkout with --repo instead of the installed release')
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--seconds', type=int, default=10)
    parser.add_argument('--workers', type=int, default=1)
    parser.add_argument('--concurrency', type=int, default=50)
    parser.add_argument('--ws-connections', type=int, default=20)
    parser.add_argument('--cases', nargs='+', choices=CASES, default=CASES)
    parser.add_argument('--order', nargs='+', choices=['release', 'main', 'baseline', 'candidate'])
    args = parser.parse_args()
    if min(args.seconds, args.workers, args.concurrency, args.ws_connections) <= 0:
        parser.error('numeric settings must be positive')
    args.repo = args.repo.resolve()
    if args.baseline_repo:
        args.baseline_repo = args.baseline_repo.resolve()
    labels = ('baseline', 'candidate') if args.baseline_repo else ('release', 'main')
    args.order = args.order or [labels[0], labels[1], labels[1], labels[0]]
    if any(label not in labels for label in args.order):
        parser.error(f'use --order labels {labels} for this comparison')
    args.output.mkdir(parents=True, exist_ok=True)
    if (args.output / 'results.jsonl').exists() or list(args.output.glob('*-metadata.json')):
        parser.error('use a fresh output directory for each experiment')
    args.run_id = str(time.time_ns())
    perl, installed = shutil.which('perl'), shutil.which('pagi-server')
    if not perl or (not installed and not args.baseline_repo) or not shutil.which('hey'):
        parser.error('perl, installed pagi-server, and hey must be on PATH')
    env = dict(os.environ, LIBEV_FLAGS='8', PERL_FUTURE_NO_XS='1')
    # Preserve perlbrew/local::lib paths; record them so development overrides are visible.
    probe = 'use PAGI::Server; use Future; print "$^V $PAGI::Server::VERSION $INC{q(PAGI/Server.pm)} $INC{q(Future.pm)}\\n";'
    metadata = {'settings': vars(args) | {'repo': str(args.repo), 'output': str(args.output), 'baseline_repo': str(args.baseline_repo) if args.baseline_repo else None},
                'perl': perl, 'installed_runner': installed, 'env': {key: env.get(key) for key in ('LIBEV_FLAGS', 'PERL_FUTURE_NO_XS', 'PERL5LIB', 'PERL5OPT')},
                'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=args.repo, text=True).strip(),
                'status': subprocess.check_output(['git', 'status', '--short'], cwd=args.repo, text=True),
                'runner_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                'checkout_lib_sha256': {str(p.relative_to(args.repo)): hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((args.repo/'lib').rglob('*.pm'))},
                'app_sha256': {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in HERE.glob('*.pl')}}
    for variant, source in zip(labels, (args.baseline_repo, args.repo)):
        command = [perl] + (['-I' + str(source / 'lib')] if source else []) + ['-e', probe]
        metadata[variant] = subprocess.check_output(command, env=env, text=True).strip()
        if source:
            metadata[variant + '_source'] = {
                'repo': str(source),
                'head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=source, text=True).strip(),
                'status': subprocess.check_output(['git', 'status', '--short'], cwd=source, text=True),
                'lib_sha256': {str(p.relative_to(source)): hashlib.sha256(p.read_bytes()).hexdigest()
                               for p in sorted((source / 'lib').rglob('*.pm'))},
            }
    (args.output / f'{args.run_id}-metadata.json').write_text(json.dumps(metadata, indent=2) + '\n')
    for case in args.cases:
        for variant in args.order:
            run_one(args, variant, case, env, perl, installed)

if __name__ == '__main__':
    main()
