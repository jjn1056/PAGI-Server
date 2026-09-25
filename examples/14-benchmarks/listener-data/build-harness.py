from pathlib import Path
s=Path('examples/14-benchmarks/last-two-data/linux-harness.py').read_text()
s=s.replace("    base = case.removesuffix('-observe')", "    if case in ('get-500', 'churn', 'storm'):\n        return 'get', 'get', '/'\n    base = case.removesuffix('-observe')")
s=s.replace("'p50_s': r'50% in ([\\d.]+)', 'p99_s': r'99% in ([\\d.]+)'", "'p50_s': r'50% in ([\\d.]+)', 'p95_s': r'95% in ([\\d.]+)',\n                'p99_s': r'99% in ([\\d.]+)', 'max_s': r'Slowest:\\s+([\\d.]+)'")
s=s.replace("elif key in ('p50_s', 'p99_s'):", "elif key in ('p50_s', 'p95_s', 'p99_s'):")
a=s.index('                warm = subprocess.run(');z=s.index("                if base == 'sse':",a)
s=s[:a]+'''                if case == 'storm':
                    # Each of 500 workers sends one fresh-connection request.
                    # Keep the separate established-connection load active for
                    # the entire storm; don't dilute first-response percentiles.
                    background_command = ['taskset', '-c', '2,3', shutil.which('hey'),
                                          '-c', '100', '-z', f'{args.seconds}s', url]
                    background_path = args.output / (stamp + '.background.txt')
                    with background_path.open('w') as bglog:
                        background = subprocess.Popen(background_command, env=env,
                                                      stdout=bglog, stderr=bglog)
                        try:
                            time.sleep(1 if args.smoke else 2)
                            if background.poll() is not None:
                                raise RuntimeError('Background load stopped before the storm')
                            burst_command = client + ['-disable-keepalive', '-n',
                                                       str(args.concurrency), url]
                            result = subprocess.run(burst_command, env=env,
                                                    capture_output=True, text=True,
                                                    timeout=max(args.seconds - 3, 3))
                            if background.poll() is not None:
                                raise RuntimeError('Storm outlasted background load')
                            result.check_returncode()
                            background.wait(timeout=args.seconds + 5)
                            if background.returncode:
                                raise RuntimeError('Background load failed')
                        finally:
                            if background.poll() is None:
                                background.terminate()
                                background.wait(timeout=5)
                    (args.output / (stamp + '.client.txt')).write_text(result.stdout + result.stderr)
                    metrics = parse_hey(result.stdout)
                    if metrics['responses'] != args.concurrency:
                        raise RuntimeError('Missing storm responses')
                    metrics['background'] = parse_hey(background_path.read_text())
                    metrics['background_command'] = background_command
                    metrics['client_command'] = burst_command
                else:
                    if case == 'churn':
                        client += ['-disable-keepalive']
                    warm = subprocess.run(client + ['-z', '1s', url], env=env,
                                          capture_output=True, text=True, timeout=20)
                    (args.output / (stamp + '.warmup.txt')).write_text(warm.stdout + warm.stderr)
                    warm.check_returncode()
                    parse_hey(warm.stdout)
                    measured_command = client + ['-z', f'{args.seconds}s', url]
                    result = subprocess.run(measured_command, env=env,
                                            capture_output=True, text=True,
                                            timeout=args.seconds + 30)
                    (args.output / (stamp + '.client.txt')).write_text(result.stdout + result.stderr)
                    result.check_returncode()
                    metrics = parse_hey(result.stdout)
                    metrics['client_command'] = measured_command
''' +s[z:]
Path('/tmp/pagi-listener-20260925/listener-harness.py').write_text(s)
