"""Focused checks for summary isolation and bounded cleanup of owned processes."""
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('bench_runner', HERE / 'run.py')
runner = importlib.util.module_from_spec(spec)
spec.loader.exec_module(runner)

class HarnessChecks(unittest.TestCase):
    def test_summary_reports_checkout_labels_and_rates(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'results.jsonl'
            row = dict(workers=1, case='get', variant='baseline', seconds=10,
                       concurrency=50, rps=100, p50_s=.01, p99_s=.02, run_id='one')
            path.write_text(json.dumps(row) + '\n' + json.dumps(row | {'variant':'candidate', 'rps':110}) + '\n')
            result = subprocess.run([sys.executable, str(HERE/'summarize.py'), str(path)], capture_output=True, text=True)
            self.assertEqual(result.returncode, 0)
            self.assertIn('Baseline rate', result.stdout)
            self.assertIn('Candidate rate', result.stdout)
            self.assertIn('+10.0%', result.stdout)

    def test_summary_rejects_mixed_load_settings(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'results.jsonl'
            row = dict(workers=1, case='get', variant='release', seconds=10,
                       concurrency=50, rps=100, p50_s=.01, p99_s=.02, run_id='one')
            path.write_text(json.dumps(row) + '\n' + json.dumps(row | {'variant': 'main', 'concurrency': 500}) + '\n')
            result = subprocess.run([sys.executable, str(HERE/'summarize.py'), str(path)], capture_output=True, text=True)
            self.assertNotEqual(result.returncode, 0, 'must not average different loads')

    def test_cleanup_escalates_for_unresponsive_owned_process(self):
        with tempfile.TemporaryDirectory() as directory:
            ready = Path(directory)/'ready'
            code = ('import signal,time,pathlib; '
                    'signal.signal(signal.SIGINT,signal.SIG_IGN); '
                    'signal.signal(signal.SIGTERM,signal.SIG_IGN); '
                    f'pathlib.Path({str(ready)!r}).touch(); time.sleep(60)')
            proc = subprocess.Popen([sys.executable, '-c', code], start_new_session=True)
            try:
                deadline = time.monotonic() + 5
                while not ready.exists() and time.monotonic() < deadline:
                    time.sleep(.01)
                self.assertTrue(ready.exists())
                runner.stop_server(proc, grace=.1, term=.1)
                self.assertIsNotNone(proc.poll())
            finally:
                if proc.poll() is None:
                    proc.kill()
                proc.wait()

if __name__ == '__main__':
    unittest.main()
