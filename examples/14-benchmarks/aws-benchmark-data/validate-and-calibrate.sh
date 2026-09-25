#!/bin/bash
set -eo pipefail
for n in $(seq 1 90); do
    test -f /home/ubuntu/pagi-benchmark/bootstrap-complete && break
    sleep 10
done
test -f /home/ubuntu/pagi-benchmark/bootstrap-complete
source /home/ubuntu/perl5/perlbrew/etc/bashrc
perlbrew use perl-5.42.2
cd /home/ubuntu/pagi-benchmark
python3 -B - <<'VERIFY'
import hashlib,json
from pathlib import Path
for n,h in json.load(open('manifest.json')).items():
    assert hashlib.sha256(Path(n).read_bytes()).hexdigest()==h,n
print('All uploaded files match manifest.')
VERIFY
cd work
PERL_FUTURE_NO_XS=1 prove -l t/http-header-boundary.t t/15-crlf-injection.t t/52-mandatory-validation.t t/53-trailers-framing.t t/71-http-refusal-on-protocol-scopes.t > ../current-tests.log 2>&1
PERL_FUTURE_NO_XS=1 perl examples/14-benchmarks/check-apps.pl > ../apps-check.log 2>&1
python3 -B examples/14-benchmarks/check-harness.py > ../harness-check.log 2>&1
cd ..
taskset -c 1 python3 -B compare-linux.py --mode smoke > smoke.log 2>&1
taskset -c 1 python3 -B compare-linux.py --mode stability > stability.log 2>&1
touch calibration-complete
