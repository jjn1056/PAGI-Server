#!/bin/bash
set -eo pipefail
source /home/ubuntu/perl5/perlbrew/etc/bashrc
perlbrew use perl-5.42.2
cd /home/ubuntu/pagi-benchmark/request-timing-20260925
unset PERL5LIB PERL5OPT NYTPROF PAGI_FUTURE_XS
(cd candidate && PERL_FUTURE_NO_XS=1 prove -l t/44-access-log-format.t) > focused-tests.log 2>&1
taskset -c 1 python3 compare.py --smoke > smoke.log 2>&1
taskset -c 1 python3 compare.py > timed.log 2>&1
python3 summarize.py . > summary.txt
