#!/bin/bash
set -e
source /home/ubuntu/perl5/perlbrew/etc/bashrc
perlbrew use perl-5.42.2
cd /home/ubuntu/pagi-benchmark
unset PERL5LIB PERL5OPT NYTPROF PAGI_FUTURE_XS
out=/home/ubuntu/pagi-benchmark/focused-profile-874b120
cp profile-linux.py run-profile.sh "$out/"
vmstat -w 1 > "$out/vmstat.txt" &
monitor_pid=$!
trap 'kill "$monitor_pid" 2>/dev/null || true' EXIT
for round in 1 2; do
    if [ "$round" = 1 ]; then order='release current'; else order='current release'; fi
    taskset -c 1 python3 profile-linux.py --output "$out/sub-round-$round" --mode sub --requests 5000 --concurrency 25 --cases get post-observe --variants $order
done
taskset -c 1 python3 profile-linux.py --output "$out/lines" --mode line --requests 2000 --concurrency 25 --cases get post-observe --variants release current
