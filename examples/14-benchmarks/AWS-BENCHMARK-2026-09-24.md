# AWS benchmark machine — 2026-09-24

Status: first comparison complete; instance confirmed **stopped** after results
were downloaded. Disk and setup are retained for restart.

A standalone Ubuntu 24.04 EC2 instance for CPAN/current comparisons. It uses
the same AWS account and public subnet as the Larp beta machine, but its own
instance, key and security group. Larp infrastructure was not changed.

- Instance: `i-077635273be935c19`, region `us-east-1`, local AWS profile `default`.
- Type: `c7a.xlarge`, four physical AMD EPYC cores (one thread/core), 8 GiB RAM.
- Purchase: On-Demand, no Auto Scaling group, no CPU credit mechanism.
- Disk: 24 GiB encrypted gp3, deleted on instance termination.
- Compute price checked at creation: $0.20528/hour. Storage and public IPv4 extra.
- Public IP at creation: `100.48.99.234`; query it again after a stop/start.
- SSH user `ubuntu`; local key `~/.ssh/pagi-benchmark-20260924`.
- SSH-only group `sg-0c9f8ec1dc4567189`, restricted to `129.222.77.89/32`.
- Host ED25519 fingerprint verified against AWS console output:
  `SHA256:KueqZSpGcdty8vvj/FyckqFAJboaRiXrcEBBb/uJtbs`.
- Eight-hour `pagi-benchmark-autostop.timer` stops the instance after boot.

## Access and lifecycle

```sh
aws --profile default --region us-east-1 ec2 start-instances --instance-ids i-077635273be935c19
aws --profile default --region us-east-1 ec2 describe-instances --instance-ids i-077635273be935c19 --query 'Reservations[0].Instances[0].{State:State.Name,IP:PublicIpAddress}'
ssh -i ~/.ssh/pagi-benchmark-20260924 -o HostKeyAlias=pagi-benchmark -o UserKnownHostsFile=~/.ssh/pagi-benchmark.known_hosts ubuntu@100.48.99.234
aws --profile default --region us-east-1 ec2 stop-instances --instance-ids i-077635273be935c19
```

Replace the SSH IP with the address returned after starting. If the client's
public IP changes, update only this benchmark group's SSH rule. The private
key is local only; it is not in this repository or on the instance. No AWS or
GitHub credentials are installed on the server. Stopping avoids idle compute
charges but retains billable disk storage. Permanent cleanup means terminating
this instance, then deleting its dedicated group and imported key pair after
saving any needed results. Never target the Larp instance.

## Sources and reproducibility

The requested baseline is CPAN `PAGI-Server-0.002013`, downloaded and installed
from its release tarball. Current is the full uncommitted working tree from
`experiment/http-simplification`, base `07a03b8`, including the saved performance
changes and completion/header cleanup. No source patch is applied remotely.

Current files were copied over SSH as a tar archive: lib, bin, tests, dependency
files and benchmark scripts. `manifest.json` records SHA256 for each file;
the driver verifies these and loaded dependency hashes after every run. No
`.git` directory is transferred. Once the changes settle, prefer a checkout
of an exact committed revision and record that revision in every result.

Both variants use the same Perl 5.42.2 and installed dependencies, with
`PERL_FUTURE_NO_XS=1`, `LIBEV_FLAGS=4` (Linux epoll; the Mac runs used 8/kqueue) and Loop::EV 0.05. Core versions are
recorded in each run's metadata. The fresh instance has no swap configured;
available memory and process usage are checked before benchmarking.

The first CPAN install failed one startup-banner test because the separate
PAGI spec distribution was absent. The spec
was installed with its tests skipped to break its server test-dependency cycle,
then the unmodified Server release was reinstalled with tests enabled. This is distinct from skipping dependency
tests during toolchain setup. Original failure output is retained.

## Running the checks

Remote files live under `/home/ubuntu/pagi-benchmark`. In an SSH shell:

```sh
source ~/perl5/perlbrew/etc/bashrc
perlbrew use perl-5.42.2
cd ~/pagi-benchmark
taskset -c 1 python3 -B compare-linux.py --mode smoke
taskset -c 1 python3 -B compare-linux.py --mode stability
taskset -c 1 python3 -B compare-linux.py --mode compare
```

The archived Linux harness is the existing benchmark runner with CPU affinity
and GNU time resource collection added. Server uses CPU 0; clients CPUs 2–3;
driver and monitoring CPU 1. `GOMAXPROCS=2` bounds the hey worker runtime.
One server worker, 25 HTTP clients or 20 WebSocket connections. Smoke uses two
seconds; stability and comparison use 15 measured seconds plus warmup.

Stability repeats the CPAN POST observer baseline four times. Comparison runs
GET, ten response headers, POST observer, streaming observer, SSE and WebSocket,
in release/current/current/release order for each case. These are diagnostic
measurements, not a replacement for a many-worker capacity test. The load
generator can still limit throughput, especially the Perl WebSocket client;
inspect pidstat alongside request/message rates and latency.

Raw output includes resource usage, vmstat, pidstat, command lines, exact source
and dependency identities, and response preflight checks. Reject broken runs,
keep noisy samples visible, and do not combine these Linux numbers with Mac
measurements. An unchanged build must be stable enough before interpreting a
small percentage difference.

## First CPAN/current comparison

The CPAN installation passed 143 files / 620 tests after the documented spec
setup workaround. The current tree passed the selected five files / 69 tests,
six example checks, four harness tests and all twelve protocol smoke runs.
The full current test suite was not rerun here.

Four unchanged CPAN POST runs were 7,545.5, 7,573.3, 7,447.4 and 7,635.0
requests/sec: 2.49% total spread relative to their mean.

The main comparison completed all 24 runs with matching source/dependency
hashes: 1,383,235 successful measured HTTP responses and 752,302 validated
WebSocket echoes. Means of two samples per variant:

| Workload | CPAN release | Current working tree | Current vs release |
|---|---:|---:|---:|
| get | 8,706.1 | 7,416.5 | -14.81% |
| headers | 7,499.7 | 6,876.0 | -8.32% |
| post-observe | 7,541.8 | 6,626.2 | -12.14% |
| stream-observe | 508.9 | 496.3 | -2.48% |
| sse | 214.5 | 214.1 | -0.21% |
| websocket | 12,631.8 | 12,443.3 | -1.49% |

Units are HTTP responses/sec, except WebSocket messages/sec. Each SSE response
contains 100 events. Labels `main` in raw output mean the current experimental
working tree, not Server's Git main branch. Both code versions still print
0.002013; source paths and hashes, not version text, distinguish them.

All 399 post-initial vmstat samples reported zero swap-in/out, zero CPU steal
and zero I/O wait at the tool's one-second/integer precision. Server processes
had no major page faults. HTTP hey CPU peaked at 40% of one core; server CPU
time was approximately 16.1 seconds for each 15-second measurement plus warmup,
consistent with a busy single server core. GNU time includes startup and
preflight, so these figures are not per-request CPU attribution.

The pidstat name filter captured hey and the Perl WebSocket client, but the
server renames its process and was not matched by that filter. Server resource
figures come from GNU time instead. The WebSocket client averaged about 93%
CPU (maximum 99%) in its sampled intervals: message-rate parity is potentially
client-limited, not proof of equal maximum server capacity.

The GET, ten-header and POST gaps are larger than observed repeat variation and
point consistently toward a slower current version in this setup. SSE is close;
streaming and WebSocket differences are too small for strong conclusions from
two samples, especially with the WebSocket client limitation. No samples were
discarded. This establishes a useful Linux baseline; it does not establish
which individual change caused the HTTP gap, invalidate correctness fixes,
or explain all earlier macOS observations. No new optimization was attempted.

Archive: `aws-benchmark-data/results/`, with summaries, raw clients, server
resource usage, host/process sampling, validation logs and metadata. Source
snapshot remains on the stopped instance and locally at
`/tmp/pagi-aws-benchmark-20260924/payload`; this repository archives its manifest.
