# Isolated AWS benchmark machine — 2026-09-24

Approved design: user's approval to provision follows the proposed standalone
On-Demand c7a.xlarge, one worker and separate CPU affinity for server/client.
Use the Larp account/setup as reference; never change its instance or deployment.

## Work map

- PAGI-Server: /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/experiment-http-simplification; branch experiment/http-simplification; base 07a03b8 plus saved uncommitted completion/header experiments. Ticket: session performance investigation. Owned: benchmark provisioning notes/scripts/evidence only. Deployment: a new isolated EC2 instance. Push target: none; no commit/push or runtime change.
- LarpAdventuresWeb: /Users/jnapiorkowski/Desktop/LarpAdventuresWeb; main at ba4e6c04d98ebaedaa924b7fcc2bfcf672b0c45c. Read-only setup reference. Existing i-019ee8d811e8712aa and all Larp resources excluded. No ticket/change/deployment/push.
- PAGI-Tools: /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Tools; branch feature/universal-connection-tools at d7ae64e8d130e4856e5ab270d1b9c7ad72. Workspace only; no changes/deployment/push.

## Steps

1. Verify AWS identity, Larp inventory, pricing, official AMI and instance availability (done).
2. Create a separate ED25519 SSH key and security group with SSH restricted to 129.222.77.89/32; default account 268512542470, us-east-1, existing public subnet subnet-a71bdad1 / VPC vpc-fcfe6b98. Do not change shared route tables or existing groups.
3. Launch one c7a.xlarge (4 physical cores, 8 GiB), official Ubuntu AMI ami-0045d7fc2ad003464; 24 GiB encrypted gp3, IMDSv2, shutdown=stop, no IAM role, no autoscaling, no Spot. Tag Project=PAGI and Purpose=benchmark. Compute $0.20528/hour; storage/public IPv4 extra. Add eight-hour shutdown timer.
4. Install build tools, Perl 5.42.2, matching primary dependency versions, hey and Python; retain logs. Verify SSH host key against AWS console output. No repository/AWS/private credentials copied to host.
5. Transfer exact release/baseline/candidate source snapshots and benchmark apps via SSH, with SHA256 manifests. Run correctness smoke checks and unchanged-build repeated benchmarks before drawing comparisons. Capture host topology, versions, affinity, CPU/paging and output.
6. Save machine ID, access, start/stop/cleanup commands and evidence in Server worktree. Leave stopped when work completes to avoid idle compute charges; root disk persists. Do not claim macOS reproduction from Linux measurements.

## User steering

Install current PAGI-Server release from CPAN (verified 0.002013) and compare
it directly with the full current working tree first. Intermediate snapshots
are deferred. Use Git commit deployments after changes settle; today's
uncommitted tree is transferred with SHA256 manifest.
