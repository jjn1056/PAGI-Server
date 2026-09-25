# Quiet rerun readiness check — 2026-09-24

User authorized a rerun if the machine was quiet. No benchmark was started.
Runtime hashes still match the saved release/baseline/candidate identities.

At 14:28 local time, top reported 46,784 swapouts in a three-second interval.
Two subsequent forty-second observation periods had no further swapouts but
continued swapins. The final period (14:29:59–14:30:39) showed 1,843–8,724
swapins per ten-second interval, CPU 90.93–94.50% idle, compressed memory
increasing from 1,919 MB to 2,003 MB and unused memory decreasing from 581 MB
to 436 MB. See quiet-attempt-load.txt. These readings do not prove that the
benchmark itself would page or that earlier differences were caused by paging;
they do mean that the desired settled-machine comparison was not available.

No processes were stopped, runtime files changed, or samples discarded. Keep
the current experiment uncommitted and its timing conclusion inconclusive.
