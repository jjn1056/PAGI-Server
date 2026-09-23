# Shared HTTP receive implementation experiment

User approved extending the existing performance modifications, on one development stream.

| Repository | Ticket | Branch / base | Owned changes | Deployment / push |
| --- | --- | --- | --- | --- |
| /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/experiment-http-simplification | Session performance investigation | experiment/http-simplification / 659ce38 | Shared HTTP receive routine, ownership/cancellation checks, paired benchmark evidence | Local only; no deployment or push |
| /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server | Same | main / 4625b00 with existing release preparation | None | None |
| /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Tools | Same | Not part of implementation | None | None |

The before snapshot in /tmp/pagi-shared-receive-20260923/before is a read-only benchmark reference copied from the current experiment's lib and bin, not another branch or development stream. All implementation stays in the existing experiment checkout. Installed dependencies remain unchanged.

Bounded design: move the per-scope async body closure to one named routine; retain the receive wrapper, cap/clean-end callbacks, Future tracking and protocol behavior. Shift the connection out of the async routine's argument list and weaken the lexical before awaiting, to avoid retaining the connection. Read framing values from the existing request record.

Verification: establish current behavior; test suspended-receive ownership, cancellation and repeated reads; run body, disconnect, clean-end, unread-body and reentrant cancellation checks, then full suite. Compare exact pre-change snapshot with updated branch sequentially on installed EV 0.05, GET, small POST and stream. Record all samples. No additional optimization if the result is flat or negative.
