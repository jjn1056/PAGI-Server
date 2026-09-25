# HTTP send-state ownership experiment

Repository: /Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/experiment-http-simplification
Ticket: session performance investigation, bounded follow-up approved by user.
Branch: experiment/http-simplification
Base: 6dbcf47 (runtime checkpoint 874b120)
Owned changes: one HTTP/1 send factory state-storage simplification, regression checks, benchmark evidence.
Deployment boundary: existing isolated AWS benchmark instance i-077635273be935c19; stop afterwards.
Push target: none requested. No other repository, root main, or branch is in scope.

Use the connection's existing scalar slot as the ordinary HTTP state owner; keep the refusal delegate's HTTP state private because it translates into different outer protocol labels. Preserve the existing on_state callback only for those translations, with no new option or flag. Each keep-alive scope needs its own scalar slot; an old send must not alias a new scope's state. Scalar references must not retain the connection.

Verify existing behavior and added lifetime/scope isolation checks before and after. Run the full suite if candidate survives targeted checks. Benchmark release, saved checkpoint, candidate in reversed orders on existing AWS host; retain only if simpler and no material regression. No public API/spec changes.

AWS SSH source rule updated from previous 129.222.77.89/32 to current 98.97.23.128/32; TCP 22 only, benchmark security group sg-0c9f8ec1dc4567189. No widened ingress.
Decision after complete HTTP measurements: reject candidate. Small mixed effects do not justify scalar-detachment invariant; retain evidence and restore original runtime/test tree once validation ends.
