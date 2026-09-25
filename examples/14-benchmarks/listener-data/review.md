# Independent listener review

Read-only review of the isolated PR13 adaptation found one new medium issue:
a callback that paused/closed/removed the listener did not stop the batch.
Real-socket reproduction: pause accepted 3 clients instead of 1; close accepted 1
then threw EBADF; removal tried invoking the callback again on a removed notifier.
The candidate now checks the current handle, read readiness and loop membership
before accepting again. The reviewer rechecked the guard and regression tests
and found no further required change.

Accepted sockets remain nonblocking and stream-owned. The single-worker SSL
extension retains stock Listener; multiworker SSL is upgraded after ordinary
accept. Error delivery distinguishes EAGAIN from EMFILE. A 64-connection bound is
not a bound on elapsed time; fairness remains a measured question.

Pre-existing, not changed here: Server's accept-error callback signature takes
the socket as the error, and Listener 0.805 rejects configuring on_accept_error
(the existing eval swallows that). Inherited single-worker TLS behavior is
also unchanged. These were kept out of the listener performance experiment.

Focused final local suite: 11 files / 65 tests. Linux listener checks: 2 files / 8 tests.
Parent owns benchmark interpretation and any full-suite gate.
