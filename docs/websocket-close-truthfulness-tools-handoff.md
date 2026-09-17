# Handoff to the PAGI-Tools session — WebSocket close truthfulness (server side done)

Written 2026-09-16 by the PAGI-Server session. This file lives on branch
`feature/websocket-close-truthfulness` so you can read it directly. The server and
PAGI-spec side of the WebSocket-close-truthfulness work is implemented and reviewed
(final whole-branch review: MERGEABLE). Your side — the test doubles and the helper —
is next, and the end-to-end acceptance gate is joint and still open.

## Where the server code is
- Branch `feature/websocket-close-truthfulness` (this worktree:
  `/Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/feature-websocket-close-truthfulness`).
  UNMERGED as of writing — John holds the merge decision.
- To test PAGI-Tools against it before it merges, point at this checkout's lib:
  `PERL5LIB=/Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/feature-websocket-close-truthfulness/lib`
  (after it merges to PAGI-Server main, the normal main checkout is enough).
- The contract prose you code to: `PAGI/lib/PAGI/Spec/Www.pod` (the "Connection
  State" / closing-handshake sections, clarified in PAGI main commit a0a2014) and
  the RFC-cited rulings in this branch's `lib/PAGI/Server/Compliance.pod`.

## The observable contract your doubles must reproduce (portable, not server-internal)

APP-INITIATED close (the app/helper sends `websocket.close` on an accepted socket):
- The scope does NOT end clean at the send. It is PENDING until one of:
  - the peer sends its Close AND the transport/stream closes -> CLEAN (`on_complete`,
    `response_complete` true, `close_code` = the peer's code).
  - a finite close deadline elapses with no peer Close -> ABNORMAL,
    `disconnect_reason` = `close_timeout`, `close_code` 1006, `close_reason` undef.
  - the transport/stream is lost with no peer Close -> ABNORMAL, the transport-loss
    reason token, `close_code` 1006.
  - the peer ANSWERED (handshake completed) but the transport/stream does not FINISH
    closing within the bound -> ABNORMAL, `disconnect_reason` = `close_incomplete`,
    `close_code` 1006. Distinct from `close_timeout` (peer never answered).
- If a valid peer Close WAS observed, its code/reason are PRESERVED even when the
  outcome is abnormal for another reason.
- `on_complete` does NOT fire on the application's own close until the handshake
  completes. A cooperative peer completes it promptly; a silent peer yields
  `close_timeout`.

PEER-INITIATED close (peer sends Close; the app never did): the server now OWNS the
transport close and the scope is clean only at transport/stream closure, the same as
app-initiated — NOT at the reciprocal-Close point. (This changed: the earlier
early-clean was retired. The server drives the closure, so a parked app is still
notified.) The finish is bounded the same way: a peer that answered but never lets
the transport finish yields `close_incomplete`, with the peer's own close_code/reason
preserved (the outcome is carried in `disconnect_reason`, not the code).

`close_code`/`close_reason` are PEER-only: 1006 when the scope ended with no peer
Close; the deadline case and the transport-loss case both carry 1006 and are
distinguished ONLY by `disconnect_reason` (1006 alone cannot tell them apart).

## What you implement (from the approved design spec)

Test doubles (`Test::WebSocket`, `Test::ConnectionState`, `Test::Client`):
- COOPERATIVE-PEER DEFAULT: an app `websocket.close` still ends CLEAN (peer assumed
  to ack — the realistic common case). Your existing app-close tests keep their
  meaning.
- ABSENT-ACK is a TWO-STEP affordance: (1) mark the peer silent -> the scope stays
  PENDING, no terminal yet (a silent peer is not itself an ending); (2) the test
  explicitly simulates the ending, and the ending KIND selects the reason — a
  simulated deadline expiry -> `close_timeout`; a simulated stream/transport
  termination -> the transport-loss token. Both give `close_code` 1006 unless a peer
  Close was already observed.
- Assert the spec-defined SHAPE (which terminal fired), the reason TOKEN, and 1006 —
  NEVER a deadline value or any server internal.

Helper (`PAGI::WebSocket::close()` / `on_close`):
- AUTHORITATIVE: `on_close`/`close_code`/`close_reason` report the ACTUAL termination
  observed through `pagi.connection` (the peer's code/reason, or 1006 + the reason in
  `disconnect_reason` when the peer never closed). The arguments to `close()` are
  LOCAL INTENT and never substitute for the peer's response.
- `close()` returns when its Close send COMPLETES under the PAGI Send Completion
  Contract (server done with the event — NOT a flush to the peer). `on_close` fires
  later, on the observed terminal, under the existing notification ordering (no new
  promise that it fires after `close()` returns).
- CLEANUP OWNERSHIP: the server publishes the terminal WITHOUT awaiting app cleanup.
  Tools owns ONE cleanup-completion Future that SURVIVES the app handler returning.
  One callback's exception does not stop the others; failures are logged, not
  swallowed.

## Portability guardrail (non-negotiable)
Code ONLY to spec-defined tokens/outcomes (`close_timeout`, the transport-loss
token, `close_code` 1006, which callback fired). NEVER code to a server's internals,
a specific timeout value, or a timer mechanism — the server owns the mechanism; the
default is 10s but that is not part of the contract.

## The joint acceptance gate (still OPEN)
The end-to-end gate closes only when a PAGI-Tools integration test proves the Tools
doubles agree with THIS server checkout across the outcomes above. Until then, the
server contract is implemented-and-reviewed but NOT proven end-to-end.

## Notes
- The server treats a metric shift as expected: some closes that read clean now read
  `close_timeout`/abnormal. That is the point (truthful outcomes), not a regression.
- Model FOUR abnormal close reasons as portable spec tokens: `close_timeout` (peer
  never answered), `close_incomplete` (peer answered but the transport never finished
  closing within the bound), the transport-loss token (transport died first), and the
  app-walked-away `server_error`/1011. `close_code` is the peer's code whenever a valid
  peer Close was observed — including every `close_incomplete` (the peer did answer) —
  and 1006 only when no peer Close was seen (`close_timeout`, transport-loss, or the
  walk-away). The walk-away's accessor `close_code` is 1006, not 1011; 1011 is only the
  code the server SENDS in the `websocket.disconnect` event.
- Ledgered, not built (do not depend on): a numeric counter for the abnormal-close
  population (server has no stats facility; a structured warn line marks each
  `close_timeout`/`close_incomplete` instead). The strict transport-closure semantics
  for peer-initiated close, and a bound on the withheld-close finish, are now BUILT
  (server-owned closure + `close_incomplete`), not a follow-up.
