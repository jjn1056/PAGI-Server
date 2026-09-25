# Header validation boundary research — 2026-09-24

There is concrete repeated work worth a bounded design experiment. It predates
the installed release, so it is a general performance opportunity, not an
explanation of the recent regression. No runtime change has been made and no
native speedup is claimed.

## Evidence

A temporary counting probe invokes the actual HTTP/1 send factory, with a sink
stream and no backpressure. It instruments the existing primitive validators
and delegates to their originals. This measures operation counts, not timing,
and is not an end-to-end socket benchmark. Release and current agree exactly
for one, ten and thirty application fields.

| Event with ten application fields | Release | Current |
| --- | ---: | ---: |
| EventValidator name checks, response start | 10 | 10 |
| HTTP1 serializer name checks, response start | 11 | 11 |
| EventValidator name checks, trailers (including Connection wrappers) | 20 | 20 |
| HTTP1 serializer name checks, trailers | 10 | 10 |

Value checks have identical counts. The extra response-start serializer check
is the server-added Date field. Thus each application field is checked twice
at start and three times as an ordinary HTTP/1 trailer. The preceding GET
profiles independently show this duplication with one application header.

## Source/caller audit

- EventValidator::validate_headers checks tuple shape, scalar types and byte
  safety before send-sequence advancement. Keep this as the app-event boundary.
- HTTP/1 response start subsequently strips server-owned fields, adds Date and
  framing/connection fields, then the serializer repeats the byte checks.
- HTTP/1 trailers additionally pass through a Connection loop that validates
  and copies each pair before the serializer validates it again.
- HTTP/1 SSE starts use mandatory event validation and the same checked HTTP1
  serializer. Server-synthesized HTTP errors call the public serializer without
  going through an app-event validator and must retain checking.
- The documented public HTTP1 serializer is independently callable. Existing
  t/15-crlf-injection.t explicitly requires both response and trailer methods
  to reject CR/LF/NUL injection. Removing its checks outright is not acceptable.
- HTTP/1 WebSocket acceptance constructs wire text directly after validation,
  but repeats header checks in that loop. Subprotocol validation is a different
  check and must remain.
- HTTP/2 response start, trailers, SSE start and WebSocket acceptance likewise
  validate then recheck while copying pairs. The pair copies matter because
  headers may be retained until later sends; they must not be replaced by
  aliases to application arrays merely to remove validation.
- WS/SSE refusals validate HTTP events in the outer protocol validator and
  again in the delegated HTTP send path. That is a separate dispatch/ownership
  issue; do not introduce an already_validated flag or bypass their ordering
  checks in this experiment.

References: Connection.pm send/header paths; EventValidator.pm validate_headers
and protocol validators; Protocol/HTTP1.pm serialize_response_start and
serialize_trailers. The current spec's Www.pod "Header byte safety" requires
rejection through the send Future, not sanitization. Its delivery/trailer
sections describe fail-don't-mutate. No specification edit is needed.

## Proposed boundary (not implemented)

Keep public checked serializers. Extract their wire formatting into private
routines whose input contract is already-valid headers. Both the public entry
point and the internal app-send path share those same formatting routines:

    public serialize_response_start(raw headers)
        -> existing header validation
        -> private serialization routine

    app send(event)
        -> existing mandatory event validation
        -> existing state/framing/header preparation
        -> same private serialization routine

The analogous trailer split removes the middle Connection validation loop as
well. The checked public methods continue to serve direct callers and synthetic
error responses. Server-generated fields on the internal route are presently
known constants, derived framing values and the server's Date formatter; audit
all such additions before switching a caller. Do not assume arbitrary input
becomes trusted merely because it is inside Server.

This adds a small static distinction between checked entry points and encoding.
It adds no validation mode, cache, metadata flag, copied state record or second
encoding implementation. That is a tradeoff to assess, not automatically a
simpler design. The formatter body must remain single-source. Independent
public serializer behavior, including its invalid-input behavior, needs tests.

For the first implementation candidate, bound the work to HTTP/1 serializers
and the ordinary HTTP response/trailer callers (also exercised by refusals).
SSE's use of the public serializer can remain initially. Leave HTTP/2, the
manual WebSocket accept builder, outer refusal validation and header-scan
consolidation unchanged so one benchmark result measures one idea.

## Required checks before accepting a candidate

Retain rejection before sequence mutation, including invalid fields that would
otherwise be stripped and HEAD events whose payload is suppressed. Verify a
valid retry after an invalid start/trailer, direct serializer rejection,
server errors, duplicate fields/order, HTTP/1.0, HEAD, Date/Upgrade ownership,
trailers and refusals. Existing injection, mandatory validation, header ownership
and framing tests provide substantial coverage; add focused cases only for
missing behavior, not tests of a particular private method layout.

Measure release, saved current and candidate with identical headers/bodies:
keep the existing small GET/POST controls and add a modest 10-field response
(and optionally a 30-field sensitivity case). If many-header gains require
special flags or broader dispatch surgery, stop. A count reduction is evidence
of removed work, not evidence of faster requests. Do not treat a win over
current as automatically restoring release parity.

## Status

Research complete. Recommendation: review this checked-entry/shared-encoder
shape before implementation. The other header scans, HTTP/2 rechecks and
refusal double-validation remain possible later work, not bundled changes.
The probe, full counts, current source hashes and work map are in
[header-boundary-data](header-boundary-data/). No source changes, tests,
benchmarks, commits or pushes were performed by this research pass; the probe
was the only executed behavior check.
