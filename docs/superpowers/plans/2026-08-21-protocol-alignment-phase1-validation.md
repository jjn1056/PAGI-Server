# Protocol Alignment Phase 1: Mandatory Event Validation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make PAGI outgoing-event validation mandatory, complete, and shared across every send path (HTTP/1, HTTP/2, WebSocket, SSE, lifespan), with explicit per-family sequence state machines, so malformed and mis-sequenced events fail the `$send` Future in every environment.

**Architecture:** `PAGI::Server::EventValidator` grows from a dev-mode field checker into the single shared authority: strict shape validation (anchored numerics, 0/1 booleans, header byte-safety), complete type registries with unknown-type rejection and extension gating, and pure sequence-transition functions (`advance_http`, `advance_websocket`, `advance_sse`, `advance_lifespan`). Each send dispatcher in `Connection.pm`/`Server.pm` calls it unconditionally, after its transport-closed no-op check (spec order: closed-check → validate → act). `validate_events` becomes a deprecated no-op.

**Tech Stack:** Perl 5 (perlbrew `perl-5.42.2@default`), Test2::V0, IO::Async, Net::Async::HTTP, Future::AsyncAwait.

**Spec:** `docs/superpowers/specs/2026-08-21-pagi-server-protocol-alignment-design.md` sections 6–7 (invariants, mandatory validation), 15.1 (validation matrix), plus PAGI spec repo `main` @ `a7ed9cc` (`PAGI/lib/PAGI/Spec/Www.pod`, `Lifespan.pod`).

## Global Constraints

- Worktree: `/Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server/.worktrees/pagi-0.4-alignment`, branch `fix/pagi-0.4-alignment`, based on `main` @ `b2290dd`. Never read or copy code from the `perf/http-serving-25` checkout at `/Users/jnapiorkowski/Desktop/PAGI-Project/PAGI-Server`.
- Every Perl command runs under perlbrew: `bash -c 'source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.42.2@default && <command>'`. Never system perl.
- Tests are Test2::V0. Behavioral tests spin a real server (`PAGI::Server->new(app => ..., host => '127.0.0.1', port => 0, quiet => 1)`) — no mocks. No new source-text regex tests (design §15); tasks that break existing regex tests replace them with behavioral equivalents in the same commit.
- Error strings from validation are `die`/`croak` messages ending in `\n` where the existing dispatcher style does (Connection.pm) and croak-style in EventValidator (existing style there uses `croak` without `\n` — keep each file's existing convention).
- Validation must stay cheap (design §6.5): no filesystem, network, deep copies. Extra unknown fields on events are always legal (design §6.4).
- POD for every new public function in the same commit. `Changes` entries land in Task 9.
- Update the tracking table (bottom of this file) in the same commit as each task.
- Full-suite runs happen once, at Task 10 — not after every task (design §15.7). Capture verification output to files, never judge truncated output: `prove ... 2>&1 | tee /tmp/phase1-taskN.out` then read the file.

## File Structure

- `lib/PAGI/Server/EventValidator.pm` — rewritten: primitives, header checks, type registries, extension gating, sequence machines. Stays a function library (no OO), loaded with `use` (no longer lazy `require`).
- `lib/PAGI/Server/Connection.pm` — six send dispatchers wired: h1 HTTP (~line 2690 `return async sub` after `sub _create_send`), h1 SSE (~3595), h1 WS (~3930), h2 HTTP (`_h2_create_send`, line 814), h2 WS (`_h2_create_websocket_send`, line 1104), h2 SSE (~1425). Header helpers `_validate_header_name`/`_validate_header_value` (lines 44–63) delegate to EventValidator.
- `lib/PAGI/Server.pm` — lifespan `$send` (~line 4094) validated + phase-tracked; `validate_events` init (~2331) deprecated.
- Tests: extend `t/40-event-validation.t` (unit); new `t/52-mandatory-validation.t` (h1 behavioral), `t/http2/26-mandatory-validation.t` (h2 behavioral), `t/lifespan-send-validation.t`; repair fallout in `t/38-unrecognized-event-type.t`.

---

### Task 1: Strict primitive shapes in EventValidator

**Files:**
- Modify: `lib/PAGI/Server/EventValidator.pm`
- Test: `t/40-event-validation.t`

**Interfaces:**
- Consumes: nothing new.
- Produces: internal helpers used by later tasks: `_is_nonneg_int($v)`, `_is_bool01($v)`, `_is_nonneg_number($v)` — all return boolean, treat `undef`/refs as false. Public behavior change: anchored numeric checks; `more`/`trailers` accept only 0/1; `sse.send` validates `event`/`id` newline bans and non-negative `retry`; `websocket.send` payload must be defined (`bytes => undef` no longer counts as provided).

- [ ] **Step 1: Write the failing tests** — add these subtests to `t/40-event-validation.t` before `done_testing`:

```perl
subtest 'anchored numeric validation' => sub {
    for my $bad ("5\n", " 5", "5 ", "+5", "-5", "5.0", "0x5", [], {}) {
        my $label = ref $bad ? ref($bad) . ' ref' : "'" . ($bad =~ s/\n/\\n/r) . "'";
        like(
            dies { PAGI::Server::EventValidator::validate_http_send({ type => 'http.response.start', status => $bad }) },
            qr/must be a non-negative integer/,
            "status $label throws"
        );
    }
    ok(
        lives { PAGI::Server::EventValidator::validate_http_send({ type => 'http.response.start', status => 200 }) },
        'plain integer status ok'
    );
};

subtest 'boolean-like fields accept only 0 or 1' => sub {
    for my $bad (2, -1, 'yes', '', "1\n", 1.5) {
        like(
            dies { PAGI::Server::EventValidator::validate_http_send({ type => 'http.response.body', body => 'x', more => $bad }) },
            qr/'more' must be 0 or 1/,
            "more '$bad' throws"
        );
    }
    ok( lives { PAGI::Server::EventValidator::validate_http_send({ type => 'http.response.body', body => 'x', more => 1 }) }, 'more 1 ok');
    ok( lives { PAGI::Server::EventValidator::validate_http_send({ type => 'http.response.body', body => 'x', more => 0 }) }, 'more 0 ok');
    like(
        dies { PAGI::Server::EventValidator::validate_http_send({ type => 'http.response.start', status => 200, trailers => 'soon' }) },
        qr/'trailers' must be 0 or 1/,
        'trailers non-bool throws'
    );
};

subtest 'sse.send field safety' => sub {
    like(
        dies { PAGI::Server::EventValidator::validate_sse_send({ type => 'sse.send', data => 'x', event => "up\ndate" }) },
        qr/'event' must not contain newline/,
        'newline in event throws'
    );
    like(
        dies { PAGI::Server::EventValidator::validate_sse_send({ type => 'sse.send', data => 'x', id => "1\r2" }) },
        qr/'id' must not contain newline/,
        'CR in id throws'
    );
    like(
        dies { PAGI::Server::EventValidator::validate_sse_send({ type => 'sse.send', data => 'x', retry => -5 }) },
        qr/'retry' must be a non-negative integer/,
        'negative retry throws'
    );
    ok(
        lives { PAGI::Server::EventValidator::validate_sse_send({ type => 'sse.send', data => "multi\nline", event => 'update', id => '7', retry => 3000 }) },
        'valid full sse.send ok (data may contain newlines)'
    );
};

subtest 'interval validation is anchored' => sub {
    for my $bad ('1.2.3', '.', '', '5x', "3\n") {
        like(
            dies { PAGI::Server::EventValidator::validate_sse_send({ type => 'sse.keepalive', interval => $bad }) },
            qr/'interval' must be a non-negative number/,
            "interval '$bad' throws"
        );
    }
    ok( lives { PAGI::Server::EventValidator::validate_sse_send({ type => 'sse.keepalive', interval => 0 }) }, 'interval 0 ok');
    ok( lives { PAGI::Server::EventValidator::validate_websocket_send({ type => 'websocket.keepalive', interval => 30.5, timeout => 20 }) }, 'float interval + timeout ok');
    like(
        dies { PAGI::Server::EventValidator::validate_websocket_send({ type => 'websocket.keepalive', interval => 30, timeout => 'later' }) },
        qr/'timeout' must be a non-negative number/,
        'non-numeric ws timeout throws'
    );
};

subtest 'websocket.send payload must be defined' => sub {
    like(
        dies { PAGI::Server::EventValidator::validate_websocket_send({ type => 'websocket.send', bytes => undef }) },
        qr/exactly one of bytes\/text/,
        'undef bytes does not count as provided'
    );
};
```

- [ ] **Step 2: Run to verify failure**

Run: `bash -c 'source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.42.2@default && prove -l t/40-event-validation.t 2>&1 | tee /tmp/phase1-task1-fail.out'` then read the file.
Expected: new subtests FAIL (old regexes accept `"5\n"` because `/^\d+$/` matches with trailing newline; no newline/retry checks exist).

- [ ] **Step 3: Implement** — in `lib/PAGI/Server/EventValidator.pm`, add primitives after `use Carp qw(croak);`:

```perl
# --- Primitive shape checks (anchored; undef and refs are always false) ---

sub _is_nonneg_int {
    my ($v) = @_;
    return defined $v && !ref $v && $v =~ /\A[0-9]+\z/;
}

sub _is_bool01 {
    my ($v) = @_;
    return defined $v && !ref $v && $v =~ /\A[01]\z/;
}

sub _is_nonneg_number {
    my ($v) = @_;
    return defined $v && !ref $v && $v =~ /\A[0-9]+(?:\.[0-9]+)?\z/;
}
```

Then update every field check to use them, with these exact rules and messages (`$T` below stands for the event type string in the message):

- `status` (http.response.start required; ws denial start required; sse decline start required; sse.start optional): `croak "$T 'status' must be a non-negative integer"` unless `_is_nonneg_int`.
- `offset`, `length` (http.response.body, optional): `croak "$T '<field>' must be a non-negative integer"` unless `_is_nonneg_int` when the key exists and is defined.
- `more` (http.response.body, ws denial body, sse decline body — optional): when key exists and defined, `croak "$T 'more' must be 0 or 1"` unless `_is_bool01`.
- `trailers` (http.response.start, optional): same 0/1 rule, message `'trailers' must be 0 or 1`.
- `code` (websocket.close, optional): `_is_nonneg_int`, message `'code' must be a non-negative integer`.
- `interval` (websocket.keepalive, sse.keepalive — required): `croak "$T requires 'interval' field"` unless key exists; `croak "$T 'interval' must be a non-negative number"` unless `_is_nonneg_number`.
- `timeout` (websocket.keepalive, optional): `_is_nonneg_number`, message `'timeout' must be a non-negative number`.
- `websocket.send`: replace the `exists`-count with defined-count: `my $count = (defined $event->{bytes} ? 1 : 0) + (defined $event->{text} ? 1 : 0);` keep message `websocket.send requires exactly one of bytes/text (got $count)`.
- `sse.send`: after the existing `data` checks add:

```perl
    for my $f (qw(event id)) {
        next unless exists $event->{$f} && defined $event->{$f};
        croak "sse.send '$f' must be a string" if ref $event->{$f};
        croak "sse.send '$f' must not contain newline characters"
            if $event->{$f} =~ /[\r\n]/;
    }
    if (exists $event->{retry} && defined $event->{retry}) {
        croak "sse.send 'retry' must be a non-negative integer"
            unless _is_nonneg_int($event->{retry});
    }
```

- [ ] **Step 4: Run to verify pass**

Run: `bash -c 'source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.42.2@default && prove -l t/40-event-validation.t 2>&1 | tee /tmp/phase1-task1-pass.out'` and read the file. Expected: PASS (all subtests, including the pre-existing ones — their valid-event cases must still pass).

- [ ] **Step 5: Commit**

```bash
git add lib/PAGI/Server/EventValidator.pm t/40-event-validation.t docs/superpowers/plans/2026-08-21-protocol-alignment-phase1-validation.md
git commit -m "feat(validator): anchored numerics, 0/1 booleans, sse field safety"
```

---

### Task 2: Shared header validation

**Files:**
- Modify: `lib/PAGI/Server/EventValidator.pm`, `lib/PAGI/Server/Connection.pm:44-63`
- Test: `t/40-event-validation.t`, existing `t/15-crlf-injection.t` must stay green

**Interfaces:**
- Consumes: nothing from Task 1.
- Produces: `PAGI::Server::EventValidator::check_header_name($name)` and `check_header_value($value)` (croak on bad bytes, return the value); `validate_headers($headers, $event_type)` (croaks unless arrayref of 2-element arrayrefs of defined non-ref scalars with safe bytes). Called by every shape validator that sees a `headers` key. `Connection.pm`'s `_validate_header_name`/`_validate_header_value` become one-line delegates (their `die "...\n"` messages preserved by the shared functions — see Step 3).

- [ ] **Step 1: Write the failing tests** — add to `t/40-event-validation.t`:

```perl
subtest 'header tuple and byte safety validation' => sub {
    my $start = sub { { type => 'http.response.start', status => 200, headers => $_[0] } };
    like( dies { PAGI::Server::EventValidator::validate_http_send($start->([ 'not-a-tuple' ])) },
        qr/each header must be a 2-element array reference/, 'flat element throws');
    like( dies { PAGI::Server::EventValidator::validate_http_send($start->([ ['a','b','c'] ])) },
        qr/each header must be a 2-element array reference/, '3-element tuple throws');
    like( dies { PAGI::Server::EventValidator::validate_http_send($start->([ ['a', undef] ])) },
        qr/header name and value must be defined strings/, 'undef value throws');
    like( dies { PAGI::Server::EventValidator::validate_http_send($start->([ [['ref'],'b'] ])) },
        qr/header name and value must be defined strings/, 'ref name throws');
    like( dies { PAGI::Server::EventValidator::validate_http_send($start->([ ['x-evil', "a\r\nInjected: yes"] ])) },
        qr/contains CR, LF, or null byte/, 'CRLF value throws');
    like( dies { PAGI::Server::EventValidator::validate_http_send($start->([ ["x\x01bad", 'v'] ])) },
        qr/contains control characters/, 'control char in name throws');
    ok( lives { PAGI::Server::EventValidator::validate_http_send($start->([ ['content-type','text/plain'], ['set-cookie','a=1'], ['set-cookie','b=2'] ])) },
        'valid headers with duplicates ok');

    # Shared across families: ws denial start and sse decline start use the same checks
    like( dies { PAGI::Server::EventValidator::validate_websocket_send({ type => 'websocket.http.response.start', status => 401, headers => [ ['h',"v\n"] ] }, { extensions => { 'websocket.http.response' => {} } }) },
        qr/contains CR, LF, or null byte/, 'ws denial headers validated');
    like( dies { PAGI::Server::EventValidator::validate_sse_send({ type => 'sse.http.response.start', status => 404, headers => [ ['h',"v\0"] ] }) },
        qr/contains CR, LF, or null byte/, 'sse decline headers validated');
};
```

Note: the ws-denial call passes an `$opts` second argument that Task 3 defines. To keep this task self-contained, Task 2's implementation must already tolerate (ignore) a second argument — Perl functions do by default.

- [ ] **Step 2: Run to verify failure** — `prove -l t/40-event-validation.t` under perlbrew, tee to `/tmp/phase1-task2-fail.out`, read it. Expected: FAIL (`headers` currently only checked as `ref eq 'ARRAY'`).

- [ ] **Step 3: Implement** — in EventValidator add:

```perl
# --- Shared header validation (byte safety per PAGI Www.pod "Header byte safety") ---
# Names: no CR/LF/NUL and no other control characters. Values: no CR/LF/NUL.

sub check_header_value {
    my ($value) = @_;
    die "Invalid header value: contains CR, LF, or null byte\n"
        if $value =~ /[\r\n\0]/;
    return $value;
}

sub check_header_name {
    my ($name) = @_;
    die "Invalid header name: contains CR, LF, or null byte\n"
        if $name =~ /[\r\n\0]/;
    die "Invalid header name: contains control characters\n"
        if $name =~ /[[:cntrl:]]/;
    return $name;
}

sub validate_headers {
    my ($headers, $event_type) = @_;
    croak "$event_type 'headers' must be an array reference"
        unless ref $headers eq 'ARRAY';
    for my $h (@$headers) {
        croak "$event_type headers: each header must be a 2-element array reference"
            unless ref $h eq 'ARRAY' && @$h == 2;
        croak "$event_type headers: header name and value must be defined strings"
            if !defined $h->[0] || !defined $h->[1] || ref $h->[0] || ref $h->[1];
        check_header_name($h->[0]);
        check_header_value($h->[1]);
    }
    return;
}
```

Replace every existing `'headers' must be an array reference` check (http.response.start, http.response.trailers, websocket.accept, ws denial start, sse.start, sse decline start) with `validate_headers($event->{headers}, '<event type>') if exists $event->{headers} && defined $event->{headers};`.

In `Connection.pm`, replace the bodies of `_validate_header_name` and `_validate_header_value` (lines 44–63) with delegation, keeping `_validate_subprotocol` untouched:

```perl
sub _validate_header_value { PAGI::Server::EventValidator::check_header_value($_[0]) }

sub _validate_header_name  { PAGI::Server::EventValidator::check_header_name($_[0]) }
```

and add `use PAGI::Server::EventValidator;` to Connection.pm's module header (top of file, with the other `use` lines). The die messages are byte-identical to the old ones, so `t/15-crlf-injection.t` stays green.

- [ ] **Step 4: Run to verify pass** — `prove -l t/40-event-validation.t t/15-crlf-injection.t` under perlbrew, tee to `/tmp/phase1-task2-pass.out`, read it. Expected: PASS both files.

- [ ] **Step 5: Commit**

```bash
git add lib/PAGI/Server/EventValidator.pm lib/PAGI/Server/Connection.pm t/40-event-validation.t docs/superpowers/plans/2026-08-21-protocol-alignment-phase1-validation.md
git commit -m "feat(validator): shared header tuple and byte-safety validation"
```

---

### Task 3: Type registries, unknown-type rejection, extension gating, lifespan validator

**Files:**
- Modify: `lib/PAGI/Server/EventValidator.pm`
- Test: `t/40-event-validation.t`

**Interfaces:**
- Consumes: Task 1 primitives, Task 2 `validate_headers`.
- Produces: final validator signatures used by Tasks 5–8:
  - `validate_http_send($event, $opts)` — `$opts = { extensions => \%scope_extensions }`, optional. Recognized: `http.response.start`, `http.response.body`, `http.response.trailers`, `http.fullflush` (the last only when `$opts->{extensions}{fullflush}` exists — otherwise croak `"Extension not enabled: fullflush"`).
  - `validate_websocket_send($event, $opts)` — recognized: `websocket.accept`, `websocket.send`, `websocket.close`, `websocket.keepalive`, plus `websocket.http.response.start`/`websocket.http.response.body` only when `$opts->{extensions}{'websocket.http.response'}` exists (else croak `"Extension not enabled: websocket.http.response"`).
  - `validate_sse_send($event)` — recognized: `sse.start`, `sse.send`, `sse.comment`, `sse.keepalive`, `sse.close`, `sse.http.response.start`, `sse.http.response.body` (decline is core, no gating).
  - `validate_lifespan_send($event)` — recognized: `lifespan.startup.complete`, `lifespan.startup.failed`, `lifespan.shutdown.complete`, `lifespan.shutdown.failed`; `message` optional, must be a defined non-ref string when present (croak `"$T 'message' must be a string"`).
  - Unknown types croak exactly: `"Unrecognized event type '$type' for $family protocol"` where `$family` is `http`/`websocket`/`sse`/`lifespan` (matches `Connection.pm::_unrecognized_event_type` wording).

- [ ] **Step 1: Write the failing tests** — add to `t/40-event-validation.t`:

```perl
subtest 'unknown event types are rejected per family' => sub {
    like( dies { PAGI::Server::EventValidator::validate_http_send({ type => 'http.response.bod' }) },
        qr/Unrecognized event type 'http\.response\.bod' for http protocol/, 'http typo throws');
    like( dies { PAGI::Server::EventValidator::validate_websocket_send({ type => 'websocket.pong' }) },
        qr/Unrecognized event type .* for websocket protocol/, 'ws unknown throws');
    like( dies { PAGI::Server::EventValidator::validate_sse_send({ type => 'sse.push' }) },
        qr/Unrecognized event type .* for sse protocol/, 'sse unknown throws');
    like( dies { PAGI::Server::EventValidator::validate_http_send({}) },
        qr/Unrecognized event type '' for http protocol/, 'missing type throws');
    ok( lives { PAGI::Server::EventValidator::validate_http_send({ type => 'http.response.body', body => 'x', unknown_extra => 1 }) },
        'extra fields on a known type remain legal');
};

subtest 'extension-gated event types' => sub {
    like( dies { PAGI::Server::EventValidator::validate_http_send({ type => 'http.fullflush' }) },
        qr/Extension not enabled: fullflush/, 'fullflush without extension throws');
    ok( lives { PAGI::Server::EventValidator::validate_http_send({ type => 'http.fullflush' }, { extensions => { fullflush => {} } }) },
        'fullflush with extension ok');
    like( dies { PAGI::Server::EventValidator::validate_websocket_send({ type => 'websocket.http.response.start', status => 401 }) },
        qr/Extension not enabled: websocket\.http\.response/, 'ws denial without extension throws');
    ok( lives { PAGI::Server::EventValidator::validate_websocket_send({ type => 'websocket.http.response.start', status => 401 }, { extensions => { 'websocket.http.response' => {} } }) },
        'ws denial with extension ok');
};

subtest 'lifespan send validation' => sub {
    ok( lives { PAGI::Server::EventValidator::validate_lifespan_send({ type => 'lifespan.startup.complete' }) }, 'startup.complete ok');
    ok( lives { PAGI::Server::EventValidator::validate_lifespan_send({ type => 'lifespan.startup.failed', message => 'db down' }) }, 'startup.failed with message ok');
    like( dies { PAGI::Server::EventValidator::validate_lifespan_send({ type => 'lifespan.startup.done' }) },
        qr/Unrecognized event type .* for lifespan protocol/, 'unknown lifespan type throws');
    like( dies { PAGI::Server::EventValidator::validate_lifespan_send({ type => 'lifespan.shutdown.failed', message => {} }) },
        qr/'message' must be a string/, 'ref message throws');
};
```

- [ ] **Step 2: Run to verify failure** — `prove -l t/40-event-validation.t`, tee `/tmp/phase1-task3-fail.out`, read. Expected: FAIL (unknown types currently fall through silently; `validate_lifespan_send` undefined).

- [ ] **Step 3: Implement** — restructure the three `validate_*_send` functions as full dispatch tables with a shared unknown-type croak, add `$opts` (`my ($event, $opts) = @_; my $ext = ($opts && $opts->{extensions}) || {};`), the two extension gates, and:

```perl
sub validate_lifespan_send {
    my ($event) = @_;
    my $type = $event->{type} // '';

    if ($type eq 'lifespan.startup.complete' or $type eq 'lifespan.shutdown.complete') {
        # no fields beyond type
    }
    elsif ($type eq 'lifespan.startup.failed' or $type eq 'lifespan.shutdown.failed') {
        if (exists $event->{message} && defined $event->{message}) {
            croak "$type 'message' must be a string" if ref $event->{message};
        }
    }
    else {
        croak "Unrecognized event type '$type' for lifespan protocol";
    }
    return;
}
```

The `else` branch of each family's dispatch chain croaks `"Unrecognized event type '$type' for $family protocol"`. Remove the old trailing comments about fullflush "no required fields".

- [ ] **Step 4: Run to verify pass** — `prove -l t/40-event-validation.t`, tee `/tmp/phase1-task3-pass.out`, read. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lib/PAGI/Server/EventValidator.pm t/40-event-validation.t docs/superpowers/plans/2026-08-21-protocol-alignment-phase1-validation.md
git commit -m "feat(validator): complete type registries, unknown rejection, extension gating, lifespan events"
```

---

### Task 4: Sequence state machines

**Files:**
- Modify: `lib/PAGI/Server/EventValidator.pm`
- Test: `t/40-event-validation.t`

**Interfaces:**
- Consumes: nothing (pure functions; shape validation is separate and runs first).
- Produces (used verbatim by Tasks 5–8): each takes `($state, $event)` and returns the next state string, or croaks with a message starting `"cannot send '<type>' ..."`. Initial states: HTTP `'initial'`, WebSocket `'connecting'`, SSE `'initial'`, lifespan `'startup_pending'`.
  - `advance_http($state, $event)` — states `initial`, `started`, `started_t` (trailers declared), `awaiting_trailers`, `complete`. `initial`+start → `started`/`started_t` (by `trailers` field); `started`+body(more=1)→`started`; `started`+body(terminal: more=0/absent, or file/fh present)→`complete`; `started_t`+body(more=1)→`started_t`; `started_t`+body(terminal)→`awaiting_trailers`; `awaiting_trailers`+trailers→`complete`; fullflush legal in `started`/`started_t`/`awaiting_trailers` (state unchanged). Croaks: any event in `initial` except start (`cannot send '<type>' before http.response.start`); start when not `initial` (`cannot send duplicate http.response.start`); trailers unless `awaiting_trailers` (`cannot send http.response.trailers: trailers were not declared or body is not complete`); anything in `complete` (`cannot send '<type>': response already complete`).
  - `advance_websocket($state, $event)` — states `connecting`, `accepted`, `denial`, `denial_complete`, `closed`. `connecting`: accept→`accepted`, close→`closed`, denial start→`denial`, send/keepalive croak `cannot send '<type>' before websocket.accept`. `accepted`: send/keepalive→`accepted`, close→`closed`, accept croaks `cannot send 'websocket.accept' after websocket.accept`. `denial`: denial body(more=1)→`denial`, (terminal)→`denial_complete`. Croaks: denial events after accept (`cannot send '<type>' after websocket.accept`); accept/send/keepalive after denial start (`cannot send '<type>' after websocket.http.response.start`); anything in `closed` (`cannot send '<type>' after websocket.close`) or `denial_complete` (`cannot send '<type>': denial response already complete`).
  - `advance_sse($state, $event)` — states `initial`, `streaming`, `declining`, `decline_complete`, `closed`. `initial`: sse.start→`streaming`, decline start→`declining`, anything else croaks `cannot send '<type>' before sse.start`. `streaming`: send/comment/keepalive→`streaming`, sse.close→`closed`, decline start croaks `cannot decline with sse.http.response.start after sse.start`, duplicate sse.start croaks `cannot send duplicate sse.start`. `declining`: decline body(more=1)→`declining`, (terminal)→`decline_complete`, anything else croaks `cannot send '<type>' after sse.http.response.start`. `closed`: sse.close→`closed` (idempotent), anything else croaks `cannot send '<type>' after sse.close`. `decline_complete`: anything croaks `cannot send '<type>': decline response already complete`.
  - `advance_lifespan($state, $event)` — states `startup_pending`, `running`, `shutdown_pending`, `finished`. `startup_pending`: startup.complete→`running`, startup.failed→`finished`. `shutdown_pending`: shutdown.complete/failed→`finished`. Everything else croaks `cannot send '<type>' during lifespan phase '<state>'`. (The server moves `running`→`shutdown_pending` itself when it emits the shutdown event — not via this function.)

- [ ] **Step 1: Write the failing tests** — add to `t/40-event-validation.t` (representative matrix; the implementer extends the same pattern to every croak listed above — each croak message in the Interfaces block gets at least one `like(dies ...)` and each legal transition one `is(...)`):

```perl
subtest 'advance_http transition matrix' => sub {
    my $adv = \&PAGI::Server::EventValidator::advance_http;
    is( $adv->('initial', { type => 'http.response.start', status => 200 }), 'started', 'start -> started');
    is( $adv->('initial', { type => 'http.response.start', status => 200, trailers => 1 }), 'started_t', 'start+trailers -> started_t');
    is( $adv->('started', { type => 'http.response.body', body => 'x', more => 1 }), 'started', 'streaming chunk keeps started');
    is( $adv->('started', { type => 'http.response.body', body => 'x' }), 'complete', 'terminal body -> complete');
    is( $adv->('started', { type => 'http.response.body', file => '/tmp/f' }), 'complete', 'file body -> complete');
    is( $adv->('started_t', { type => 'http.response.body', body => 'x', more => 0 }), 'awaiting_trailers', 'terminal body with declared trailers -> awaiting_trailers');
    is( $adv->('awaiting_trailers', { type => 'http.response.trailers', headers => [] }), 'complete', 'trailers -> complete');
    like( dies { $adv->('initial', { type => 'http.response.body', body => 'x' }) }, qr/before http\.response\.start/, 'body before start');
    like( dies { $adv->('started', { type => 'http.response.start', status => 200 }) }, qr/duplicate http\.response\.start/, 'duplicate start');
    like( dies { $adv->('started', { type => 'http.response.trailers' }) }, qr/not declared/, 'undeclared trailers');
    like( dies { $adv->('complete', { type => 'http.response.body', body => 'x' }) }, qr/already complete/, 'body after completion');
};

subtest 'advance_sse close is idempotent, streams stay exclusive' => sub {
    my $adv = \&PAGI::Server::EventValidator::advance_sse;
    is( $adv->('initial', { type => 'sse.start' }), 'streaming', 'start -> streaming');
    is( $adv->('streaming', { type => 'sse.close' }), 'closed', 'close -> closed');
    is( $adv->('closed', { type => 'sse.close' }), 'closed', 'second close idempotent');
    like( dies { $adv->('closed', { type => 'sse.send', data => 'x' }) }, qr/after sse\.close/, 'send after close');
    is( $adv->('initial', { type => 'sse.http.response.start', status => 404 }), 'declining', 'decline start');
    like( dies { $adv->('streaming', { type => 'sse.http.response.start', status => 404 }) }, qr/after sse\.start/, 'decline after start');
    like( dies { $adv->('declining', { type => 'sse.send', data => 'x' }) }, qr/after sse\.http\.response\.start/, 'stream event while declining');
};

subtest 'advance_websocket denial and accept are exclusive' => sub {
    my $adv = \&PAGI::Server::EventValidator::advance_websocket;
    is( $adv->('connecting', { type => 'websocket.accept' }), 'accepted', 'accept');
    is( $adv->('connecting', { type => 'websocket.http.response.start', status => 401 }), 'denial', 'denial start');
    like( dies { $adv->('connecting', { type => 'websocket.keepalive', interval => 30 }) }, qr/before websocket\.accept/, 'keepalive before accept');
    like( dies { $adv->('accepted', { type => 'websocket.accept' }) }, qr/after websocket\.accept/, 'duplicate accept');
    like( dies { $adv->('accepted', { type => 'websocket.http.response.start', status => 401 }) }, qr/after websocket\.accept/, 'denial after accept');
    like( dies { $adv->('denial', { type => 'websocket.send', text => 'x' }) }, qr/after websocket\.http\.response\.start/, 'frame while denying');
    like( dies { $adv->('closed', { type => 'websocket.send', text => 'x' }) }, qr/after websocket\.close/, 'send after close');
};

subtest 'advance_lifespan phases' => sub {
    my $adv = \&PAGI::Server::EventValidator::advance_lifespan;
    is( $adv->('startup_pending', { type => 'lifespan.startup.complete' }), 'running', 'startup completes');
    is( $adv->('shutdown_pending', { type => 'lifespan.shutdown.complete' }), 'finished', 'shutdown completes');
    like( dies { $adv->('startup_pending', { type => 'lifespan.shutdown.complete' }) }, qr/during lifespan phase 'startup_pending'/, 'shutdown result during startup');
    like( dies { $adv->('running', { type => 'lifespan.startup.complete' }) }, qr/during lifespan phase 'running'/, 'late startup result');
};
```

- [ ] **Step 2: Run to verify failure** — `prove -l t/40-event-validation.t`, tee `/tmp/phase1-task4-fail.out`, read. Expected: FAIL (functions don't exist).

- [ ] **Step 3: Implement** the four functions in EventValidator exactly per the Interfaces block. Terminal-body detection for HTTP:

```perl
sub _http_body_is_terminal {
    my ($event) = @_;
    return 1 if defined $event->{file} || defined $event->{fh};
    my $more = $event->{more} // 0;
    return !$more;
}
```

Croak messages must match the Interfaces block verbatim — Tasks 5–8's behavioral tests match against them.

- [ ] **Step 4: Run to verify pass** — `prove -l t/40-event-validation.t`, tee `/tmp/phase1-task4-pass.out`, read. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lib/PAGI/Server/EventValidator.pm t/40-event-validation.t docs/superpowers/plans/2026-08-21-protocol-alignment-phase1-validation.md
git commit -m "feat(validator): per-family send sequence state machines"
```

---

### Task 5: HTTP/1 HTTP dispatcher — mandatory validation and sequencing

**Files:**
- Modify: `lib/PAGI/Server/Connection.pm` (h1 HTTP send closure, `return async sub` near line 2690; the `if ($weak_self->{validate_events})` guard near 2703)
- Create: `t/52-mandatory-validation.t`
- Modify: `t/38-unrecognized-event-type.t` (only if broken — see Step 4)

**Interfaces:**
- Consumes: `validate_http_send($event, { extensions => ... })` (Task 3), `advance_http` (Task 4).
- Produces: the wiring pattern Tasks 6–7 replicate: closed-check first (already present: `return Future->done if $weak_self->{closed};`), then unconditional shape validation, then `$seq = advance_http($seq, $event)`, then the existing behavior branches. The h1 closure gains `my $seq = 'initial';` beside its existing `$response_started` declaration.

- [ ] **Step 1: Write the failing behavioral test** — create `t/52-mandatory-validation.t`:

```perl
use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use Net::Async::HTTP;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";
use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# Each path exercises one send-contract violation. The app catches the failed
# $send Future and reports what happened in a valid response, so the test
# observes validation through real server behavior, never source inspection.
my $app = async sub {
    my ($scope, $receive, $send) = @_;
    die "unsupported scope" if $scope->{type} ne 'http';
    while (1) {
        my $e = await $receive->();
        last if $e->{type} ne 'http.request' || !$e->{more};
    }
    my $path = $scope->{path} // '/';

    my $report = async sub {
        my ($err) = @_;
        $err //= 'NO-ERROR';
        $err =~ s/\n/ /g;
        await $send->({ type => 'http.response.start', status => 200,
                        headers => [['content-type','text/plain']] });
        await $send->({ type => 'http.response.body', body => $err, more => 0 });
    };

    if ($path eq '/bad-status') {
        my $err = do { local $@; eval { await $send->({ type => 'http.response.start', status => "50\n0" }) }; $@ };
        return await $report->($err);
    }
    if ($path eq '/body-before-start') {
        my $err = do { local $@; eval { await $send->({ type => 'http.response.body', body => 'x' }) }; $@ };
        return await $report->($err);
    }
    if ($path eq '/unknown-type') {
        my $err = do { local $@; eval { await $send->({ type => 'http.response.bod', body => 'x' }) }; $@ };
        return await $report->($err);
    }
    if ($path eq '/duplicate-start') {
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-type','text/plain']] });
        my $err = do { local $@; eval { await $send->({ type => 'http.response.start', status => 500 }) }; $@ };
        $err =~ s/\n/ /g;
        await $send->({ type => 'http.response.body', body => "dup:$err", more => 0 });
        return;
    }
    if ($path eq '/undeclared-trailers') {
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-type','text/plain']] });
        my $err = do { local $@; eval { await $send->({ type => 'http.response.trailers', headers => [['x-t','1']] }) }; $@ };
        $err =~ s/\n/ /g;
        await $send->({ type => 'http.response.body', body => "trailers:$err", more => 0 });
        return;
    }
    if ($path eq '/post-close') {
        # Post-close no-op: complete the response, wait for the client to go
        # away, then send a malformed event. Spec order: closed-check runs
        # BEFORE validation, so this must RESOLVE, not fail.
        await $send->({ type => 'http.response.start', status => 200,
                        headers => [['content-type','text/plain'], ['connection','close']] });
        await $send->({ type => 'http.response.body', body => 'bye', more => 0 });
        while (1) {
            my $e = await $receive->();
            last if $e->{type} eq 'http.disconnect';
        }
        $PostClose::RESULT = do {
            local $@;
            eval { await $send->({ type => 'http.response.bod', body => 'zombie' }); 'resolved' }
                // "failed: $@";
        };
        return;
    }
    await $report->(undef);   # control path: no violation
};

my $loop = IO::Async::Loop->new;
my $server = PAGI::Server->new(
    app => $app, host => '127.0.0.1', port => 0, quiet => 1,
    validate_events => 0,   # THE POINT: validation must run anyway
);
$loop->add($server);
$server->listen->get;
my $port = $server->port;
my $http = Net::Async::HTTP->new;
$loop->add($http);

my $get = sub { $http->GET("http://127.0.0.1:$port$_[0]")->get };

like( $get->('/bad-status')->content, qr/must be a non-negative integer/,
    'malformed status fails the send Future with validate_events => 0' );
like( $get->('/body-before-start')->content, qr/before http\.response\.start/,
    'body before start fails' );
like( $get->('/unknown-type')->content, qr/Unrecognized event type/,
    'unknown type fails' );
like( $get->('/duplicate-start')->content, qr/dup:.*duplicate http\.response\.start/,
    'duplicate start fails without disturbing the real response' );
like( $get->('/undeclared-trailers')->content, qr/trailers:.*not declared/,
    'undeclared trailers fail' );
is( $get->('/ok')->content, 'NO-ERROR', 'a conforming app is unaffected' );

# Post-close: malformed send after client disconnect resolves as a no-op
is( $get->('/post-close')->content, 'bye', 'post-close response delivered' );
$loop->loop_once(0.1) for 1..5;   # let the disconnect and probe land
is( $PostClose::RESULT, 'resolved',
    'malformed send after close resolves as a no-op (closed-check precedes validation)' );

# Dev-configuration parity: validate_events => 1 behaves identically
my $dev_server = PAGI::Server->new(
    app => $app, host => '127.0.0.1', port => 0, quiet => 1,
    validate_events => 1,
);
$loop->add($dev_server);
$dev_server->listen->get;
my $dev_port = $dev_server->port;
like( $http->GET("http://127.0.0.1:$dev_port/bad-status")->get->content,
    qr/must be a non-negative integer/,
    'development configuration validates identically' );
$dev_server->shutdown->get;

$server->shutdown->get;
done_testing;
```

- [ ] **Step 2: Run to verify failure** — `prove -l t/52-mandatory-validation.t`, tee `/tmp/phase1-task5-fail.out`, read. Expected: FAIL — with `validate_events => 0` nothing validates: `/bad-status` responds with a garbage status line or hangs, `/body-before-start` and `/duplicate-start` are silently ignored so the app sees `NO-ERROR`.

- [ ] **Step 3: Implement** — in the h1 HTTP send closure (`_create_send`'s `return async sub` near line 2690):

Replace:

```perl
        # Dev-mode event validation (PAGI spec compliance)
        if ($weak_self->{validate_events}) {
            require PAGI::Server::EventValidator;
            PAGI::Server::EventValidator::validate_http_send($event);
        }
```

with (the `use` from Task 2 makes `require` unnecessary):

```perl
        # Mandatory event validation and sequencing (PAGI spec compliance).
        # Order per spec: transport-closed no-op check above runs first.
        PAGI::Server::EventValidator::validate_http_send(
            $event, { extensions => $weak_self->{extensions} });
        $seq = PAGI::Server::EventValidator::advance_http($seq, $event);
```

Declare `my $seq = 'initial';` next to the closure's existing `my $response_started = 0;` (same `_create_send` scope). Then remove the silent sequence guards the machine replaces in this closure: `return if $response_started;` under `http.response.start` (duplicate start now croaks), and any `return unless $response_started;` / `return if <body-complete flag>;` guards under the body and trailers branches (body-before-start and body-after-complete now croak). Keep `$response_started` itself: the behavior code still branches on it. A croak inside this `async sub` before its first `await` becomes a failed Future — exactly the contract the test observes.

- [ ] **Step 4: Run to verify pass, plus neighbors** — `prove -l t/52-mandatory-validation.t t/40-event-validation.t t/38-unrecognized-event-type.t t/01-hello-http.t t/02-streaming.t t/10-http-compliance.t t/16-chunked-validation.t t/http-incomplete-response.t`, tee `/tmp/phase1-task5-pass.out`, read in full. Expected: all PASS. If `t/38`'s source-regex subtest fails because call sites moved, replace the failed assertions with a behavioral note pointing at `t/52-mandatory-validation.t` — the file keeps its `helper function logic` subtest. If `t/40`'s `server has validation hooks` source-regex subtest fails (it counts `EventValidator::validate_` calls and the `if (\$weak_self->{validate_events})` guard), rewrite that subtest to assert the new reality behaviorally-adjacent: count `validate_` calls with the new expected number and assert the guard is GONE (`unlike ... qr/if \(\$weak_self->\{validate_events\}\)/`) — final conversion to fully behavioral coverage completes in Task 9.

- [ ] **Step 5: Commit**

```bash
git add lib/PAGI/Server/Connection.pm t/52-mandatory-validation.t t/38-unrecognized-event-type.t t/40-event-validation.t docs/superpowers/plans/2026-08-21-protocol-alignment-phase1-validation.md
git commit -m "feat: mandatory validation and sequencing on the HTTP/1 send path"
```

---

### Task 6: HTTP/1 SSE and WebSocket dispatchers

**Files:**
- Modify: `lib/PAGI/Server/Connection.pm` (h1 SSE send closure ~3595–3615; h1 WS send closure ~3930–3950)
- Test: extend `t/52-mandatory-validation.t`

**Interfaces:**
- Consumes: `validate_sse_send`, `validate_websocket_send($event, { extensions => ... })`, `advance_sse`, `advance_websocket`.
- Produces: h1 SSE/WS closures each hold `my $seq = 'initial';` / `my $seq = 'connecting';` and run closed-check → validate → advance → behavior. The ad-hoc sequence dies in the SSE closure (`cannot send '$type' after sse.close` near line 3592-area and `cannot send '$type' after sse.http.response.start` near 3596, and the `cannot decline with sse.http.response.start after sse.start` die near 1555's h1 twin) are REMOVED — the machine now owns those croaks, with the same message texts (Task 4 chose them to match).

- [ ] **Step 1: Write the failing tests** — append to `t/52-mandatory-validation.t` before `done_testing` (reuse `$loop`; new server with an SSE+WS app):

```perl
# --- SSE: mis-sequencing and malformed fields fail the send Future ---
my $sse_app = async sub {
    my ($scope, $receive, $send) = @_;
    if ($scope->{type} eq 'sse') {
        my $e = await $receive->();   # sse.request
        my $path = $scope->{path} // '/';
        if ($path eq '/sse-send-before-start') {
            my $err = do { local $@; eval { await $send->({ type => 'sse.send', data => 'early' }) }; $@ };
            # decline with the error so the client can read it as a plain response
            await $send->({ type => 'sse.http.response.start', status => 200, headers => [['content-type','text/plain']] });
            ($err //= 'NO-ERROR') =~ s/\n/ /g;
            await $send->({ type => 'sse.http.response.body', body => $err, more => 0 });
            return;
        }
        if ($path eq '/sse-newline-event') {
            await $send->({ type => 'sse.start', status => 200 });
            my $err = do { local $@; eval { await $send->({ type => 'sse.send', data => 'x', event => "a\nb" }) }; $@ };
            ($err //= 'NO-ERROR') =~ s/\n/ /g;
            await $send->({ type => 'sse.send', data => "err=$err" });
            await $send->({ type => 'sse.close' });
            return;
        }
        if ($path eq '/sse-send-after-close') {
            await $send->({ type => 'sse.start', status => 200 });
            await $send->({ type => 'sse.send', data => 'one' });
            await $send->({ type => 'sse.close' });
            my $err = do { local $@; eval { await $send->({ type => 'sse.send', data => 'late' }) }; $@ };
            $SseAfterClose::ERR = $err;   # observed via package var after request
            return;
        }
    }
    die "unsupported scope $scope->{type}";
};

my $sse_server = PAGI::Server->new(app => $sse_app, host => '127.0.0.1', port => 0, quiet => 1, validate_events => 0);
$loop->add($sse_server);
$sse_server->listen->get;
my $sse_port = $sse_server->port;

my $sse_get = sub {
    $http->GET("http://127.0.0.1:$sse_port$_[0]", headers => { Accept => 'text/event-stream' })->get;
};

like( $sse_get->('/sse-send-before-start')->content, qr/before sse\.start/,
    'sse.send before sse.start fails the Future' );
like( $sse_get->('/sse-newline-event')->content, qr/must not contain newline/,
    'newline in sse event name fails' );
$sse_get->('/sse-send-after-close');
like( $SseAfterClose::ERR // '', qr/after sse\.close/,
    'sse.send after sse.close fails' );
$sse_server->shutdown->get;
```

For WebSocket, mirror the pattern with a raw handshake (copy the handshake helper style used in `t/04-websocket.t` — connect a raw `IO::Socket::INET`, send the upgrade request, then the app attempts `websocket.send` with both `bytes` and `text` (shape croak) and `websocket.keepalive` before accept (sequence croak `before` — note: keepalive in `connecting` croaks via the machine), recording errors in package vars asserted after the close handshake. The implementer lifts the exact client boilerplate from `t/04-websocket.t` — the assertion pattern is identical to SSE above: `like($WsErrs::SHAPE, qr/exactly one of bytes\/text/)` and `like($WsErrs::SEQ, qr/cannot send 'websocket.keepalive'/)`.

- [ ] **Step 2: Run to verify failure** — `prove -l t/52-mandatory-validation.t`, tee `/tmp/phase1-task6-fail.out`, read. Expected: new subtests FAIL (with `validate_events => 0`, nothing checks; some violations are silently ignored today).

- [ ] **Step 3: Implement** — in the h1 SSE closure (~3595): delete the two ad-hoc `die` guards (`after sse.close`, `after sse.http.response.start` — the ones immediately above the timer reset), replace the `if ($weak_self->{validate_events})` block with:

```perl
        PAGI::Server::EventValidator::validate_sse_send($event);
        $seq = PAGI::Server::EventValidator::advance_sse($seq, $event);
```

adding `my $seq = 'initial';` in the closure's enclosing scope, and delete the now-redundant `die "cannot decline with sse.http.response.start after sse.start\n"` in the h1 decline branch. Keep `$weak_self->{sse_started}` and friends for behavior. In the h1 WS closure (~3930): same pattern with `my $seq = 'connecting';`, `validate_websocket_send($event, { extensions => $weak_self->{extensions} })`, `advance_websocket`. Remove the `return if $weak_self->{websocket_accepted};` silent guard under `websocket.accept` — duplicate accept now croaks via the machine's `accepted`+accept transition (defined and unit-tested in Task 4).

- [ ] **Step 4: Run to verify pass, plus neighbors** — `prove -l t/52-mandatory-validation.t t/04-websocket.t t/05-sse.t t/33-ws-sse-idle-timeout.t t/14-websocket-invalid-utf8.t`, tee `/tmp/phase1-task6-pass.out`, read fully. Expected: PASS. Any existing test that relied on a silently-ignored duplicate event now failing is a finding to fix in the app fixture, not by weakening validation.

- [ ] **Step 5: Commit**

```bash
git add lib/PAGI/Server/Connection.pm t/52-mandatory-validation.t t/40-event-validation.t docs/superpowers/plans/2026-08-21-protocol-alignment-phase1-validation.md
git commit -m "feat: mandatory validation and sequencing on HTTP/1 SSE and WebSocket send paths"
```

---

### Task 7: HTTP/2 dispatchers

**Files:**
- Modify: `lib/PAGI/Server/Connection.pm` (`_h2_create_send` line 814; `_h2_create_websocket_send` line 1104; h2 SSE send guard ~1437)
- Create: `t/http2/26-mandatory-validation.t`

**Interfaces:**
- Consumes: the same validator/machine functions, with the h2 twist that Phase 1 does NOT implement h2 trailers/file/fh behavior (that is Phase 2). Events that pass validation but lack h2 behavior (`http.response.trailers`, `file`/`fh` bodies, gated `http.fullflush`) fail with an explicit `die "... is not yet implemented on HTTP/2\n"` stub. **Ordering is load-bearing:** the stub checks run AFTER shape validation but BEFORE `advance_http` — if the machine advanced first, the state would record an event the dispatcher never performed, stranding a conforming app in `complete`/`awaiting_trailers` with no way to finish its response. Phase 2 deletes the stubs.
- Produces: all six send paths validate unconditionally; `t/http2/26-mandatory-validation.t` is the h2 twin of `t/52`.

- [ ] **Step 1: Write the failing test** — create `t/http2/26-mandatory-validation.t`, modeled directly on the client setup in `t/http2/11-streaming.t` (same `Net::HTTP2::nghttp2` availability skip, same client boilerplate — lift it verbatim), with an app exercising: `/bad-status` (status `"50\n0"` → failed Future, app reports in valid response), `/body-before-start`, `/unknown-type`, `/duplicate-start` — assertion patterns identical to Task 5's, plus the two stub paths. Because the stubs fail BEFORE the state machine advances (see Interfaces), the app can still finish its response and carry the error to the client — no package vars, no hangs:

```perl
# /file-body app path: probe the stub, then respond normally
#   await $send->({ type => 'http.response.start', status => 200, headers => [['content-type','text/plain']] });
#   my $err = do { local $@; eval { await $send->({ type => 'http.response.body', file => '/etc/hosts' }) }; $@ };
#   $err =~ s/\n/ /g;
#   await $send->({ type => 'http.response.body', body => "file:$err", more => 0 });
like( $get->('/file-body')->content, qr/file:.*not yet implemented on HTTP\/2/,
    'file body on h2 fails loudly instead of sending an empty body' );

# /trailers app path: declare trailers, stream one chunk, probe the trailers
# stub, then finish with a terminal body (legal: the failed stub never advanced
# the machine, so state is still streaming).
#   await $send->({ type => 'http.response.start', status => 200, trailers => 1, headers => [['content-type','text/plain']] });
#   await $send->({ type => 'http.response.body', body => 'x', more => 1 });
#   my $err = do { local $@; eval { await $send->({ type => 'http.response.trailers', headers => [['x-t','1']] }) }; $@ };
#   $err =~ s/\n/ /g;
#   await $send->({ type => 'http.response.body', body => "trailers:$err", more => 0 });
like( $get->('/trailers')->content, qr/trailers:.*not yet implemented on HTTP\/2/,
    'declared trailers on h2 fail loudly at the trailers event' );
```

- [ ] **Step 2: Run to verify failure** — `prove -l t/http2/26-mandatory-validation.t`, tee `/tmp/phase1-task7-fail.out`, read. Expected: FAIL — today `/bad-status` becomes a 200 (missing-status default), `/file-body` sends an empty body, `/trailers` dies with `Unrecognized event type`.

- [ ] **Step 3: Implement** — in `_h2_create_send`'s `return async sub` (the one dispatching on `http.response.start`/`http.response.body` around line 895): after the existing closed/stream checks, in this exact order (see Interfaces for why):

```perl
        # 1. Shape validation (mandatory)
        PAGI::Server::EventValidator::validate_http_send(
            $event, { extensions => $weak_self->{extensions} });

        # 2. Phase-1 stubs: valid events whose HTTP/2 behavior lands in Phase 2.
        #    These fail BEFORE the sequence machine advances, so a conforming
        #    app that probes them can still finish its response.
        die "http.response.trailers is not yet implemented on HTTP/2\n"
            if $type eq 'http.response.trailers';
        die "http.fullflush is not yet implemented on HTTP/2\n"
            if $type eq 'http.fullflush';
        die "http.response.body 'file'/'fh' is not yet implemented on HTTP/2\n"
            if $type eq 'http.response.body'
            && (defined $event->{file} || defined $event->{fh});

        # 3. Sequence enforcement
        $seq = PAGI::Server::EventValidator::advance_http($seq, $event);
```

(declare `my $seq = 'initial';` beside `$streaming_started`; the fullflush stub is only reachable when the extension is advertised — unadvertised fullflush already croaked in validation). Remove the silent `return if $ss->{response_started};` duplicate-start guard and the silent `return unless $ss->{response_started};` body-before-start guard (the machine croaks now). Note one accepted Phase-1 semantic: on h2, a trailers event hits the stub before sequence-checking, so even an *undeclared* trailers event reports "not yet implemented" rather than "not declared" — honest either way; Phase 2 removes the stub and restores the h1-identical message. Same wiring pattern (validate → advance, no stubs needed) for `_h2_create_websocket_send` (`my $seq = 'connecting';`) and the h2 SSE closure at ~1437 (replace its `validate_events` guard with unconditional `validate_sse_send` + `advance_sse`, deleting its two ad-hoc sequence dies at ~1422/1428 and the ~1555 decline-after-start die, same as Task 6 did for h1).

- [ ] **Step 4: Run to verify pass, plus the h2 group** — `prove -l t/http2/ 2>&1 | tee /tmp/phase1-task7-pass.out`, read fully. Expected: all PASS, including the new file.

- [ ] **Step 5: Commit**

```bash
git add lib/PAGI/Server/Connection.pm t/http2/26-mandatory-validation.t docs/superpowers/plans/2026-08-21-protocol-alignment-phase1-validation.md
git commit -m "feat: mandatory validation and sequencing on all HTTP/2 send paths"
```

---

### Task 8: Lifespan send validation and phase enforcement

**Files:**
- Modify: `lib/PAGI/Server.pm` (lifespan `$send` at ~4094; the code that queues `lifespan.shutdown` — find it via `grep -n "lifespan.shutdown'" lib/PAGI/Server.pm` and set the phase there)
- Create: `t/lifespan-send-validation.t`

**Interfaces:**
- Consumes: `validate_lifespan_send`, `advance_lifespan`.
- Produces: `$self->{lifespan_phase}` (`'startup_pending'` when the lifespan exchange starts, set to `'shutdown_pending'` by the server when it queues the shutdown event). The lifespan `$send` validates + advances before its dispatch chain; a wrong-phase or unknown event fails the app's send Future and leaves server state untouched.

- [ ] **Step 1: Write the failing test** — create `t/lifespan-send-validation.t` (modeled on `t/06-lifespan.t` boilerplate: build server, `$loop->add`, `listen`, shutdown):

```perl
use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";
use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

my %seen;
my $app = async sub {
    my ($scope, $receive, $send) = @_;
    return unless $scope->{type} eq 'lifespan';
    while (1) {
        my $msg = await $receive->();
        if ($msg->{type} eq 'lifespan.startup') {
            $seen{unknown}  = do { local $@; eval { await $send->({ type => 'lifespan.startup.done' }) }; $@ };
            $seen{early_shutdown} = do { local $@; eval { await $send->({ type => 'lifespan.shutdown.complete' }) }; $@ };
            $seen{bad_message} = do { local $@; eval { await $send->({ type => 'lifespan.startup.failed', message => {} }) }; $@ };
            await $send->({ type => 'lifespan.startup.complete' });
            $seen{late_startup} = do { local $@; eval { await $send->({ type => 'lifespan.startup.complete' }) }; $@ };
        }
        elsif ($msg->{type} eq 'lifespan.shutdown') {
            await $send->({ type => 'lifespan.shutdown.complete' });
            return;
        }
    }
};

my $loop = IO::Async::Loop->new;
my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0, quiet => 1);
$loop->add($server);
$server->listen->get;
ok( $server->is_running, 'server started despite the app probing invalid lifespan sends' );
$server->shutdown->get;

like( $seen{unknown},        qr/Unrecognized event type .* for lifespan protocol/, 'unknown lifespan type failed the Future' );
like( $seen{early_shutdown}, qr/during lifespan phase 'startup_pending'/,          'shutdown result during startup failed' );
like( $seen{bad_message},    qr/'message' must be a string/,                        'ref message failed' );
like( $seen{late_startup},   qr/during lifespan phase 'running'/,                   'duplicate startup.complete failed' );

done_testing;
```

- [ ] **Step 2: Run to verify failure** — `prove -l t/lifespan-send-validation.t`, tee `/tmp/phase1-task8-fail.out`, read. Expected: FAIL — today the unknown type is silently ignored and `early_shutdown` is silently ACCEPTED (it sets `$self->{shutdown_complete} = 1` — a live bug: a mis-phased app corrupts shutdown bookkeeping), and the server may misbehave at shutdown. The test also implicitly covers that bug.

- [ ] **Step 3: Implement** — in `_run_lifespan_startup` set `$self->{lifespan_phase} = 'startup_pending';` before creating `$send`; add at the top of the `$send` async sub:

```perl
        PAGI::Server::EventValidator::validate_lifespan_send($event);
        $self->{lifespan_phase} = PAGI::Server::EventValidator::advance_lifespan(
            $self->{lifespan_phase}, $event);
```

(with `use PAGI::Server::EventValidator;` added to Server.pm's header) and at the site that queues `{ type => 'lifespan.shutdown' }` into the send queue, set `$self->{lifespan_phase} = 'shutdown_pending';` immediately before queueing.

- [ ] **Step 4: Run to verify pass, plus lifespan neighbors** — `prove -l t/lifespan-send-validation.t t/06-lifespan.t t/lifespan-mode.t t/lifespan-decline-clean-return.t t/lifespan-startup-timeout.t t/lifespan-post-startup-failure.t`, tee `/tmp/phase1-task8-pass.out`, read fully. Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add lib/PAGI/Server.pm t/lifespan-send-validation.t docs/superpowers/plans/2026-08-21-protocol-alignment-phase1-validation.md
git commit -m "feat: validate lifespan sends and enforce protocol phases"
```

---

### Task 9: Deprecate `validate_events`; documentation

**Files:**
- Modify: `lib/PAGI/Server.pm` (~2331 init and the `=item validate_events` POD at ~1573), `lib/PAGI/Server/Connection.pm:139` (constructor comment), `lib/PAGI/Server/EventValidator.pm` (POD rewrite), `Changes`, `UPGRADING.md`, `t/40-event-validation.t`, `t/25-runner-production.t` (only if it asserts validation is off in production — check with `grep -n validate t/25-runner-production.t`)
- Test: `t/40-event-validation.t`

**Interfaces:**
- Consumes: everything landed in Tasks 1–8.
- Produces: `validate_events` accepted, documented as a deprecated no-op (core validation is unconditional; the option controls nothing in this release). The `PAGI_ENV` auto-enable line remains harmless (the stored flag is read by nothing).

- [ ] **Step 1: Write the failing test** — in `t/40-event-validation.t`, replace the three source-inspection subtests (`server has validation hooks`, `PAGI::Server exposes validate_events option`, `PAGI::Server auto-enables validate_events via PAGI_ENV`) with:

```perl
subtest 'validate_events is a deprecated no-op' => sub {
    # Core validation is unconditional: t/52-mandatory-validation.t and
    # t/http2/26-mandatory-validation.t construct servers with
    # validate_events => 0 and prove malformed sends still fail.
    # Here: the option is still accepted for compatibility.
    require PAGI::Server;
    ok( lives { PAGI::Server->new(app => sub {}, quiet => 1, validate_events => 0) },
        'validate_events => 0 accepted' );
    ok( lives { PAGI::Server->new(app => sub {}, quiet => 1, validate_events => 1) },
        'validate_events => 1 accepted' );
};
```

- [ ] **Step 2: Run to verify state** — `prove -l t/40-event-validation.t`, tee `/tmp/phase1-task9-fail.out`, read. Expected: PASS already (the subtest documents, not drives, code — the *failing* part of this task is `podchecker`/docs below being stale, and any `t/25-runner-production.t` fallout).

- [ ] **Step 3: Update code comments and docs** —
  - `Server.pm` init comment becomes: `# Deprecated: core event validation is mandatory; this flag is retained for compatibility and controls nothing.`
  - `Server.pm` POD `=item validate_events => $bool`: rewrite to state validation is always on per the PAGI spec, the option is deprecated and ignored, and will be removed in a future release.
  - `EventValidator.pm`: rewrite the header comment and POD — it is the mandatory shared validator; document `validate_http_send`, `validate_websocket_send`, `validate_sse_send`, `validate_lifespan_send`, `validate_headers`, `check_header_name`, `check_header_value`, `advance_http`, `advance_websocket`, `advance_sse`, `advance_lifespan` (each with a one-paragraph description, states, and croak behavior).
  - `Changes` (under the next unreleased version):

```
    - PAGI outgoing-event validation is now mandatory on every send path
      (HTTP/1, HTTP/2, WebSocket, SSE, lifespan): malformed, mis-sequenced,
      unknown, and unadvertised-extension events fail the $send Future in
      all environments. validate_events is deprecated and ignored.
    - Send sequencing is enforced per scope: events before response start,
      duplicate starts, bodies after completion, undeclared trailers, and
      wrong-phase lifespan results now fail instead of being silently
      ignored or defaulted.
```

  - `UPGRADING.md`: add a section explaining that applications sending malformed or out-of-sequence events now receive failed `$send` Futures even in production, with the common cases (missing `status`, misspelled types, `more` values other than 0/1) and the fix (send conforming events; the failures name the exact field).

- [ ] **Step 4: Verify** — `bash -c '... && podchecker lib/PAGI/Server.pm lib/PAGI/Server/EventValidator.pm lib/PAGI/Server/Connection.pm && prove -l t/40-event-validation.t t/25-runner-production.t 2>&1 | tee /tmp/phase1-task9-pass.out'`, read the file. Expected: POD OK, tests PASS.

- [ ] **Step 5: Commit**

```bash
git add lib/PAGI/Server.pm lib/PAGI/Server/Connection.pm lib/PAGI/Server/EventValidator.pm Changes UPGRADING.md t/40-event-validation.t t/25-runner-production.t docs/superpowers/plans/2026-08-21-protocol-alignment-phase1-validation.md
git commit -m "docs: deprecate validate_events; document mandatory validation"
```

---

### Task 10: Phase gate — full verification

**Files:**
- Modify: this plan's tracking table; `docs/superpowers/specs/2026-08-21-pagi-server-protocol-alignment-design.md` only if a drift/deviation was recorded.

- [ ] **Step 1: Full suite** — `bash -c 'source ~/perl5/perlbrew/etc/bashrc && perlbrew use perl-5.42.2@default && prove -lr t/ 2>&1 | tee /tmp/phase1-full-suite.out'`; read the END of the file for the summary AND grep it for `^Result:` and any `Failed` — never judge from the terminal scrollback.
- [ ] **Step 2: Hygiene** — `git diff main --check` (whitespace), `podchecker` over every touched module, and a syntax check of each touched module under the project Perl (`perl -c -Ilib lib/PAGI/Server.pm lib/PAGI/Server/Connection.pm lib/PAGI/Server/EventValidator.pm`).
- [ ] **Step 3: Spec drift check** — re-read PAGI spec `main` @ HEAD; confirm still `a7ed9cc` or record drift in the design doc per its section 2.2.
- [ ] **Step 4: Tracking table final update + commit** — every row must show its real commit SHA and test counts from the tee'd files.

```bash
git add docs/superpowers/plans/2026-08-21-protocol-alignment-phase1-validation.md
git commit -m "docs: phase 1 verification gate results"
```

---

## Tracking

Update the row in the same commit as the task (Global Constraints). "Tests" = real counts from the tee'd prove output, not estimates.

| Task | Status | Commit | Tests (added/passing) | Verification evidence |
|------|--------|--------|-----------------------|-----------------------|
| 1. Primitive shapes | not started | — | — | — |
| 2. Shared headers | not started | — | — | — |
| 3. Registries + gating | not started | — | — | — |
| 4. Sequence machines | not started | — | — | — |
| 5. h1 HTTP wiring | not started | — | — | — |
| 6. h1 SSE/WS wiring | not started | — | — | — |
| 7. h2 wiring | not started | — | — | — |
| 8. Lifespan | not started | — | — | — |
| 9. Deprecation + docs | not started | — | — | — |
| 10. Phase gate | not started | — | — | — |

## Deviations

None recorded. A deviation from this plan gets an ID (D1, D2, …), a rationale, and John's sign-off here BEFORE work builds on it.
