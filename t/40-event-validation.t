#!/usr/bin/env perl

# =============================================================================
# Test: PAGI::Server::EventValidator - Mandatory event validation
#
# Per main.mkdn: Servers must raise exceptions if events are missing required
# fields or event fields are of the wrong type.
# =============================================================================

use strict;
use warnings;
use Test2::V0;

use lib 'lib';
require PAGI::Server::EventValidator;

# =============================================================================
# HTTP Event Validation
# =============================================================================

subtest 'http.response.start validation' => sub {
    # Missing status should die
    like(
        dies { PAGI::Server::EventValidator::validate_http_send({ type => 'http.response.start' }) },
        qr/requires 'status'/,
        'missing status throws'
    );

    # Non-integer status should die
    like(
        dies { PAGI::Server::EventValidator::validate_http_send({ type => 'http.response.start', status => 'ok' }) },
        qr/must be a non-negative integer/,
        'non-integer status throws'
    );

    # Undef status should die
    like(
        dies { PAGI::Server::EventValidator::validate_http_send({ type => 'http.response.start', status => undef }) },
        qr/must be a non-negative integer/,
        'undef status throws'
    );

    # Invalid headers type should die
    like(
        dies { PAGI::Server::EventValidator::validate_http_send({ type => 'http.response.start', status => 200, headers => 'bad' }) },
        qr/must be an array reference/,
        'non-array headers throws'
    );

    # Valid events should pass
    ok(
        lives { PAGI::Server::EventValidator::validate_http_send({ type => 'http.response.start', status => 200 }) },
        'valid event with status only'
    );

    ok(
        lives { PAGI::Server::EventValidator::validate_http_send({ type => 'http.response.start', status => 200, headers => [] }) },
        'valid event with empty headers'
    );

    ok(
        lives { PAGI::Server::EventValidator::validate_http_send({ type => 'http.response.start', status => 404, headers => [['content-type', 'text/plain']] }) },
        'valid event with headers'
    );
};

subtest 'http.response.body validation' => sub {
    # Multiple body sources should die
    like(
        dies { PAGI::Server::EventValidator::validate_http_send({ type => 'http.response.body', body => 'x', file => '/tmp/x' }) },
        qr/exactly one of body\/file\/fh/,
        'body and file throws'
    );

    like(
        dies { PAGI::Server::EventValidator::validate_http_send({ type => 'http.response.body', body => 'x', fh => \*STDOUT }) },
        qr/exactly one of body\/file\/fh/,
        'body and fh throws'
    );

    # Invalid offset should die
    like(
        dies { PAGI::Server::EventValidator::validate_http_send({ type => 'http.response.body', body => 'x', offset => 'bad' }) },
        qr/'offset' must be a non-negative integer/,
        'non-integer offset throws'
    );

    # Invalid length should die
    like(
        dies { PAGI::Server::EventValidator::validate_http_send({ type => 'http.response.body', body => 'x', length => 'bad' }) },
        qr/'length' must be a non-negative integer/,
        'non-integer length throws'
    );

    # Valid events should pass
    ok(
        lives { PAGI::Server::EventValidator::validate_http_send({ type => 'http.response.body' }) },
        'empty body is valid (defaults to empty string)'
    );

    ok(
        lives { PAGI::Server::EventValidator::validate_http_send({ type => 'http.response.body', body => 'hello' }) },
        'body string is valid'
    );

    ok(
        lives { PAGI::Server::EventValidator::validate_http_send({ type => 'http.response.body', file => '/tmp/x' }) },
        'file path is valid'
    );
};

subtest 'http.response.trailers validation' => sub {
    # Invalid headers type should die
    like(
        dies { PAGI::Server::EventValidator::validate_http_send({ type => 'http.response.trailers', headers => 'bad' }) },
        qr/must be an array reference/,
        'non-array headers throws'
    );

    # Valid events should pass
    ok(
        lives { PAGI::Server::EventValidator::validate_http_send({ type => 'http.response.trailers' }) },
        'no headers is valid'
    );

    ok(
        lives { PAGI::Server::EventValidator::validate_http_send({ type => 'http.response.trailers', headers => [] }) },
        'empty headers is valid'
    );
};

# =============================================================================
# WebSocket Event Validation
# =============================================================================

subtest 'websocket.send validation' => sub {
    # Neither bytes nor text should die
    like(
        dies { PAGI::Server::EventValidator::validate_websocket_send({ type => 'websocket.send' }) },
        qr/exactly one of bytes\/text/,
        'missing both throws'
    );

    # Both bytes and text should die
    like(
        dies { PAGI::Server::EventValidator::validate_websocket_send({ type => 'websocket.send', bytes => 'x', text => 'y' }) },
        qr/exactly one of bytes\/text/,
        'both present throws'
    );

    # Valid events should pass
    ok(
        lives { PAGI::Server::EventValidator::validate_websocket_send({ type => 'websocket.send', bytes => 'binary' }) },
        'bytes only is valid'
    );

    ok(
        lives { PAGI::Server::EventValidator::validate_websocket_send({ type => 'websocket.send', text => 'hello' }) },
        'text only is valid'
    );
};

subtest 'websocket.close validation' => sub {
    # Invalid code should die
    like(
        dies { PAGI::Server::EventValidator::validate_websocket_send({ type => 'websocket.close', code => 'bad' }) },
        qr/'code' must be a non-negative integer/,
        'non-integer code throws'
    );

    # Valid events should pass
    ok(
        lives { PAGI::Server::EventValidator::validate_websocket_send({ type => 'websocket.close' }) },
        'no code is valid (uses default)'
    );

    ok(
        lives { PAGI::Server::EventValidator::validate_websocket_send({ type => 'websocket.close', code => 1000 }) },
        'integer code is valid'
    );
};

subtest 'websocket.keepalive validation' => sub {
    # Missing interval should die
    like(
        dies { PAGI::Server::EventValidator::validate_websocket_send({ type => 'websocket.keepalive' }) },
        qr/requires 'interval'/,
        'missing interval throws'
    );

    # Invalid interval should die
    like(
        dies { PAGI::Server::EventValidator::validate_websocket_send({ type => 'websocket.keepalive', interval => 'bad' }) },
        qr/'interval' must be a non-negative number/,
        'non-number interval throws'
    );

    # Valid events should pass
    ok(
        lives { PAGI::Server::EventValidator::validate_websocket_send({ type => 'websocket.keepalive', interval => 30 }) },
        'integer interval is valid'
    );

    ok(
        lives { PAGI::Server::EventValidator::validate_websocket_send({ type => 'websocket.keepalive', interval => 30.5 }) },
        'float interval is valid'
    );
};

# =============================================================================
# SSE Event Validation
# =============================================================================

subtest 'sse.send validation' => sub {
    # Missing data should die
    like(
        dies { PAGI::Server::EventValidator::validate_sse_send({ type => 'sse.send' }) },
        qr/requires 'data'/,
        'missing data throws'
    );

    # Non-string data should die
    like(
        dies { PAGI::Server::EventValidator::validate_sse_send({ type => 'sse.send', data => [] }) },
        qr/'data' must be a string/,
        'array data throws'
    );

    # Valid events should pass
    ok(
        lives { PAGI::Server::EventValidator::validate_sse_send({ type => 'sse.send', data => 'hello' }) },
        'string data is valid'
    );

    ok(
        lives { PAGI::Server::EventValidator::validate_sse_send({ type => 'sse.send', data => '' }) },
        'empty string data is valid'
    );
};

subtest 'sse.comment validation' => sub {
    # Missing comment should die
    like(
        dies { PAGI::Server::EventValidator::validate_sse_send({ type => 'sse.comment' }) },
        qr/requires 'comment'/,
        'missing comment throws'
    );

    # Non-string comment should die
    like(
        dies { PAGI::Server::EventValidator::validate_sse_send({ type => 'sse.comment', comment => {} }) },
        qr/'comment' must be a string/,
        'hashref comment throws'
    );

    # Valid events should pass
    ok(
        lives { PAGI::Server::EventValidator::validate_sse_send({ type => 'sse.comment', comment => 'keepalive' }) },
        'string comment is valid'
    );
};

subtest 'sse.keepalive validation' => sub {
    # Missing interval should die
    like(
        dies { PAGI::Server::EventValidator::validate_sse_send({ type => 'sse.keepalive' }) },
        qr/requires 'interval'/,
        'missing interval throws'
    );

    # Invalid interval should die
    like(
        dies { PAGI::Server::EventValidator::validate_sse_send({ type => 'sse.keepalive', interval => 'x' }) },
        qr/'interval' must be a non-negative number/,
        'non-number interval throws'
    );

    # Valid events should pass
    ok(
        lives { PAGI::Server::EventValidator::validate_sse_send({ type => 'sse.keepalive', interval => 15 }) },
        'integer interval is valid'
    );
};

subtest 'sse.keepalive comment validation' => sub {
    # An unpaired UTF-16 surrogate has no UTF-8 representation -- must be
    # rejected at arm time, not later inside the timer tick.
    like(
        dies { PAGI::Server::EventValidator::validate_sse_send({ type => 'sse.keepalive', interval => 15, comment => "\x{D800}" }) },
        qr/sse\.keepalive 'comment' must be a UTF-8-encodable string/,
        'unencodable surrogate comment throws'
    );

    # A reference is never a string.
    like(
        dies { PAGI::Server::EventValidator::validate_sse_send({ type => 'sse.keepalive', interval => 15, comment => {} }) },
        qr/sse\.keepalive 'comment' must be a UTF-8-encodable string/,
        'ref comment throws'
    );

    # A valid non-ASCII comment round-trips fine.
    ok(
        lives { PAGI::Server::EventValidator::validate_sse_send({ type => 'sse.keepalive', interval => 15, comment => 'håndtering' }) },
        'UTF-8-encodable non-ASCII comment is valid'
    );

    # comment is optional; absent stays valid (defaults to '').
    ok(
        lives { PAGI::Server::EventValidator::validate_sse_send({ type => 'sse.keepalive', interval => 15 }) },
        'absent comment is valid'
    );
};

# =============================================================================
# Server Integration Test (behavioral)
# =============================================================================

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

# =============================================================================
# Strict primitive shape checks
# =============================================================================

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

    # Shared across families: a refusal on either protocol scope runs the
    # same http checks
    like( dies { PAGI::Server::EventValidator::validate_websocket_send({ type => 'http.response.start', status => 401, headers => [ ['h',"v\n"] ] }) },
        qr/contains CR, LF, or null byte/, 'ws refusal headers validated');
    like( dies { PAGI::Server::EventValidator::validate_sse_send({ type => 'http.response.start', status => 404, headers => [ ['h',"v\0"] ] }) },
        qr/contains CR, LF, or null byte/, 'sse refusal headers validated');
};

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
    # A refusal is an ordinary HTTP response on the scope, not an extension:
    # the protocol-prefixed event types are gone and nothing gates the real
    # ones (Www.pod "Refusing the handshake": "there is nothing to advertise
    # or detect").
    like( dies { PAGI::Server::EventValidator::validate_websocket_send({ type => 'websocket.http.response.start', status => 401 }) },
        qr/Unrecognized event type 'websocket\.http\.response\.start' for websocket protocol/,
        'the protocol-prefixed refusal start is not an event type');
    like( dies { PAGI::Server::EventValidator::validate_websocket_send({ type => 'websocket.http.response.start', status => 401 }, { extensions => { 'websocket.http.response' => {} } }) },
        qr/Unrecognized event type 'websocket\.http\.response\.start' for websocket protocol/,
        'and no extension brings it back');
    like( dies { PAGI::Server::EventValidator::validate_sse_send({ type => 'sse.http.response.start', status => 404 }) },
        qr/Unrecognized event type 'sse\.http\.response\.start' for sse protocol/,
        'the sse twin is gone too');
    like( dies { PAGI::Server::EventValidator::validate_sse_send({ type => 'http.fullflush' }) },
        qr/Extension not enabled: fullflush/, 'sse fullflush without extension throws');
    ok( lives { PAGI::Server::EventValidator::validate_sse_send({ type => 'http.fullflush' }, { extensions => { fullflush => {} } }) },
        'sse fullflush with extension ok');
};

subtest 'HTTP response events are valid on websocket and sse scopes before accept/start' => sub {
    for my $ev ({ type => 'http.response.start', status => 401, headers => [] },
                { type => 'http.response.body', body => 'x', more => 0 },
                { type => 'http.response.body', file => '/etc/hosts' },
                { type => 'http.response.trailers', headers => [['x-t', '1']] }) {
        ok(lives { PAGI::Server::EventValidator::validate_websocket_send($ev, {}) }, "websocket accepts $ev->{type}");
        ok(lives { PAGI::Server::EventValidator::validate_sse_send($ev, {}) },       "sse accepts $ev->{type}");
    }
    # The rules are the http ones, not a parallel set.
    like(dies { PAGI::Server::EventValidator::validate_websocket_send({ type => 'http.response.start' }, {}) },
        qr/http\.response\.start requires 'status'/, 'HTTP validation rules apply on websocket');
    like(dies { PAGI::Server::EventValidator::validate_sse_send({ type => 'http.response.start' }, {}) },
        qr/http\.response\.start requires 'status'/, 'HTTP validation rules apply on sse');
    like(dies { PAGI::Server::EventValidator::validate_websocket_send({ type => 'http.response.start', status => 401, headers => [['bad name', "v\r\n"]] }, {}) },
        qr/Invalid header value/, 'header byte safety applies on websocket');
};

subtest 'a WebSocket refusal status must be 300 or above; SSE refusals may use any status' => sub {
    for my $status (101, 200, 204) {
        like(dies { PAGI::Server::EventValidator::validate_websocket_send({ type => 'http.response.start', status => $status, headers => [] }, {}) },
            qr/WebSocket refusal status must be 300 or above \(got $status\)/, "websocket refuses $status");
        ok(lives { PAGI::Server::EventValidator::validate_sse_send({ type => 'http.response.start', status => $status, headers => [] }, {}) },
            "sse accepts $status");
    }
    ok(lives { PAGI::Server::EventValidator::validate_websocket_send({ type => 'http.response.start', status => 300, headers => [] }, {}) }, 'websocket accepts 300');
    ok(lives { PAGI::Server::EventValidator::validate_websocket_send({ type => 'http.response.start', status => 401, headers => [] }, {}) }, 'websocket accepts 401');
};

subtest 'lifespan send validation' => sub {
    ok( lives { PAGI::Server::EventValidator::validate_lifespan_send({ type => 'lifespan.startup.complete' }) }, 'startup.complete ok');
    ok( lives { PAGI::Server::EventValidator::validate_lifespan_send({ type => 'lifespan.startup.failed', message => 'db down' }) }, 'startup.failed with message ok');
    like( dies { PAGI::Server::EventValidator::validate_lifespan_send({ type => 'lifespan.startup.done' }) },
        qr/Unrecognized event type .* for lifespan protocol/, 'unknown lifespan type throws');
    like( dies { PAGI::Server::EventValidator::validate_lifespan_send({ type => 'lifespan.shutdown.failed', message => {} }) },
        qr/'message' must be a string/, 'ref message throws');
};

# =============================================================================
# Sequence State Machines
# =============================================================================

subtest 'advance_http transition matrix' => sub {
    my $adv = \&PAGI::Server::EventValidator::advance_http;
    is( $adv->('initial', { type => 'http.response.start', status => 200 }), 'started', 'start -> started');
    is( $adv->('initial', { type => 'http.response.start', status => 200, trailers => 1 }), 'started_t', 'start+trailers -> started_t');
    is( $adv->('started', { type => 'http.response.body', body => 'x', more => 1 }), 'started_i', 'streaming chunk marks inline bytes delivered');
    is( $adv->('started', { type => 'http.response.body', body => 'x' }), 'complete', 'terminal body -> complete');
    is( $adv->('started', { type => 'http.response.body', file => '/tmp/f' }), 'complete', 'file body -> complete');
    is( $adv->('started', { type => 'http.response.body', fh => \*STDOUT, more => 1 }), 'complete', 'fh body is always terminal regardless of more');
    is( $adv->('started_t', { type => 'http.response.body', body => 'x', more => 1 }), 'started_t_i', 'streaming chunk marks inline bytes delivered, trailers still declared');
    is( $adv->('started_t', { type => 'http.response.body', body => 'x', more => 0 }), 'awaiting_trailers', 'terminal body with declared trailers -> awaiting_trailers');
    is( $adv->('awaiting_trailers', { type => 'http.response.trailers', headers => [] }), 'complete', 'trailers -> complete');
    is( $adv->('started', { type => 'http.fullflush' }), 'started', 'fullflush leaves started unchanged');
    is( $adv->('started_t', { type => 'http.fullflush' }), 'started_t', 'fullflush leaves started_t unchanged');
    is( $adv->('awaiting_trailers', { type => 'http.fullflush' }), 'awaiting_trailers', 'fullflush leaves awaiting_trailers unchanged');
    like( dies { $adv->('initial', { type => 'http.response.body', body => 'x' }) }, qr/before http\.response\.start/, 'body before start');
    like( dies { $adv->('initial', { type => 'http.response.trailers' }) }, qr/before http\.response\.start/, 'trailers before start');
    like( dies { $adv->('started', { type => 'http.response.start', status => 200 }) }, qr/duplicate http\.response\.start/, 'duplicate start');
    like( dies { $adv->('started_t', { type => 'http.response.start', status => 200 }) }, qr/duplicate http\.response\.start/, 'duplicate start after trailers declared');
    like( dies { $adv->('started', { type => 'http.response.trailers' }) }, qr/not declared/, 'undeclared trailers');
    like( dies { $adv->('started_t', { type => 'http.response.trailers' }) }, qr/not declared/, 'trailers before body complete');
    like( dies { $adv->('complete', { type => 'http.response.body', body => 'x' }) }, qr/already complete/, 'body after completion');
    like( dies { $adv->('complete', { type => 'http.response.trailers' }) }, qr/already complete/, 'trailers after completion');
    like( dies { $adv->('complete', { type => 'http.response.start', status => 200 }) }, qr/already complete/, 'start after completion');
    like( dies { $adv->('awaiting_trailers', { type => 'http.response.body', body => 'x' }) }, qr/awaiting_trailers/, 'body while awaiting trailers is rejected');
};

subtest 'a response body is inline events or one opaque event, never both' => sub {
    # PAGI::Spec::Www, "Payload kinds do not mix within a response": an
    # application MUST NOT send a file/fh event once inline body bytes have
    # been delivered, and the server MUST fail such a send. A compressing
    # intermediary commits to an encoding before it can know a delegated
    # payload will follow, and cannot compress bytes the server streams on
    # the application's behalf.
    my $adv = \&PAGI::Server::EventValidator::advance_http;

    # Delivering an inline chunk is what closes the door.
    is( $adv->('started', { type => 'http.response.body', body => 'x', more => 1 }),
        'started_i', 'an inline chunk records that inline bytes were delivered');
    is( $adv->('started_t', { type => 'http.response.body', body => 'x', more => 1 }),
        'started_t_i', 'likewise when trailers were declared');

    like( dies { $adv->('started_i', { type => 'http.response.body', file => '/tmp/f' }) },
        qr/after inline body bytes/, 'file after an inline chunk is rejected');
    like( dies { $adv->('started_i', { type => 'http.response.body', fh => \*STDOUT }) },
        qr/after inline body bytes/, 'fh after an inline chunk is rejected');
    like( dies { $adv->('started_t_i', { type => 'http.response.body', file => '/tmp/f' }) },
        qr/after inline body bytes/, 'and with trailers declared');

    # Everything legal still advances exactly as before.
    is( $adv->('started', { type => 'http.response.body', file => '/tmp/f' }), 'complete',
        'a file event alone is still the whole body');
    is( $adv->('started_i', { type => 'http.response.body', body => 'x', more => 1 }),
        'started_i', 'further inline chunks are fine');
    is( $adv->('started_i', { type => 'http.response.body', body => 'x', more => 0 }),
        'complete', 'and terminate normally');
    is( $adv->('started_t_i', { type => 'http.response.body', body => 'x', more => 0 }),
        'awaiting_trailers', 'reaching the trailers phase as before');
    is( $adv->('started_i', { type => 'http.fullflush' }), 'started_i',
        'fullflush leaves the inline marker alone');
};

subtest 'advance_sse close is idempotent, streams stay exclusive' => sub {
    my $adv = \&PAGI::Server::EventValidator::advance_sse;
    is( $adv->('initial', { type => 'sse.start' }), 'streaming', 'start -> streaming');
    is( $adv->('streaming', { type => 'sse.send', data => 'x' }), 'streaming', 'send keeps streaming');
    is( $adv->('streaming', { type => 'sse.comment', comment => 'x' }), 'streaming', 'comment keeps streaming');
    is( $adv->('streaming', { type => 'sse.keepalive', interval => 15 }), 'streaming', 'keepalive keeps streaming');
    is( $adv->('streaming', { type => 'http.fullflush' }), 'streaming', 'fullflush leaves streaming unchanged');
    like( dies { $adv->('initial', { type => 'http.fullflush' }) }, qr/before sse\.start/, 'fullflush before start');
    is( $adv->('streaming', { type => 'sse.close' }), 'closed', 'close -> closed');
    is( $adv->('closed', { type => 'sse.close' }), 'closed', 'second close idempotent');
    like( dies { $adv->('closed', { type => 'sse.send', data => 'x' }) }, qr/after sse\.close/, 'send after close');
    like( dies { $adv->('initial', { type => 'sse.send', data => 'x' }) }, qr/before sse\.start/, 'send before start');
    like( dies { $adv->('streaming', { type => 'sse.start' }) }, qr/duplicate sse\.start/, 'duplicate start');
};

subtest 'advance_sse: refusal states' => sub {
    my $adv = \&PAGI::Server::EventValidator::advance_sse;
    is($adv->('initial', { type => 'http.response.start', status => 404 }), 'refusing', 'start refuses');
    is($adv->('refusing', { type => 'http.response.body', more => 1 }), 'refusing', 'more keeps refusing');
    is($adv->('refusing', { type => 'http.response.body' }), 'refusal_complete', 'terminal');
    is($adv->('refusing', { type => 'http.response.body', file => '/etc/hosts' }), 'refusal_complete', 'a file body is terminal');
    is($adv->('refusing', { type => 'http.response.trailers' }), 'refusal_complete', 'trailers terminate');
    like(dies { $adv->('streaming', { type => 'http.response.start', status => 404 }) }, qr/after sse\.start/, 'refusal after start');
    like(dies { $adv->('streaming', { type => 'http.response.body', body => 'x' }) }, qr/after sse\.start/, 'refusal body while streaming');
    like(dies { $adv->('refusing', { type => 'sse.send', data => 'x' }) }, qr/after http\.response\.start/, 'stream events after refusal start');
    like(dies { $adv->('refusing', { type => 'sse.start' }) }, qr/after http\.response\.start/, 'sse.start after refusal start');
    like(dies { $adv->('refusal_complete', { type => 'sse.close' }) }, qr/refusal already complete/, 'anything after refusal complete');
    like(dies { $adv->('refusal_complete', { type => 'http.response.body' }) }, qr/refusal already complete/, 'another body after refusal complete');
};

subtest 'advance_websocket: refusal states and websocket.close before accept fails' => sub {
    my $adv = \&PAGI::Server::EventValidator::advance_websocket;
    is( $adv->('connecting', { type => 'websocket.accept' }), 'accepted', 'accept');
    is( $adv->('accepted', { type => 'websocket.send', text => 'x' }), 'accepted', 'send keeps accepted');
    is( $adv->('accepted', { type => 'websocket.keepalive', interval => 30 }), 'accepted', 'keepalive keeps accepted');
    is( $adv->('accepted', { type => 'websocket.close' }), 'closed', 'close after accept');
    is($adv->('connecting', { type => 'http.response.start', status => 401 }), 'refusing', 'start refuses');
    is($adv->('refusing', { type => 'http.response.body', more => 1 }), 'refusing', 'more keeps refusing');
    is($adv->('refusing', { type => 'http.response.body' }), 'refusal_complete', 'terminal completes');
    is($adv->('refusing', { type => 'http.response.body', file => '/etc/hosts' }), 'refusal_complete', 'a file body is terminal');
    is($adv->('refusing', { type => 'http.response.trailers' }), 'refusal_complete', 'trailers terminate');
    like(dies { $adv->('connecting', { type => 'websocket.close' }) }, qr/before websocket\.accept/, 'close before accept is out of sequence');
    like(dies { $adv->('refusing', { type => 'websocket.accept' }) }, qr/after http\.response\.start/, 'accept after refusal start');
    like(dies { $adv->('accepted', { type => 'http.response.start', status => 401 }) }, qr/after websocket\.accept/, 'HTTP events after accept');
    like(dies { $adv->('accepted', { type => 'http.response.body' }) }, qr/after websocket\.accept/, 'HTTP body after accept');
    like(dies { $adv->('refusal_complete', { type => 'http.response.body' }) }, qr/refusal already complete/, 'nothing after completion');
    like( dies { $adv->('connecting', { type => 'websocket.keepalive', interval => 30 }) }, qr/before websocket\.accept/, 'keepalive before accept');
    like( dies { $adv->('connecting', { type => 'websocket.send', text => 'x' }) }, qr/before websocket\.accept/, 'send before accept');
    like( dies { $adv->('accepted', { type => 'websocket.accept' }) }, qr/after websocket\.accept/, 'duplicate accept');
    like( dies { $adv->('refusing', { type => 'websocket.send', text => 'x' }) }, qr/after http\.response\.start/, 'frame while refusing');
    like( dies { $adv->('refusing', { type => 'websocket.keepalive', interval => 30 }) }, qr/after http\.response\.start/, 'keepalive while refusing');
    like( dies { $adv->('closed', { type => 'websocket.send', text => 'x' }) }, qr/after websocket\.close/, 'send after close');
    like( dies { $adv->('closed', { type => 'websocket.close' }) }, qr/after websocket\.close/, 'second close also croaks (websocket close is not idempotent)');
};

subtest 'advance_lifespan phases' => sub {
    my $adv = \&PAGI::Server::EventValidator::advance_lifespan;
    is( $adv->('startup_pending', { type => 'lifespan.startup.complete' }), 'running', 'startup completes');
    is( $adv->('startup_pending', { type => 'lifespan.startup.failed' }), 'finished', 'startup fails');
    is( $adv->('shutdown_pending', { type => 'lifespan.shutdown.complete' }), 'finished', 'shutdown completes');
    is( $adv->('shutdown_pending', { type => 'lifespan.shutdown.failed' }), 'finished', 'shutdown fails');
    like( dies { $adv->('startup_pending', { type => 'lifespan.shutdown.complete' }) }, qr/during lifespan phase 'startup_pending'/, 'shutdown result during startup');
    like( dies { $adv->('running', { type => 'lifespan.startup.complete' }) }, qr/during lifespan phase 'running'/, 'late startup result');
    like( dies { $adv->('finished', { type => 'lifespan.startup.complete' }) }, qr/during lifespan phase 'finished'/, 'anything after finished');
};

subtest 'scope_started and scope_send_clean read the validator state' => sub {
    my $started = \&PAGI::Server::EventValidator::scope_started;
    my $clean   = \&PAGI::Server::EventValidator::scope_send_clean;
    # websocket
    ok(!$started->('websocket', 'connecting'), 'ws connecting: not started');
    ok( $started->('websocket', $_), "ws $_: started") for qw(accepted refusing refusal_complete closed);
    ok(!$clean->('websocket', $_),   "ws $_: not a clean send-side end") for qw(connecting accepted refusing);
    ok( $clean->('websocket', $_),   "ws $_: clean send-side end") for qw(refusal_complete closed);
    # sse
    ok(!$started->('sse', 'initial'), 'sse initial: not started');
    ok( $started->('sse', $_), "sse $_: started") for qw(streaming refusing refusal_complete closed);
    ok(!$clean->('sse', $_),   "sse $_: not a clean send-side end") for qw(initial streaming refusing);
    ok( $clean->('sse', $_),   "sse $_: clean send-side end") for qw(refusal_complete closed);
    # http (delegates to the existing terminal notion)
    ok(!$started->('http', 'initial'), 'http initial: not started');
    ok( $started->('http', $_), "http $_: started")
        for qw(started started_t started_i started_t_i awaiting_trailers complete);
    ok( $clean->('http', 'complete'), 'http complete: clean');
    ok(!$clean->('http', $_), "http $_: not a clean send-side end")
        for qw(initial started started_t started_i started_t_i awaiting_trailers);
    # every state advance_* can return is classified
    ok(!$started->('websocket', undef), 'undef state: not started');
    ok(!$clean->('sse', undef), 'undef state: not clean');
    like(dies { $started->('bogus', 'x') }, qr/unknown scope kind/, 'unknown kind dies');
    like(dies { $clean->('bogus', 'x') }, qr/unknown scope kind/, 'unknown kind dies for send_clean');
};

done_testing;
