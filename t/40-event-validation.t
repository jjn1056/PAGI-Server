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

    # Shared across families: ws denial start and sse decline start use the same checks
    like( dies { PAGI::Server::EventValidator::validate_websocket_send({ type => 'websocket.http.response.start', status => 401, headers => [ ['h',"v\n"] ] }, { extensions => { 'websocket.http.response' => {} } }) },
        qr/contains CR, LF, or null byte/, 'ws denial headers validated');
    like( dies { PAGI::Server::EventValidator::validate_sse_send({ type => 'sse.http.response.start', status => 404, headers => [ ['h',"v\0"] ] }) },
        qr/contains CR, LF, or null byte/, 'sse decline headers validated');
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
    like( dies { PAGI::Server::EventValidator::validate_websocket_send({ type => 'websocket.http.response.start', status => 401 }) },
        qr/Extension not enabled: websocket\.http\.response/, 'ws denial without extension throws');
    ok( lives { PAGI::Server::EventValidator::validate_websocket_send({ type => 'websocket.http.response.start', status => 401 }, { extensions => { 'websocket.http.response' => {} } }) },
        'ws denial with extension ok');
    like( dies { PAGI::Server::EventValidator::validate_sse_send({ type => 'http.fullflush' }) },
        qr/Extension not enabled: fullflush/, 'sse fullflush without extension throws');
    ok( lives { PAGI::Server::EventValidator::validate_sse_send({ type => 'http.fullflush' }, { extensions => { fullflush => {} } }) },
        'sse fullflush with extension ok');
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
    is( $adv->('started', { type => 'http.response.body', body => 'x', more => 1 }), 'started', 'streaming chunk keeps started');
    is( $adv->('started', { type => 'http.response.body', body => 'x' }), 'complete', 'terminal body -> complete');
    is( $adv->('started', { type => 'http.response.body', file => '/tmp/f' }), 'complete', 'file body -> complete');
    is( $adv->('started', { type => 'http.response.body', fh => \*STDOUT, more => 1 }), 'complete', 'fh body is always terminal regardless of more');
    is( $adv->('started_t', { type => 'http.response.body', body => 'x', more => 1 }), 'started_t', 'streaming chunk keeps started_t');
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
    is( $adv->('declining', { type => 'sse.http.response.body', more => 1 }), 'declining', 'decline body chunk keeps declining');
    is( $adv->('declining', { type => 'sse.http.response.body' }), 'decline_complete', 'terminal decline body -> decline_complete');
    like( dies { $adv->('closed', { type => 'sse.send', data => 'x' }) }, qr/after sse\.close/, 'send after close');
    is( $adv->('initial', { type => 'sse.http.response.start', status => 404 }), 'declining', 'decline start');
    like( dies { $adv->('initial', { type => 'sse.send', data => 'x' }) }, qr/before sse\.start/, 'send before start');
    like( dies { $adv->('streaming', { type => 'sse.http.response.start', status => 404 }) }, qr/after sse\.start/, 'decline after start');
    like( dies { $adv->('streaming', { type => 'sse.start' }) }, qr/duplicate sse\.start/, 'duplicate start');
    like( dies { $adv->('streaming', { type => 'sse.http.response.body', body => 'x' }) }, qr/after sse\.start/, 'decline body while streaming croaks');
    like( dies { $adv->('declining', { type => 'sse.send', data => 'x' }) }, qr/after sse\.http\.response\.start/, 'stream event while declining');
    like( dies { $adv->('decline_complete', { type => 'sse.close' }) }, qr/decline response already complete/, 'anything after decline complete');
};

subtest 'advance_websocket denial and accept are exclusive' => sub {
    my $adv = \&PAGI::Server::EventValidator::advance_websocket;
    is( $adv->('connecting', { type => 'websocket.accept' }), 'accepted', 'accept');
    is( $adv->('connecting', { type => 'websocket.http.response.start', status => 401 }), 'denial', 'denial start');
    is( $adv->('connecting', { type => 'websocket.close' }), 'closed', 'close while connecting');
    is( $adv->('accepted', { type => 'websocket.send', text => 'x' }), 'accepted', 'send keeps accepted');
    is( $adv->('accepted', { type => 'websocket.keepalive', interval => 30 }), 'accepted', 'keepalive keeps accepted');
    is( $adv->('accepted', { type => 'websocket.close' }), 'closed', 'close after accept');
    is( $adv->('denial', { type => 'websocket.http.response.body', more => 1 }), 'denial', 'denial body chunk keeps denial');
    is( $adv->('denial', { type => 'websocket.http.response.body' }), 'denial_complete', 'terminal denial body -> denial_complete');
    like( dies { $adv->('connecting', { type => 'websocket.keepalive', interval => 30 }) }, qr/before websocket\.accept/, 'keepalive before accept');
    like( dies { $adv->('connecting', { type => 'websocket.send', text => 'x' }) }, qr/before websocket\.accept/, 'send before accept');
    like( dies { $adv->('accepted', { type => 'websocket.accept' }) }, qr/after websocket\.accept/, 'duplicate accept');
    like( dies { $adv->('accepted', { type => 'websocket.http.response.start', status => 401 }) }, qr/after websocket\.accept/, 'denial after accept');
    like( dies { $adv->('accepted', { type => 'websocket.http.response.body' }) }, qr/after websocket\.accept/, 'denial body after accept');
    like( dies { $adv->('denial', { type => 'websocket.send', text => 'x' }) }, qr/after websocket\.http\.response\.start/, 'frame while denying');
    like( dies { $adv->('denial', { type => 'websocket.keepalive', interval => 30 }) }, qr/after websocket\.http\.response\.start/, 'keepalive while denying');
    like( dies { $adv->('denial', { type => 'websocket.accept' }) }, qr/after websocket\.http\.response\.start/, 'accept while denying');
    like( dies { $adv->('closed', { type => 'websocket.send', text => 'x' }) }, qr/after websocket\.close/, 'send after close');
    like( dies { $adv->('closed', { type => 'websocket.close' }) }, qr/after websocket\.close/, 'second close also croaks (websocket close is not idempotent)');
    like( dies { $adv->('denial_complete', { type => 'websocket.close' }) }, qr/denial response already complete/, 'anything after denial complete');
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

done_testing;
