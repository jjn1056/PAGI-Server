#!/usr/bin/env perl

# =============================================================================
# Test: PAGI::Server::EventValidator - Dev-mode event validation
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

# =============================================================================
# Server Integration Test (source inspection)
# =============================================================================

subtest 'server has validation hooks' => sub {
    my $source = do {
        open my $fh, '<', 'lib/PAGI/Server/Connection.pm' or die "Cannot read: $!";
        local $/;
        <$fh>;
    };

    # Verify validate_events flag in constructor
    like(
        $source,
        qr/validate_events\s*=>\s*\$args\{validate_events\}/,
        'validate_events in Connection constructor'
    );

    # Verify validation calls in send handlers (HTTP, SSE, H2 SSE, WebSocket)
    my @validation_calls = $source =~ /EventValidator::validate_/g;
    is(scalar @validation_calls, 4, 'four validation calls (HTTP, SSE, H2 SSE, WebSocket)');

    # Verify conditional on validate_events
    like(
        $source,
        qr/if \(\$weak_self->\{validate_events\}\)/,
        'validation is conditional on flag'
    );
};

subtest 'PAGI::Server exposes validate_events option' => sub {
    my $source = do {
        open my $fh, '<', 'lib/PAGI/Server.pm' or die "Cannot read: $!";
        local $/;
        <$fh>;
    };

    like(
        $source,
        qr/\{validate_events\}\s*=\s*delete \$params->\{validate_events\}/,
        'validate_events in Server _init'
    );

    like(
        $source,
        qr/validate_events\s*=>\s*\$self->\{validate_events\}/,
        'validate_events passed to Connection'
    );
};

subtest 'PAGI::Server auto-enables validate_events via PAGI_ENV' => sub {
    my $source = do {
        open my $fh, '<', 'lib/PAGI/Server.pm' or die "Cannot read: $!";
        local $/;
        <$fh>;
    };

    # Check that validate_events checks PAGI_ENV
    like(
        $source,
        qr/PAGI_ENV.*development/s,
        'validate_events checks PAGI_ENV for development mode'
    );
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

done_testing;
