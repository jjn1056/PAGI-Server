use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";

# Configure Future::IO for tests that use Future::IO->sleep()
eval { require Future::IO::Impl::IOAsync; 1 }
    or plan skip_all => 'Future::IO::Impl::IOAsync required for SSE tests';

use PAGI::Server;
use IO::Socket::INET;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# Media-range SSE detection (design doc section 11.5; PAGI Www.pod "SSE
# Connection Detection"): sse iff the request's Accept header(s) contain the
# exact media range text/event-stream, case-insensitively, with q > 0. This
# is a boolean client-signal test, not content negotiation -- wildcards
# never signal SSE, q=0 is an explicit refusal, and near-miss substrings
# (e.g. text/event-streamer) must not false-positive.

my $loop = IO::Async::Loop->new;

# Observed by the app on every request.
our $DETECTED_TYPE;

sub create_server {
    my ($test_app) = @_;

    my $app = $test_app // async sub {
        my ($scope, $receive, $send) = @_;

        $DETECTED_TYPE = $scope->{type};

        if ($scope->{type} eq 'sse') {
            # Decline cleanly so the client isn't left hanging.
            await $send->({ type => 'sse.http.response.start', status => 204, headers => [] });
            await $send->({ type => 'sse.http.response.body', body => '', more => 0 });
        }
        else {
            await $receive->();
            await $send->({
                type    => 'http.response.start',
                status  => 200,
                headers => [['content-type', 'text/plain']],
            });
            await $send->({ type => 'http.response.body', body => 'ok', more => 0 });
        }
    };

    my $server = PAGI::Server->new(
        app              => $app,
        host             => '127.0.0.1',
        port             => 0,
        quiet            => 1,
        shutdown_timeout => 1,
    );
    $loop->add($server);
    $server->listen->get;
    return $server;
}

# Sends a GET request with one Accept header per element of @accepts (so
# repeated Accept headers can be exercised as well as a single combined one).
sub send_request {
    my ($port, @accepts) = @_;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5,
    ) or return;

    my $req = "GET / HTTP/1.1\r\nHost: 127.0.0.1:$port\r\n";
    $req .= "Accept: $_\r\n" for @accepts;
    $req .= "\r\n";
    print $sock $req;

    $sock->blocking(0);
    my $deadline = time + 5;
    while (time < $deadline) {
        $loop->loop_once(0.05);
        last if defined $DETECTED_TYPE;
    }
    my $wire = '';
    while (1) {
        my $n = sysread($sock, my $buf, 4096);
        last unless $n;
        $wire .= $buf;
    }
    close $sock;
    $loop->loop_once(0.05) for 1 .. 10;
    return $wire;
}

# Parses an HTTP/1.1 chunked-encoding body into a list of [hex_len,
# content_bytes] pairs, one per chunk actually received. Stops (without
# dying) at the terminating 0-length chunk, or at the first incomplete
# trailing chunk -- callers that read a still-open SSE stream (rather than
# reading to EOF) will often have a partial chunk at the tail.
sub parse_chunks {
    my ($body) = @_;
    my @chunks;
    my $offset = 0;
    while (1) {
        my $nl = index($body, "\r\n", $offset);
        last if $nl < 0;
        my $hexlen = substr($body, $offset, $nl - $offset);
        last unless $hexlen =~ /^[0-9a-fA-F]+$/;
        my $len = hex($hexlen);
        my $data_start = $nl + 2;
        last if $len == 0;   # terminating chunk -- not consumed
        last if length($body) < $data_start + $len + 2;   # incomplete trailing chunk
        last unless substr($body, $data_start + $len, 2) eq "\r\n";
        push @chunks, [$hexlen, substr($body, $data_start, $len)];
        $offset = $data_start + $len + 2;
    }
    return @chunks;
}

# PAGI Www.pod "Send SSE": String fields (data, event, id, comment) MUST be
# UTF-8 encoded by the server before transmission (design section 11.2). The
# server internally stores these as Perl character strings; the encode must
# happen exactly once, at the wire boundary, and all chunk-length math must
# be computed on the resulting BYTE string.
subtest 'SSE UTF-8 wire encoding: data/event/id/comment are UTF-8 octets, chunk-size counts bytes' => sub {
    my $test_app = async sub {
        my ($scope, $receive, $send) = @_;
        die "Unsupported scope type: $scope->{type}" if $scope->{type} ne 'sse';

        await $send->({
            type    => 'sse.start',
            status  => 200,
            headers => [ [ 'content-type', 'text/event-stream' ] ],
        });

        await $send->({
            type  => 'sse.send',
            event => "\x{e9}vent",
            data  => "caf\x{e9}",
            id    => "\x{e9}d",
        });

        await $send->({ type => 'sse.comment', comment => "r\x{e9}sum\x{e9}" });

        await $send->({ type => 'sse.close' });
    };

    my $server = create_server($test_app);
    my $port   = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5,
    );

    SKIP: {
        skip "Cannot connect", 1 unless $sock;

        print $sock "GET / HTTP/1.1\r\n";
        print $sock "Host: 127.0.0.1:$port\r\n";
        print $sock "Accept: text/event-stream\r\n";
        print $sock "\r\n";

        $sock->blocking(0);
        my $response = '';
        my $deadline = time + 5;
        while (time < $deadline) {
            my $buf;
            my $n = sysread($sock, $buf, 4096);
            if (defined $n) {
                last if $n == 0;   # EOF (a client Connection: close would give one)
                $response .= $buf;
            }
            # A clean sse.close keeps the connection alive (design 11.6), so
            # stop at the terminating zero-length chunk, not at EOF.
            last if $response =~ /\r\n0\r\n\r\n/;
            $loop->loop_once(0.1);
        }
        close $sock;

        my ($header, $body) = split /\r\n\r\n/, $response, 2;
        my @chunks = parse_chunks($body // '');

        is(scalar(@chunks), 2, 'two SSE frames received (event, comment)')
            or diag "wire body: " . (defined $body ? unpack('H*', $body) : '(undef)');

        my ($event_chunk, $comment_chunk) = @chunks;

        # U+00E9 (e-acute) is UTF-8 octets \xc3\xa9. Assert the exact octets,
        # not the character -- this must be a byte string on the wire.
        like($event_chunk->[1], qr/event: \xc3\xa9vent\n/, 'event field UTF-8 octets on the wire');
        like($event_chunk->[1], qr/data: caf\xc3\xa9\n/,   'data field UTF-8 octets on the wire');
        like($event_chunk->[1], qr/id: \xc3\xa9d\n/,       'id field UTF-8 octets on the wire');
        is(hex($event_chunk->[0]), length($event_chunk->[1]),
            'event frame: chunk-size prefix equals BYTE length');

        like($comment_chunk->[1], qr/^:r\xc3\xa9sum\xc3\xa9\n\n\z/, 'comment field UTF-8 octets on the wire');
        is(hex($comment_chunk->[0]), length($comment_chunk->[1]),
            'comment frame: chunk-size prefix equals BYTE length');
    }

    $server->shutdown->get;
};

subtest 'SSE UTF-8 wire encoding: invalid string fails the send Future; stream stays usable' => sub {
    my $invalid_err;

    my $test_app = async sub {
        my ($scope, $receive, $send) = @_;
        die "Unsupported scope type: $scope->{type}" if $scope->{type} ne 'sse';

        await $send->({
            type    => 'sse.start',
            status  => 200,
            headers => [ [ 'content-type', 'text/event-stream' ] ],
        });

        # A lone UTF-16 surrogate has no UTF-8 representation.
        eval { await $send->({ type => 'sse.send', data => "bad \x{D800}" }) };
        $invalid_err = $@;

        await $send->({ type => 'sse.send', event => 'after', data => 'still-fine' });
        await $send->({ type => 'sse.close' });
    };

    my $server = create_server($test_app);
    my $port   = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5,
    );

    SKIP: {
        skip "Cannot connect", 1 unless $sock;

        print $sock "GET / HTTP/1.1\r\n";
        print $sock "Host: 127.0.0.1:$port\r\n";
        print $sock "Accept: text/event-stream\r\n";
        print $sock "\r\n";

        $sock->blocking(0);
        my $response = '';
        my $deadline = time + 5;
        while (time < $deadline) {
            my $buf;
            my $n = sysread($sock, $buf, 4096);
            if (defined $n) {
                last if $n == 0;   # EOF (a client Connection: close would give one)
                $response .= $buf;
            }
            # A clean sse.close keeps the connection alive (design 11.6), so
            # stop at the terminating zero-length chunk, not at EOF.
            last if $response =~ /\r\n0\r\n\r\n/;
            $loop->loop_once(0.1);
        }
        close $sock;

        like($invalid_err, qr/not encodable as UTF-8/i, 'invalid surrogate send fails the send Future');
        unlike($response, qr/bad/, 'invalid payload never reached the wire');
        like($response, qr/event: after\ndata: still-fine\n\n/,
            'stream stayed usable: subsequent valid send arrived on the wire');
    }

    $server->shutdown->get;
};

subtest 'SSE UTF-8 wire encoding: keepalive comment is UTF-8-encoded on the wire' => sub {
    my $test_app = async sub {
        my ($scope, $receive, $send) = @_;
        die "Unsupported scope type: $scope->{type}" if $scope->{type} ne 'sse';

        await $send->({
            type    => 'sse.start',
            status  => 200,
            headers => [ [ 'content-type', 'text/event-stream' ] ],
        });

        await $send->({ type => 'sse.keepalive', interval => 0.2, comment => "p\x{e9}ng" });
        await $send->({ type => 'sse.send', data => 'start' });

        await $loop->delay_future(after => 0.5);

        await $send->({ type => 'sse.send', data => 'end' });
    };

    my $server = create_server($test_app);
    my $port   = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5,
    );

    SKIP: {
        skip "Cannot connect", 1 unless $sock;

        print $sock "GET / HTTP/1.1\r\n";
        print $sock "Host: 127.0.0.1:$port\r\n";
        print $sock "Accept: text/event-stream\r\n";
        print $sock "\r\n";

        $sock->blocking(0);
        my $response = '';
        my $deadline = time + 5;
        while (time < $deadline) {
            my $buf;
            my $n = sysread($sock, $buf, 4096);
            $response .= $buf if defined $n && $n > 0;
            $loop->loop_once(0.1);
            last if $response =~ /data: end\n/;
        }
        $loop->loop_once(0.1) for 1 .. 3;
        while (1) {
            my $buf;
            my $n = sysread($sock, $buf, 4096);
            last unless defined $n && $n > 0;
            $response .= $buf;
        }
        close $sock;

        my ($header, $body) = split /\r\n\r\n/, $response, 2;
        my @chunks = parse_chunks($body // '');

        my @pings = grep { $_->[1] =~ /^:p\xc3\xa9ng\n\n\z/ } @chunks;
        ok(scalar(@pings) >= 1, 'at least one keepalive comment arrived, UTF-8-encoded')
            or diag "wire body: " . (defined $body ? unpack('H*', $body) : '(undef)');
        for my $c (@pings) {
            is(hex($c->[0]), length($c->[1]), 'keepalive frame: chunk-size prefix equals BYTE length');
        }
    }

    $server->shutdown->get;
};

subtest 'SSE keepalive: unencodable comment fails the send Future at arm time, not later inside the timer tick' => sub {
    my $invalid_err;

    my $test_app = async sub {
        my ($scope, $receive, $send) = @_;
        die "Unsupported scope type: $scope->{type}" if $scope->{type} ne 'sse';

        await $send->({
            type    => 'sse.start',
            status  => 200,
            headers => [ [ 'content-type', 'text/event-stream' ] ],
        });

        # A lone UTF-16 surrogate has no UTF-8 representation. Before this
        # task this armed a timer that died on its first tick; now it must
        # fail THIS send's Future immediately, and no timer is ever armed.
        eval { await $send->({ type => 'sse.keepalive', interval => 0.1, comment => "bad \x{D800}" }) };
        $invalid_err = $@;

        await $send->({ type => 'sse.send', data => 'start' });

        # Bounded wait spanning several would-be keepalive intervals -- long
        # enough that a wrongly-armed timer would have ticked at least once.
        await $loop->delay_future(after => 0.5);

        await $send->({ type => 'sse.send', data => 'end' });
    };

    my $server = create_server($test_app);
    my $port   = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5,
    );

    SKIP: {
        skip "Cannot connect", 1 unless $sock;

        print $sock "GET / HTTP/1.1\r\n";
        print $sock "Host: 127.0.0.1:$port\r\n";
        print $sock "Accept: text/event-stream\r\n";
        print $sock "\r\n";

        $sock->blocking(0);
        my $response = '';
        my $deadline = time + 5;
        while (time < $deadline) {
            my $buf;
            my $n = sysread($sock, $buf, 4096);
            $response .= $buf if defined $n && $n > 0;
            $loop->loop_once(0.1);
            last if $response =~ /data: end\n/;
        }
        $loop->loop_once(0.1) for 1 .. 3;
        while (1) {
            my $buf;
            my $n = sysread($sock, $buf, 4096);
            last unless defined $n && $n > 0;
            $response .= $buf;
        }
        close $sock;

        like($invalid_err, qr/sse\.keepalive 'comment' must be a UTF-8-encodable string/,
            'unencodable keepalive comment fails the send Future at arm time');

        my ($header, $body) = split /\r\n\r\n/, $response, 2;
        my @chunks = parse_chunks($body // '');
        my @pings = grep { $_->[1] =~ /^:/ } @chunks;
        is(scalar(@pings), 0, 'no keepalive tick ever fired -- no comment bytes on the wire');

        like($response, qr/data: start\n/, 'stream stayed usable: send before the failed arm still landed');
        like($response, qr/data: end\n/,   'stream stayed usable: send after the failed arm still landed');
    }

    $server->shutdown->get;
};

my @matrix = (
    ['text/event-stream',                             'sse',  'exact match'],
    ['TEXT/EVENT-STREAM',                             'sse',  'case-insensitive match'],
    ['text/event-stream;q=0.4',                        'sse',  'q>0 signals sse'],
    ['text/event-stream, text/html;q=0.9',             'sse',  'presence, not preference'],
    ['text/event-stream;q=0',                          'http', 'q=0 is an explicit refusal'],
    ['*/*',                                            'http', 'wildcard never signals sse'],
    ['text/*',                                         'http', 'partial wildcard never signals sse'],
    ['application/json, text/event-streamer',           'http', 'substring near-miss must not false-positive'],
);

for my $row (@matrix) {
    my ($accept, $expected, $label) = @$row;
    subtest "Accept: $accept -> $expected ($label)" => sub {
        local $DETECTED_TYPE;
        my $server = create_server();
        my $port   = $server->port;
        send_request($port, $accept);
        is($DETECTED_TYPE, $expected, "scope type is $expected");
        $server->shutdown->get;
    };
}

subtest 'two Accept headers, only the second carries the range -> sse' => sub {
    local $DETECTED_TYPE;
    my $server = create_server();
    my $port   = $server->port;
    send_request($port, 'application/json', 'text/event-stream');
    is($DETECTED_TYPE, 'sse', 'scope type is sse');
    $server->shutdown->get;
};

# Design section 11.1: truthful SSE request bodies. The h1 SSE receive
# closure must deliver GET requests as a single empty terminal sse.request,
# and POST/PUT bodies -- whether framed with Content-Length, chunked
# Transfer-Encoding, or gated behind Expect: 100-continue -- completely and
# with truthful `more` flags, exactly like the plain-http receive path.

subtest 'GET SSE: single empty terminal sse.request event (design section 11.1)' => sub {
    my $first_event;
    my $second_event;

    my $test_app = async sub {
        my ($scope, $receive, $send) = @_;
        die "Unsupported scope type: $scope->{type}" if $scope->{type} ne 'sse';

        $first_event = await $receive->();

        await $send->({
            type    => 'sse.start',
            status  => 200,
            headers => [ [ 'content-type', 'text/event-stream' ] ],
        });

        # The scope's sse.request event fires exactly once; a second
        # receive() call must not deliver another one -- it waits for the
        # client to disconnect.
        $second_event = await $receive->();
    };

    my $server = create_server($test_app);
    my $port   = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5,
    );

    SKIP: {
        skip "Cannot connect", 4 unless $sock;

        print $sock "GET / HTTP/1.1\r\n";
        print $sock "Host: 127.0.0.1:$port\r\n";
        print $sock "Accept: text/event-stream\r\n";
        print $sock "\r\n";

        $sock->blocking(0);
        my $deadline = time + 3;
        while (time < $deadline && !$first_event) {
            $loop->loop_once(0.1);
        }

        is($first_event->{type}, 'sse.request', 'first event is sse.request');
        is($first_event->{body}, '', 'GET body is empty');
        is($first_event->{more}, 0, 'GET terminal event has more=>0');

        close $sock;

        $deadline = time + 3;
        while (time < $deadline && !$second_event) {
            $loop->loop_once(0.1);
        }

        is($second_event->{type}, 'sse.disconnect',
            'second receive() is sse.disconnect, not a second sse.request');
    }

    $server->shutdown->get;
};

subtest 'POST SSE with Content-Length body delivered truthfully across multiple reads (h1, pin)' => sub {
    my @events;

    my $test_app = async sub {
        my ($scope, $receive, $send) = @_;
        die "Unsupported scope type: $scope->{type}" if $scope->{type} ne 'sse';

        while (1) {
            my $event = await $receive->();
            push @events, $event;
            last if $event->{type} ne 'sse.request' || !$event->{more};
        }

        await $send->({
            type    => 'sse.start',
            status  => 200,
            headers => [ [ 'content-type', 'text/event-stream' ] ],
        });
        my $body = join('', map { $_->{body} // '' } @events);
        await $send->({ type => 'sse.send', data => $body });

        # Wait for the client to disconnect rather than returning
        # immediately -- returning here would tear the connection down
        # synchronously from inside the same read callback that just
        # completed the body, which is an unrelated connection-teardown
        # timing concern this test isn't exercising.
        await $receive->();
    };

    my $server = create_server($test_app);
    my $port   = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5,
    );

    SKIP: {
        skip "Cannot connect", 4 unless $sock;

        my $body = 'Hello World';
        my $head = "POST / HTTP/1.1\r\nHost: 127.0.0.1:$port\r\nAccept: text/event-stream\r\n"
                 . "Content-Length: " . length($body) . "\r\n\r\n";
        print $sock $head;

        $sock->blocking(0);
        $loop->loop_once(0.1) for 1 .. 3;

        # Send the body in two separate writes so the server must read it
        # across at least two buffer fills.
        print $sock substr($body, 0, 5);
        $loop->loop_once(0.1) for 1 .. 3;
        print $sock substr($body, 5);

        my $response = '';
        my $deadline = time + 5;
        while (time < $deadline) {
            my $buf;
            my $n = sysread($sock, $buf, 4096);
            $response .= $buf if defined $n && $n > 0;
            $loop->loop_once(0.1);
            last if $response =~ /data: Hello World/;
        }
        close $sock;
        $loop->loop_once(0.1) for 1 .. 5;

        ok(scalar(@events) >= 2, "body arrived across multiple sse.request events (got " . scalar(@events) . ")")
            or diag "events: " . join(',', map { "$_->{type}:more=" . ($_->{more} // 'undef') . ":'" . ($_->{body} // '') . "'" } @events);
        my $joined = join('', map { $_->{body} // '' } @events);
        is($joined, $body, 'concatenated body matches what was sent');
        is($events[-1]{more}, 0, 'final event has more=>0');
        like($response, qr/data: Hello World/, 'app observed the full body');
    }

    $server->shutdown->get;
};

subtest 'POST SSE with chunked Transfer-Encoding body reaches sse.request (h1)' => sub {
    my @events;

    my $test_app = async sub {
        my ($scope, $receive, $send) = @_;
        die "Unsupported scope type: $scope->{type}" if $scope->{type} ne 'sse';

        while (1) {
            my $event = await $receive->();
            push @events, $event;
            last if $event->{type} ne 'sse.request' || !$event->{more};
        }

        await $send->({
            type    => 'sse.start',
            status  => 200,
            headers => [ [ 'content-type', 'text/event-stream' ] ],
        });
        my $body = join('', map { $_->{body} // '' } @events);
        await $send->({ type => 'sse.send', data => $body });

        # Wait for the client to disconnect rather than returning
        # immediately -- returning here would tear the connection down
        # synchronously from inside the same read callback that just
        # completed the body, which is an unrelated connection-teardown
        # timing concern this test isn't exercising.
        await $receive->();
    };

    my $server = create_server($test_app);
    my $port   = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5,
    );

    SKIP: {
        skip "Cannot connect", 3 unless $sock;

        my $request = "POST / HTTP/1.1\r\n";
        $request .= "Host: 127.0.0.1:$port\r\n";
        $request .= "Accept: text/event-stream\r\n";
        $request .= "Transfer-Encoding: chunked\r\n";
        $request .= "\r\n";
        $request .= "5\r\nHello\r\n";
        $request .= "6\r\n World\r\n";
        $request .= "0\r\n\r\n";
        print $sock $request;

        $sock->blocking(0);
        my $response = '';
        my $deadline = time + 5;
        while (time < $deadline) {
            my $buf;
            my $n = sysread($sock, $buf, 4096);
            $response .= $buf if defined $n && $n > 0;
            $loop->loop_once(0.1);
            last if $response =~ /data: Hello World/;
        }
        close $sock;
        $loop->loop_once(0.1) for 1 .. 5;

        my $joined = join('', map { $_->{body} // '' } @events);
        is($joined, 'Hello World', 'chunked-encoded body reassembled and delivered via sse.request')
            or diag "events: " . join(',', map { "$_->{type}:more=" . ($_->{more} // 'undef') . ":'" . ($_->{body} // '') . "'" } @events);
        ok(@events && $events[-1]{more} == 0, 'final event has more=>0');
        like($response, qr/data: Hello World/, 'app observed the full chunked body');
    }

    $server->shutdown->get;
};

subtest 'POST SSE with Expect: 100-continue: server sends 100 Continue before body arrives (h1)' => sub {
    my @events;

    my $test_app = async sub {
        my ($scope, $receive, $send) = @_;
        die "Unsupported scope type: $scope->{type}" if $scope->{type} ne 'sse';

        while (1) {
            my $event = await $receive->();
            push @events, $event;
            last if $event->{type} ne 'sse.request' || !$event->{more};
        }

        await $send->({
            type    => 'sse.start',
            status  => 200,
            headers => [ [ 'content-type', 'text/event-stream' ] ],
        });
        my $body = join('', map { $_->{body} // '' } @events);
        await $send->({ type => 'sse.send', data => $body });

        # Wait for the client to disconnect rather than returning
        # immediately -- returning here would tear the connection down
        # synchronously from inside the same read callback that just
        # completed the body, which is an unrelated connection-teardown
        # timing concern this test isn't exercising.
        await $receive->();
    };

    my $server = create_server($test_app);
    my $port   = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5,
    );

    SKIP: {
        skip "Cannot connect", 2 unless $sock;

        my $body = 'Hello Continue';
        my $head = "POST / HTTP/1.1\r\nHost: 127.0.0.1:$port\r\nAccept: text/event-stream\r\n"
                 . "Content-Length: " . length($body) . "\r\nExpect: 100-continue\r\n\r\n";
        print $sock $head;

        $sock->blocking(0);

        # A raw client following RFC 9110 SS10.1.1 waits for "100 Continue"
        # before sending the body -- verify the server produces it without
        # having received any body bytes yet.
        my $interim = '';
        my $deadline = time + 5;
        while (time < $deadline) {
            my $buf;
            my $n = sysread($sock, $buf, 4096);
            $interim .= $buf if defined $n && $n > 0;
            $loop->loop_once(0.1);
            last if $interim =~ /\r\n\r\n/;
        }
        like($interim, qr/^HTTP\/1\.1 100 Continue\r\n/,
            'server sent 100 Continue before the client sent the body');

        print $sock $body;

        my $response = '';
        $deadline = time + 5;
        while (time < $deadline) {
            my $buf;
            my $n = sysread($sock, $buf, 4096);
            $response .= $buf if defined $n && $n > 0;
            $loop->loop_once(0.1);
            last if $response =~ /data: Hello Continue/;
        }
        close $sock;
        $loop->loop_once(0.1) for 1 .. 5;

        my $joined = join('', map { $_->{body} // '' } @events);
        is($joined, $body, 'body delivered after the 100-continue handshake');
        like($response, qr/data: Hello Continue/, 'app observed the full body');
    }

    $server->shutdown->get;
};

# ---------------------------------------------------------------------------
# Design section 11.6: connection reuse after an SSE stream ends (HTTP/1.1).
#
# sse.start advertises "Connection: keep-alive", so a clean stream end (the
# application returning, or an explicit sse.close) must hand the connection
# back to ordinary keep-alive request handling instead of closing it. These
# tests drive raw sockets so that "the SAME connection" is literally the same
# file descriptor, not a client library's pooling behavior.
# ---------------------------------------------------------------------------

# Reads from $sock, pumping the loop, until $stop matches what has been read,
# EOF arrives, or $timeout seconds pass. Returns (bytes_read, saw_eof).
sub read_until {
    my ($sock, $stop, $timeout) = @_;
    $timeout //= 5;
    my $buf = '';
    my $eof = 0;
    my $deadline = time + $timeout;
    while (time < $deadline) {
        my $chunk;
        my $n = sysread($sock, $chunk, 4096);
        if (defined $n && $n > 0) { $buf .= $chunk }
        elsif (defined $n && $n == 0) { $eof = 1; last }
        last if defined $stop && $buf =~ $stop;
        $loop->loop_once(0.02);
    }
    return ($buf, $eof);
}

sub sse_socket {
    my ($port, %opt) = @_;
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5,
    ) or return;
    my $version = $opt{http_version} // '1.1';
    my $path    = $opt{path} // '/';
    my $req = "GET $path HTTP/$version\r\nHost: 127.0.0.1:$port\r\nAccept: text/event-stream\r\n";
    $req .= "Connection: $opt{connection}\r\n" if $opt{connection};
    $req .= "\r\n";
    print $sock $req;
    $sock->blocking(0);
    return $sock;
}

# A further SSE request issued on an already-used socket, read to its terminator.
sub sse_request_on {
    my ($sock, $port, $path) = @_;
    print $sock "GET $path HTTP/1.1\r\nHost: 127.0.0.1:$port\r\n"
              . "Accept: text/event-stream\r\n\r\n";
    return read_until($sock, qr/\r\n0\r\n\r\n/);
}

# The terminating zero-length chunk of the SSE response body.
my $TERMINATOR = qr/\r\n0\r\n\r\n/;

# An ordinary (non-SSE) request issued on an already-used socket.
sub plain_request_on {
    my ($sock, $port) = @_;
    print $sock "GET /plain HTTP/1.1\r\nHost: 127.0.0.1:$port\r\n\r\n";
    return read_until($sock, qr/PLAIN-OK/);
}

# Serves SSE for text/event-stream requests (behavior chosen by $sse_body) and
# a fixed 'PLAIN-OK' response for everything else, so one server can answer
# both halves of a reuse exchange.
sub reuse_server {
    my ($sse_body) = @_;
    return create_server(async sub {
        my ($scope, $receive, $send) = @_;
        if ($scope->{type} eq 'sse') {
            return await $sse_body->($scope, $receive, $send);
        }
        await $send->({ type => 'http.response.start', status => 200,
                        headers => [['content-type', 'text/plain']] });
        await $send->({ type => 'http.response.body', body => 'PLAIN-OK', more => 0 });
        return;
    });
}

subtest 'clean SSE end by returning: connection stays open and serves the next request' => sub {
    my $server = reuse_server(async sub {
        my ($scope, $receive, $send) = @_;
        await $send->({ type => 'sse.start', status => 200 });
        await $send->({ type => 'sse.send', event => 'tick', data => 'one' });
        return;   # clean end by returning
    });
    my $port = $server->port;

    my $sock = sse_socket($port);
    SKIP: {
        skip "Cannot connect", 4 unless $sock;

        my ($stream, $eof) = read_until($sock, $TERMINATOR);
        like($stream, qr/^HTTP\/1\.1 200/,        'SSE response started');
        like($stream, qr/data: one\n/,            'event delivered');
        like($stream, $TERMINATOR,                'chunked terminator written on a clean end');
        ok(!$eof,                                 'connection NOT closed after a clean end');

        my ($second) = plain_request_on($sock, $port);
        like($second, qr/^HTTP\/1\.1 200/,        'second request served on the SAME socket');
        like($second, qr/PLAIN-OK/,               'second response body arrived');
        close $sock;
    }

    $server->shutdown->get;
};

subtest 'clean SSE end by sse.close: connection stays open and serves the next request' => sub {
    my $server = reuse_server(async sub {
        my ($scope, $receive, $send) = @_;
        await $send->({ type => 'sse.start', status => 200 });
        await $send->({ type => 'sse.send', event => 'tick', data => 'two' });
        await $send->({ type => 'sse.close' });
        return;
    });
    my $port = $server->port;

    my $sock = sse_socket($port);
    SKIP: {
        skip "Cannot connect", 4 unless $sock;

        my ($stream, $eof) = read_until($sock, $TERMINATOR);
        like($stream, qr/data: two\n/, 'event delivered');
        like($stream, $TERMINATOR,     'chunked terminator written on sse.close');
        ok(!$eof,                      'connection NOT closed after sse.close');

        my ($second) = plain_request_on($sock, $port);
        like($second, qr/PLAIN-OK/,    'second request served on the SAME socket after sse.close');
        close $sock;
    }

    $server->shutdown->get;
};

subtest 'post-sse.close send still raises, with the connection left open' => sub {
    my $post_close_err;
    my $server = reuse_server(async sub {
        my ($scope, $receive, $send) = @_;
        await $send->({ type => 'sse.start', status => 200 });
        await $send->({ type => 'sse.send', data => 'three' });
        await $send->({ type => 'sse.close' });
        $post_close_err = do {
            local $@;
            eval { await $send->({ type => 'sse.send', data => 'LATE' }); undef } // $@;
        };
        return;
    });
    my $port = $server->port;

    my $sock = sse_socket($port);
    SKIP: {
        skip "Cannot connect", 4 unless $sock;

        my ($stream, $eof) = read_until($sock, $TERMINATOR);
        like($stream, $TERMINATOR, 'chunked terminator written');
        ok(!$eof,                  'connection still open');
        like($post_close_err, qr/after sse\.close/,
            'sse.send after sse.close still raises with the connection open');
        unlike($stream, qr/LATE/,  'post-close payload never reached the wire');

        my ($second) = plain_request_on($sock, $port);
        like($second, qr/PLAIN-OK/, 'connection remained usable after the raised send');
        close $sock;
    }

    $server->shutdown->get;
};

subtest 'SSE keepalive from the finished stream does not leak into the next request' => sub {
    my $server = reuse_server(async sub {
        my ($scope, $receive, $send) = @_;
        await $send->({ type => 'sse.start', status => 200 });
        await $send->({ type => 'sse.keepalive', interval => 0.1, comment => 'ka' });
        await $send->({ type => 'sse.send', data => 'four' });
        await $loop->delay_future(after => 0.35);
        await $send->({ type => 'sse.close' });
        return;
    });
    my $port = $server->port;

    my $sock = sse_socket($port);
    SKIP: {
        skip "Cannot connect", 3 unless $sock;

        my ($stream, $eof) = read_until($sock, $TERMINATOR);
        like($stream, qr/:ka\n/,   'keepalive comments were flowing during the stream');
        like($stream, $TERMINATOR, 'chunked terminator written');
        ok(!$eof,                  'connection still open');

        my ($second) = plain_request_on($sock, $port);
        like($second, qr/PLAIN-OK/, 'second request served');

        # Give the (now stopped) keepalive timer several intervals to misfire.
        my ($trailing) = read_until($sock, undef, 0.6);
        is($trailing, '', 'no keepalive bytes leaked onto the reused connection')
            or diag "trailing bytes: " . unpack('H*', $trailing);
        close $sock;
    }

    $server->shutdown->get;
};

subtest 'client Connection: close still closes after a clean SSE end (control)' => sub {
    my $server = reuse_server(async sub {
        my ($scope, $receive, $send) = @_;
        await $send->({ type => 'sse.start', status => 200 });
        await $send->({ type => 'sse.send', data => 'five' });
        return;
    });
    my $port = $server->port;

    my $sock = sse_socket($port, connection => 'close');
    SKIP: {
        skip "Cannot connect", 2 unless $sock;

        my ($stream, $eof) = read_until($sock, undef);
        like($stream, $TERMINATOR, 'chunked terminator written');
        ok($eof,                   'Connection: close still closes the connection');
        close $sock;
    }

    $server->shutdown->get;
};

subtest 'HTTP/1.0 still closes after a clean SSE end (control)' => sub {
    my $server = reuse_server(async sub {
        my ($scope, $receive, $send) = @_;
        await $send->({ type => 'sse.start', status => 200 });
        await $send->({ type => 'sse.send', data => 'six' });
        return;
    });
    my $port = $server->port;

    my $sock = sse_socket($port, http_version => '1.0');
    SKIP: {
        skip "Cannot connect", 1 unless $sock;

        my (undef, $eof) = read_until($sock, undef);
        ok($eof, 'HTTP/1.0 still closes the connection after the stream ends');
        close $sock;
    }

    $server->shutdown->get;
};

# Keep-alive must never be handed to a client that received NO response. The
# http path treats "the application returned without starting a response" as a
# protocol error (500 + close); an SSE application that returns without
# sse.start and without declining has to get the same treatment, or the client
# sits on a silent socket until the idle timeout -- forever with timeout => 0.
subtest 'SSE app that returns without any response: 500 and close, never a silent kept-alive socket' => sub {
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };

    my $server = reuse_server(async sub {
        my ($scope, $receive, $send) = @_;
        return;   # no sse.start, no decline -- nothing at all
    });
    my $port = $server->port;

    my $sock = sse_socket($port);
    SKIP: {
        skip "Cannot connect", 3 unless $sock;

        my ($wire, $eof) = read_until($sock, undef, 3);
        like($wire, qr{^HTTP/1\.1 500}, 'server synthesized a 500, matching the http path');
        ok($eof, 'connection closed promptly instead of being kept alive with zero bytes written');
        ok((scalar grep { /without starting an SSE stream or a response/i } @warnings),
            'the protocol error was warned') or diag("warnings: @warnings");
        close $sock;
    }

    $server->shutdown->get;
};

subtest 'SSE decline that starts but never sends its terminal body: closes, does not hang' => sub {
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };

    my $server = reuse_server(async sub {
        my ($scope, $receive, $send) = @_;
        await $send->({ type => 'sse.http.response.start', status => 204, headers => [] });
        return;   # the terminal sse.http.response.body never comes
    });
    my $port = $server->port;

    my $sock = sse_socket($port);
    SKIP: {
        skip "Cannot connect", 2 unless $sock;

        my ($wire, $eof) = read_until($sock, undef, 3);
        ok($eof, 'an unfinished decline closes the connection rather than hanging');
        like($wire, qr{^HTTP/1\.1 500}, 'and reports the incomplete response as a 500');
        unlike($wire, qr/204/, 'the never-completed decline status was not sent');
        close $sock;
    }

    $server->shutdown->get;
};

# The riskiest property of the reset block: a reused connection must be able to
# run a WHOLE second SSE stream, with its own keepalive, and still come back.
subtest 'a reused connection serves a SECOND SSE stream, then a plain request' => sub {
    my $server = reuse_server(async sub {
        my ($scope, $receive, $send) = @_;
        my ($n) = (($scope->{query_string} // '') =~ /n=(\d+)/);
        $n //= 0;
        await $send->({ type => 'sse.start', status => 200 });
        await $send->({ type => 'sse.keepalive', interval => 0.1, comment => "ka$n" });
        await $send->({ type => 'sse.send', event => 'tick', data => "stream$n" });
        await $loop->delay_future(after => 0.25);
        await $send->({ type => 'sse.close' });
        return;
    });
    my $port = $server->port;

    my $sock = sse_socket($port, path => '/?n=1');
    SKIP: {
        skip "Cannot connect", 7 unless $sock;

        my ($first, $eof1) = read_until($sock, $TERMINATOR);
        like($first, qr/data: stream1\n/, 'first stream delivered its event');
        like($first, qr/:ka1\n/,          'first stream keepalive ran');
        ok(!$eof1,                        'connection survived the first stream');

        my ($second, $eof2) = sse_request_on($sock, $port, '/?n=2');
        like($second, qr{^HTTP/1\.1 200}, 'second SSE stream started on the SAME socket');
        like($second, qr/data: stream2\n/, 'second stream delivered its own event');
        like($second, qr/:ka2\n/,          'keepalive re-armed for the second stream');
        unlike($second, qr/:ka1\n/,        'the first stream keepalive did not survive the reset');
        like($second, $TERMINATOR,         'second stream framed off with its own terminator');
        ok(!$eof2,                         'connection survived the second stream too');

        my ($third) = plain_request_on($sock, $port);
        like($third, qr/PLAIN-OK/, 'ordinary request served after two SSE streams');
        close $sock;
    }

    $server->shutdown->get;
};

subtest 'abnormal mid-stream client abort still tears the connection down (control)' => sub {
    my @seen;
    my $server = reuse_server(async sub {
        my ($scope, $receive, $send) = @_;
        await $send->({ type => 'sse.start', status => 200 });
        await $send->({ type => 'sse.send', data => 'seven' });
        while (1) {                       # drain the request body, then block
            my $e = await $receive->();   # until the abort delivers sse.disconnect
            push @seen, $e;
            last if $e->{type} ne 'sse.request';
        }
        return;
    });
    my $port = $server->port;

    my $sock = sse_socket($port);
    SKIP: {
        skip "Cannot connect", 2 unless $sock;

        my ($stream) = read_until($sock, qr/data: seven\n/);
        like($stream, qr/data: seven\n/, 'stream was live before the abort');
        close $sock;                     # abort mid-stream
        $loop->loop_once(0.05) for 1 .. 20;

        is($seen[-1]{type}, 'sse.disconnect',
            'abnormal end delivers sse.disconnect to the application');
        is(scalar(keys %{$server->{connections}}), 0,
            'abnormal end still closes the connection');
    }

    $server->shutdown->get;
};

done_testing;
