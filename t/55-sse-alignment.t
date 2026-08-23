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
                last if $n == 0;   # EOF: server closed after sse.close
                $response .= $buf;
            }
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
                last if $n == 0;   # EOF: server closed after sse.close
                $response .= $buf;
            }
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

done_testing;
