use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
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
        # BEFORE validation, so this must RESOLVE, not fail. The test client
        # signals the disconnect itself (a raw-socket shutdown()) rather than
        # this response asking for one via Connection: close -- HTTP/1.1
        # strips an app-supplied Connection header (PAGI spec), so this app
        # has no lever over the wire's framing/connection headers at all.
        await $send->({ type => 'http.response.start', status => 200,
                        headers => [['content-type','text/plain']] });
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

# Post-close: malformed send after client disconnect resolves as a no-op.
# A raw socket, not the pooled Net::Async::HTTP client: the app's response no
# longer carries a Connection header for the strip to remove (see above), so
# the disconnect has to come from the client side actually going away --
# shutdown() on the write half is this suite's existing idiom for that (see
# t/http-incomplete-response.t's /cancel-before-start).
{
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5,
    );
    ok($sock, 'raw socket connected for /post-close') or last;
    $sock->blocking(0);

    print $sock "GET /post-close HTTP/1.1\r\nHost: 127.0.0.1:$port\r\n\r\n";

    my $wire = '';
    my $deadline = time + 5;
    while (time < $deadline) {
        my $buf;
        my $n = sysread($sock, $buf, 4096);
        $wire .= $buf if defined $n && $n > 0;
        $loop->loop_once(0.05);
        last if $wire =~ /0\r\n\r\n\z/;   # chunked terminator (no Content-Length here)
    }
    like( $wire, qr/bye/, 'post-close response delivered' );

    shutdown($sock, 1) or die "shutdown: $!";   # signal disconnect to the server
    my $probe_deadline = time + 5;
    while (time < $probe_deadline) {
        last if defined $PostClose::RESULT;
        $loop->loop_once(0.1);
    }
    is( $PostClose::RESULT, 'resolved',
        'malformed send after close resolves as a no-op (closed-check precedes validation)' );

    close $sock;
}

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
$loop->remove($http);

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
        if ($path eq '/sse-send-after-decline-complete') {
            await $send->({ type => 'sse.http.response.start', status => 200, headers => [['content-type','text/plain']] });
            await $send->({ type => 'sse.http.response.body', body => 'done', more => 0 });
            my $err = do { local $@; eval { await $send->({ type => 'sse.http.response.body', body => 'extra', more => 0 }) }; $@ };
            $SseAfterDeclineComplete::ERR = $err;   # observed via package var after request
            return;
        }
    }
    if ($scope->{type} eq 'http') {
        # Ordinary request, used to prove a connection survives an SSE exchange.
        await $receive->();
        await $send->({ type => 'http.response.start', status => 200,
                        headers => [['content-type','text/plain']] });
        await $send->({ type => 'http.response.body', body => 'PLAIN-OK', more => 0 });
        return;
    }
    die "unsupported scope $scope->{type}";
};

my $sse_server = PAGI::Server->new(app => $sse_app, host => '127.0.0.1', port => 0, quiet => 1, validate_events => 0);
$loop->add($sse_server);
$sse_server->listen->get;
my $sse_port = $sse_server->port;

# One pooled client for every SSE call: a cleanly ended SSE stream honors the
# "Connection: keep-alive" it advertised on sse.start (design section 11.6), so
# the pool's socket stays usable for the next request.
my $sse_client = Net::Async::HTTP->new;
$loop->add($sse_client);

my $sse_get = sub {
    return $sse_client->GET("http://127.0.0.1:$sse_port$_[0]",
        headers => { Accept => 'text/event-stream' })->get;
};

like( $sse_get->('/sse-send-before-start')->content, qr/before sse\.start/,
    'sse.send before sse.start fails the Future' );
like( $sse_get->('/sse-newline-event')->content, qr/must not contain newline/,
    'newline in sse event name fails' );
$sse_get->('/sse-send-after-close');
like( $SseAfterClose::ERR // '', qr/after sse\.close/,
    'sse.send after sse.close fails' );
$sse_get->('/sse-send-after-decline-complete');
like( $SseAfterDeclineComplete::ERR // '', qr/decline response already complete/,
    'sse.http.response.body after a completed decline fails, not silently swallowed' );

# The positive assertion behind dropping the fresh-client workaround: one raw
# socket -- literally one file descriptor -- performs an SSE exchange and then
# an ordinary request (design section 11.6).
{
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $sse_port, Proto => 'tcp', Timeout => 5,
    );
    ok($sock, 'raw socket connected') or last;
    $sock->blocking(0);

    my $read_until = sub {
        my ($stop) = @_;
        my $buf = '';
        my $deadline = time + 5;
        while (time < $deadline) {
            my $chunk;
            my $n = sysread($sock, $chunk, 4096);
            if (defined $n && $n > 0) { $buf .= $chunk }
            elsif (defined $n && $n == 0) { last }   # EOF
            last if $buf =~ $stop;
            $loop->loop_once(0.02);
        }
        return $buf;
    };

    print $sock "GET /sse-newline-event HTTP/1.1\r\nHost: 127.0.0.1:$sse_port\r\n"
              . "Accept: text/event-stream\r\n\r\n";
    my $stream = $read_until->(qr/\r\n0\r\n\r\n/);
    like($stream, qr/must not contain newline/, 'SSE exchange completed on the raw socket');
    like($stream, qr/\r\n0\r\n\r\n/, 'SSE stream ended with the chunked terminator');

    print $sock "GET /plain HTTP/1.1\r\nHost: 127.0.0.1:$sse_port\r\n\r\n";
    like($read_until->(qr/PLAIN-OK/), qr/^HTTP\/1\.1 200.*PLAIN-OK/s,
        'ordinary request served on the SAME socket after the SSE stream');
    close $sock;
}

$sse_server->shutdown->get;
$loop->remove($sse_client);

# --- WebSocket: shape and mis-sequencing errors on the send path ---
my $ws_app = async sub {
    my ($scope, $receive, $send) = @_;
    if ($scope->{type} eq 'websocket') {
        my $e = await $receive->();   # websocket.connect
        my $err1 = do { local $@; eval { await $send->({ type => 'websocket.send', bytes => 'a', text => 'b' }) }; $@ };
        ($WsErrs::SHAPE = $err1 // 'NO-ERROR') =~ s/\n/ /g;
        my $err2 = do { local $@; eval { await $send->({ type => 'websocket.keepalive', interval => 1 }) }; $@ };
        ($WsErrs::SEQ = $err2 // 'NO-ERROR') =~ s/\n/ /g;
        await $send->({ type => 'websocket.accept' });
        await $send->({ type => 'websocket.close' });
        return;
    }
    die "unsupported scope $scope->{type}";
};

my $ws_server = PAGI::Server->new(app => $ws_app, host => '127.0.0.1', port => 0, quiet => 1, validate_events => 0);
$loop->add($ws_server);
$ws_server->listen->get;
my $ws_port = $ws_server->port;

# Raw handshake boilerplate lifted from t/04-websocket.t: the app under test
# reads/writes application-level events, so a raw socket lets us drive the
# handshake without a WebSocket client library getting in the way. Pumps the
# loop until the server closes the connection, so that whatever the app
# coroutine recorded in its package vars is settled by the time we assert.
use IO::Socket::INET;

my $ws_handshake_and_drain = sub {
    my ($port) = @_;
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1',
        PeerPort => $port,
        Proto    => 'tcp',
        Timeout  => 5,
    );
    return unless $sock;

    my $key = 'dGhlIHNhbXBsZSBub25jZQ==';
    print $sock "GET / HTTP/1.1\r\n";
    print $sock "Host: 127.0.0.1:$port\r\n";
    print $sock "Upgrade: websocket\r\n";
    print $sock "Connection: Upgrade\r\n";
    print $sock "Sec-WebSocket-Key: $key\r\n";
    print $sock "Sec-WebSocket-Version: 13\r\n";
    print $sock "\r\n";

    $sock->blocking(0);
    my $deadline = time + 5;
    while (time < $deadline) {
        my $buf;
        my $n = sysread($sock, $buf, 4096);
        last if defined($n) && $n == 0;   # EOF: server closed
        $loop->loop_once(0.1);
    }
    close $sock;
    return 1;
};

SKIP: {
    skip "Cannot connect", 2 unless $ws_handshake_and_drain->($ws_port);

    like( $WsErrs::SHAPE // '', qr/exactly one of bytes\/text/,
        'websocket.send with both bytes and text fails shape validation' );
    like( $WsErrs::SEQ // '', qr/cannot send 'websocket\.keepalive'/,
        'websocket.keepalive before accept fails sequencing' );
}

$ws_server->shutdown->get;

# --- WebSocket: sends after a terminal state raise, not swallowed ---
# Same class of bug as the SSE carve-out above: websocket.close and a
# completed denial response both flip the connection's {closed} flag as a
# side effect (see _handle_disconnect_and_close), independent of a genuine
# client disconnect. A post-close/post-denial-complete send must still raise
# through advance_websocket, not silently no-op on the transport-closed check.

my $ws_after_close_app = async sub {
    my ($scope, $receive, $send) = @_;
    if ($scope->{type} eq 'websocket') {
        my $e = await $receive->();   # websocket.connect
        await $send->({ type => 'websocket.accept' });
        # App-initiated close with no client close received: the transport
        # isn't gone for real, only the app's own logical close.
        await $send->({ type => 'websocket.close' });
        my $err = do { local $@; eval { await $send->({ type => 'websocket.send', text => 'late' }) }; $@ };
        ($WsAfterClose::ERR = $err // 'NO-ERROR') =~ s/\n/ /g;
        return;
    }
    die "unsupported scope $scope->{type}";
};
my $ws_after_close_server = PAGI::Server->new(
    app => $ws_after_close_app, host => '127.0.0.1', port => 0, quiet => 1, validate_events => 0);
$loop->add($ws_after_close_server);
$ws_after_close_server->listen->get;

SKIP: {
    skip "Cannot connect", 1 unless $ws_handshake_and_drain->($ws_after_close_server->port);

    like( $WsAfterClose::ERR // '', qr/after websocket\.close/,
        'websocket.send after websocket.close raises, not silently swallowed' );
}
$ws_after_close_server->shutdown->get;

my $ws_denial_complete_app = async sub {
    my ($scope, $receive, $send) = @_;
    if ($scope->{type} eq 'websocket') {
        my $e = await $receive->();   # websocket.connect
        await $send->({ type => 'websocket.http.response.start', status => 403,
                        headers => [['content-type','text/plain']] });
        await $send->({ type => 'websocket.http.response.body', body => 'no', more => 0 });
        my $err = do { local $@; eval { await $send->({ type => 'websocket.http.response.body', body => 'extra', more => 0 }) }; $@ };
        ($WsDenialComplete::ERR = $err // 'NO-ERROR') =~ s/\n/ /g;
        return;
    }
    die "unsupported scope $scope->{type}";
};
my $ws_denial_complete_server = PAGI::Server->new(
    app => $ws_denial_complete_app, host => '127.0.0.1', port => 0, quiet => 1, validate_events => 0);
$loop->add($ws_denial_complete_server);
$ws_denial_complete_server->listen->get;

SKIP: {
    skip "Cannot connect", 1 unless $ws_handshake_and_drain->($ws_denial_complete_server->port);

    like( $WsDenialComplete::ERR // '', qr/denial response already complete/,
        'websocket.http.response.body after a completed denial raises, not silently swallowed' );
}
$ws_denial_complete_server->shutdown->get;

done_testing;
