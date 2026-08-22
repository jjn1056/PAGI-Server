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
    # A fresh client per call, deliberately not pooled through $http: a
    # completed SSE stream always closes the connection server-side (see
    # _finish_sse_stream), but the sse.start response still advertises
    # "Connection: keep-alive" (pre-existing, out of this task's scope to
    # change). A pooled client would try to reuse that dead socket for the
    # next request and fail with "Connection closed while awaiting header".
    my $client = Net::Async::HTTP->new;
    $loop->add($client);
    my $res = $client->GET("http://127.0.0.1:$sse_port$_[0]", headers => { Accept => 'text/event-stream' })->get;
    $loop->remove($client);
    return $res;
};

like( $sse_get->('/sse-send-before-start')->content, qr/before sse\.start/,
    'sse.send before sse.start fails the Future' );
like( $sse_get->('/sse-newline-event')->content, qr/must not contain newline/,
    'newline in sse event name fails' );
$sse_get->('/sse-send-after-close');
like( $SseAfterClose::ERR // '', qr/after sse\.close/,
    'sse.send after sse.close fails' );
$sse_server->shutdown->get;

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
# handshake without a WebSocket client library getting in the way.
use IO::Socket::INET;

my $ws_sock = IO::Socket::INET->new(
    PeerAddr => '127.0.0.1',
    PeerPort => $ws_port,
    Proto    => 'tcp',
    Timeout  => 5,
);

SKIP: {
    skip "Cannot connect", 2 unless $ws_sock;

    my $key = 'dGhlIHNhbXBsZSBub25jZQ==';
    print $ws_sock "GET / HTTP/1.1\r\n";
    print $ws_sock "Host: 127.0.0.1:$ws_port\r\n";
    print $ws_sock "Upgrade: websocket\r\n";
    print $ws_sock "Connection: Upgrade\r\n";
    print $ws_sock "Sec-WebSocket-Key: $key\r\n";
    print $ws_sock "Sec-WebSocket-Version: 13\r\n";
    print $ws_sock "\r\n";

    # Pump the loop until the server closes the connection (it does so right
    # after the app's websocket.close, once the shape/sequence errors above
    # have already landed in the package vars).
    $ws_sock->blocking(0);
    my $deadline = time + 5;
    while (time < $deadline) {
        my $buf;
        my $n = sysread($ws_sock, $buf, 4096);
        last if defined($n) && $n == 0;   # EOF: server closed
        $loop->loop_once(0.1);
    }
    close $ws_sock;

    like( $WsErrs::SHAPE // '', qr/exactly one of bytes\/text/,
        'websocket.send with both bytes and text fails shape validation' );
    like( $WsErrs::SEQ // '', qr/cannot send 'websocket\.keepalive'/,
        'websocket.keepalive before accept fails sequencing' );
}

$ws_server->shutdown->get;

done_testing;
