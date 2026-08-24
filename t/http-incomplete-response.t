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

# Package globals used to observe the app-facing on_disconnect/on_complete
# callbacks from outside the app closure (mirrors t/53's cross-scope pattern).
package Inc;
our ($REASON, $COMPLETED, $CL_REASON, $CL_COMPLETED, $OK_COMPLETED);
our ($THROW_REASON, $THROW_COMPLETED);
our ($CANCEL_REASON, $CANCEL_COMPLETED, $CANCEL_WAITING, $CANCEL_RETURNED);
our ($TR_REASON, $TR_COMPLETED, $TR_SENT);
our ($GATE_REASON, $GATE_COMPLETED, $GATE_WAITING, $GATE_RETURNED);
package main;

# An http app that, for /none, RETURNS without ever sending a response. The
# server must treat an incomplete response (no http.response.start) as a protocol
# error and synthesize a 500, rather than dropping the connection with no status
# line at all (which leaves the client with a bare connection close).
#
# /half and /half-cl RETURN after starting a response but never sending its
# terminal framing (no final chunk / short of the declared Content-Length).
# Per the PAGI spec this is an abnormal end: the connection must close without
# synthesizing a terminator, must not be kept alive, and must report
# server_error through on_disconnect rather than on_complete.
my $app = async sub {
    my ($scope, $receive, $send) = @_;
    die "Unsupported scope type: $scope->{type}" if $scope->{type} ne 'http';

    my $path = $scope->{path} // '/';

    # /cancel-before-start registers its callbacks and raises its flag BEFORE
    # the body loop below, because the whole point is that the client closes
    # while the app is still parked in that loop.
    if ($path eq '/cancel-before-start') {
        my $conn = $scope->{'pagi.connection'};
        $conn->on_disconnect(sub { $Inc::CANCEL_REASON = $_[0] });
        $conn->on_complete(sub { $Inc::CANCEL_COMPLETED = 1 });
        $Inc::CANCEL_WAITING = 1;
    }

    while (1) {
        my $e = await $receive->();
        last if $e->{type} ne 'http.request';
        last unless $e->{more};
    }

    if ($path eq '/cancel-before-start') {
        $Inc::CANCEL_RETURNED = 1;
        return;   # no response ever started -- the client is already gone
    }

    # /gate-disconnect: unlike /cancel-before-start (which stalls mid-body-receipt,
    # before the request itself is even fully read), this request is ordinary and
    # complete -- the top receive() loop above has already consumed it. The app
    # then makes a SECOND receive() call that explicitly parks on http.disconnect,
    # and only returns (bare -- no send() at all) once that event arrives. Learning
    # of the disconnect this way is itself the server's own machinery:
    # _handle_disconnect_and_close sets {closed} = 1 *before* completing this
    # pending receive() (see Connection.pm), so by the time the app resumes and
    # returns, the closed-check has already been satisfied. No race is possible.
    if ($path eq '/gate-disconnect') {
        my $conn = $scope->{'pagi.connection'};
        $conn->on_disconnect(sub { $Inc::GATE_REASON = $_[0] });
        $conn->on_complete(sub { $Inc::GATE_COMPLETED = 1 });
        $Inc::GATE_WAITING = 1;
        while (1) {
            my $e = await $receive->();
            last if $e->{type} eq 'http.disconnect';
        }
        $Inc::GATE_RETURNED = 1;
        return;   # RETURNS BARE: no response events were ever sent
    }

    if ($path eq '/throw-none') {
        my $conn = $scope->{'pagi.connection'};
        $conn->on_disconnect(sub { $Inc::THROW_REASON = $_[0] });
        $conn->on_complete(sub { $Inc::THROW_COMPLETED = 1 });
        die "boom before start\n";
    }

    if ($path eq '/trailers-promised') {
        my $conn = $scope->{'pagi.connection'};
        $conn->on_disconnect(sub { $Inc::TR_REASON = $_[0] });
        $conn->on_complete(sub { $Inc::TR_COMPLETED = 1 });
        # No content-length => chunked framing. Declaring trailers defers the
        # chunked terminator to the trailers write, so returning without the
        # trailers event leaves the body unterminated on the wire.
        await $send->({ type => 'http.response.start', status => 200,
                         trailers => 1,
                         headers  => [['content-type', 'text/plain']] });
        await $send->({ type => 'http.response.body', body => 'promised', more => 0 });
        $Inc::TR_SENT = 1;
        return;   # the promised trailers event never comes
    }

    return if $path eq '/none';

    if ($path eq '/half') {
        my $conn = $scope->{'pagi.connection'};
        $conn->on_disconnect(sub { $Inc::REASON = $_[0] });
        $conn->on_complete(sub { $Inc::COMPLETED = 1 });
        await $send->({ type => 'http.response.start', status => 200,
                         headers => [['content-type', 'text/plain']] });
        await $send->({ type => 'http.response.body', body => 'partial', more => 1 });
        return;
    }

    if ($path eq '/half-cl') {
        my $conn = $scope->{'pagi.connection'};
        $conn->on_disconnect(sub { $Inc::CL_REASON = $_[0] });
        $conn->on_complete(sub { $Inc::CL_COMPLETED = 1 });
        await $send->({ type => 'http.response.start', status => 200,
                         headers => [['content-type', 'text/plain'],
                                     ['content-length', '100']] });
        await $send->({ type => 'http.response.body', body => 'short', more => 1 });
        return;
    }

    if ($path eq '/ok') {
        my $conn = $scope->{'pagi.connection'};
        $conn->on_complete(sub { $Inc::OK_COMPLETED = 1 });
    }

    await $send->({ type => 'http.response.start', status => 200, headers => [['content-type','text/plain']] });
    await $send->({ type => 'http.response.body', body => 'ok', more => 0 });
};

subtest 'an app that returns without a response yields 500' => sub {
    my $loop = IO::Async::Loop->new;

    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };

    my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0, quiet => 1);
    $loop->add($server);
    $server->listen->get;

    my $http = Net::Async::HTTP->new(fail_on_error => 0);
    $loop->add($http);
    my $base = 'http://127.0.0.1:' . $server->port;

    my $ok = $http->GET("$base/ok")->get;
    is($ok->code, 200, '/ok returns 200 (sanity)');

    my $resp = eval { $http->GET("$base/none")->get };
    ok($resp, 'got an HTTP response (server did not just drop the connection)')
        or diag("GET /none failed: $@");
    is($resp->code, 500, 'an incomplete response is turned into a 500') if $resp;

    ok(
        (scalar grep { /without starting a response/i } @warnings),
        'the incomplete response is logged'
    ) or diag("warnings: @warnings");

    $server->shutdown->get;
    $loop->remove($http);
    $loop->remove($server);
};

# Raw-socket helpers (boilerplate lifted from t/53-trailers-framing.t's
# $raw_request): read from a non-blocking socket, pumping the shared loop,
# until the peer closes (sysread returns 0) or a deadline passes.
my $read_until_eof = sub {
    my ($loop, $sock) = @_;
    $sock->blocking(0);
    my $response = '';
    my $deadline = time + 5;
    while (time < $deadline) {
        my $buf;
        my $n = sysread($sock, $buf, 4096);
        if (defined $n) {
            last if $n == 0;   # EOF: server closed the connection
            $response .= $buf;
        }
        $loop->loop_once(0.1);
    }
    return $response;
};

subtest '/half: chunked response left incomplete forces abnormal closure' => sub {
    my $loop = IO::Async::Loop->new;

    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };

    my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0, quiet => 1);
    $loop->add($server);
    $server->listen->get;
    my $port = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1',
        PeerPort => $port,
        Proto    => 'tcp',
        Timeout  => 5,
    ) or die "connect failed: $!";

    # Deliberately HTTP/1.1 with no "Connection: close" -- the client wants
    # keep-alive by default. The server must veto that anyway.
    print $sock "GET /half HTTP/1.1\r\n";
    print $sock "Host: 127.0.0.1:$port\r\n";
    print $sock "\r\n";

    my $response = $read_until_eof->($loop, $sock);

    like($response, qr/^HTTP\/1\.1 200/, '/half: status line 200');
    like($response, qr/Transfer-Encoding:\s*chunked/i, '/half: chunked framing');

    # Exact tail bytes: the first chunk ("partial", 7 bytes) with NO final
    # "0\r\n\r\n" terminator -- the truncation must be observable on the wire.
    like($response, qr/\r\n\r\n7\r\npartial\r\n\z/, '/half: response ends with the first chunk, no terminator');
    unlike($response, qr/0\r\n\r\n\z/, '/half: no chunked terminator was synthesized');

    ok(
        (scalar grep { /returned with an incomplete response/i } @warnings),
        'the incomplete response is logged'
    ) or diag("warnings: @warnings");

    # Let any deferred callback processing settle.
    $loop->loop_once(0.1) for 1..3;

    is($Inc::REASON, 'server_error', '/half: on_disconnect fired with server_error');
    ok(!$Inc::COMPLETED, '/half: on_complete never fired');

    # Keep-alive veto: the same socket gets no further response.
    {
        local $SIG{PIPE} = 'IGNORE';
        print $sock "GET /ok HTTP/1.1\r\n";
        print $sock "Host: 127.0.0.1:$port\r\n";
        print $sock "\r\n";
    }
    my $second = $read_until_eof->($loop, $sock);
    is($second, '', '/half: keep-alive vetoed -- no response to a second request on the same socket');

    close $sock;
    $server->shutdown->get;
    $loop->remove($server);
};

subtest '/half-cl: content-length response left short forces abnormal closure' => sub {
    my $loop = IO::Async::Loop->new;

    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };

    my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0, quiet => 1);
    $loop->add($server);
    $server->listen->get;
    my $port = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1',
        PeerPort => $port,
        Proto    => 'tcp',
        Timeout  => 5,
    ) or die "connect failed: $!";

    print $sock "GET /half-cl HTTP/1.1\r\n";
    print $sock "Host: 127.0.0.1:$port\r\n";
    print $sock "\r\n";

    my $response = $read_until_eof->($loop, $sock);

    like($response, qr/^HTTP\/1\.1 200/, '/half-cl: status line 200');
    like($response, qr/Content-Length:\s*100/i, '/half-cl: declared content-length 100');
    unlike($response, qr/Transfer-Encoding/i, '/half-cl: not chunked');

    # Exact tail bytes: only the 5-byte body actually sent, well short of the
    # declared 100.
    like($response, qr/\r\n\r\nshort\z/, '/half-cl: response ends with the short body, nothing more');

    ok(
        (scalar grep { /returned with an incomplete response/i } @warnings),
        'the incomplete response is logged'
    ) or diag("warnings: @warnings");

    $loop->loop_once(0.1) for 1..3;

    is($Inc::CL_REASON, 'server_error', '/half-cl: on_disconnect fired with server_error');
    ok(!$Inc::CL_COMPLETED, '/half-cl: on_complete never fired');

    close $sock;
    $server->shutdown->get;
    $loop->remove($server);
};

subtest '/throw-none: app failure before start yields 500 and server_error' => sub {
    my $loop = IO::Async::Loop->new;

    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };

    my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0, quiet => 1);
    $loop->add($server);
    $server->listen->get;

    my $http = Net::Async::HTTP->new(fail_on_error => 0);
    $loop->add($http);

    my $resp = eval { $http->GET('http://127.0.0.1:' . $server->port . '/throw-none')->get };
    ok($resp, 'got an HTTP response (server did not just drop the connection)')
        or diag("GET /throw-none failed: $@");
    is($resp->code, 500, 'an app failure before start is turned into a 500') if $resp;

    ok((scalar grep { /PAGI application error: boom before start/ } @warnings),
        'the application error is logged') or diag("warnings: @warnings");

    $loop->loop_once(0.1) for 1..3;

    is($Inc::THROW_REASON, 'server_error',
        '/throw-none: on_disconnect fired with server_error');
    ok(!$Inc::THROW_COMPLETED, '/throw-none: on_complete never fired');

    $server->shutdown->get;
    $loop->remove($http);
    $loop->remove($server);
};

# Spec carve-out: when the client is already gone, an app that returns
# without a response is not a protocol error to report -- there is nobody to
# send a 500 to and nothing to log. The h2 twin lives in
# t/http2/30-connection-state.t.
subtest '/cancel-before-start: client gone before start synthesizes nothing' => sub {
    my $loop = IO::Async::Loop->new;

    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };

    my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0, quiet => 1);
    $loop->add($server);
    $server->listen->get;
    my $port = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1',
        PeerPort => $port,
        Proto    => 'tcp',
        Timeout  => 5,
    ) or die "connect failed: $!";

    # Announce a body and never send it: the app parks in its receive loop
    # with no response started.
    print $sock "POST /cancel-before-start HTTP/1.1\r\n";
    print $sock "Host: 127.0.0.1:$port\r\n";
    print $sock "Content-Length: 10\r\n";
    print $sock "\r\n";

    for (1 .. 100) {
        last if $Inc::CANCEL_WAITING;
        $loop->loop_once(0.05);
    }
    ok($Inc::CANCEL_WAITING, 'app is parked in receive() with no response started');

    # Half-close: the server sees the client go away, but this side can still
    # read, so a synthesized 500 would be observable if one were sent.
    shutdown($sock, 1) or die "shutdown: $!";

    my $response = $read_until_eof->($loop, $sock);

    ok($Inc::CANCEL_RETURNED, 'app returned after the client went away');
    is($response, '', 'no 500 (no bytes at all) was sent to the departed client');
    ok(!(scalar grep { /without starting a response/i } @warnings),
        'no incomplete-response warning for a client that already disconnected')
        or diag("warnings: @warnings");

    is($Inc::CANCEL_REASON, 'client_closed',
        '/cancel-before-start: on_disconnect fired with client_closed');
    ok(!$Inc::CANCEL_COMPLETED, '/cancel-before-start: on_complete never fired');

    close $sock;
    $server->shutdown->get;
    $loop->remove($server);
};

# Spec carve-out, again, but for the OTHER order: here the request is fully
# received before the client goes away (unlike /cancel-before-start, which
# stalls mid-body-receipt). Www.pod's "Application Produced No Response" /
# "Application Left a Response Incomplete" sections both call this out as not
# an application error once the client is already gone: no 500, no error log,
# and the disconnect reason stays the client's own (client_closed), not
# server_error.
subtest '/gate-disconnect: client gone after a complete request, app returns bare -- no 500, no app-error log' => sub {
    my $loop = IO::Async::Loop->new;

    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };

    my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0, quiet => 1);
    $loop->add($server);
    $server->listen->get;
    my $port = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1',
        PeerPort => $port,
        Proto    => 'tcp',
        Timeout  => 5,
    ) or die "connect failed: $!";
    $sock->blocking(0);

    # An ordinary, COMPLETE GET -- nothing is left outstanding on the read
    # side, so the app's top receive() loop finishes on its own; the app then
    # parks in a second receive() call explicitly awaiting http.disconnect.
    print $sock "GET /gate-disconnect HTTP/1.1\r\n";
    print $sock "Host: 127.0.0.1:$port\r\n";
    print $sock "\r\n";

    my $wait_deadline = time + 5;
    while (time < $wait_deadline) {
        last if $Inc::GATE_WAITING;
        $loop->loop_once(0.05);
    }
    ok($Inc::GATE_WAITING, 'app is parked awaiting http.disconnect with no response ever started');

    # Sanity check, taken before the socket is closed: nothing has been
    # written to the client yet (the app hasn't called send() at all).
    my $pre_close_bytes = '';
    {
        my $buf;
        my $n = sysread($sock, $buf, 4096);
        $pre_close_bytes = $buf if defined $n && $n > 0;
    }
    is($pre_close_bytes, '', 'nothing written to the client before it closes its socket');

    # Full close: the client goes away for good. Nothing more can be read
    # from this socket afterward, so what follows asserts server-side state
    # (warnings / app-error log / connection state) rather than wire bytes.
    close $sock;

    # Gate the assertions on the disconnect actually having been PROCESSED --
    # not a fixed sleep budget -- by polling connection state (the on_disconnect
    # reason) until it lands or a deadline passes.
    my $process_deadline = time + 5;
    while (time < $process_deadline) {
        last if defined $Inc::GATE_REASON;
        $loop->loop_once(0.05);
    }

    ok($Inc::GATE_RETURNED, 'app observed the disconnect via receive() and returned bare');
    is($Inc::GATE_REASON, 'client_closed',
        'connection state reports the CLIENT\'s own disconnect reason, not server_error');
    ok(!$Inc::GATE_COMPLETED, 'on_complete never fired');

    ok(!(scalar grep { /without starting a response/i } @warnings),
        'no "Application Produced No Response" warning for an already-disconnected client')
        or diag("warnings: @warnings");
    ok(!(scalar grep { /incomplete response/i } @warnings),
        'no "Application Left a Response Incomplete" warning either')
        or diag("warnings: @warnings");
    ok(!(scalar grep { /PAGI application error/i } @warnings),
        'no app-error log at all')
        or diag("warnings: @warnings");

    # The server survives: a fresh connection still gets served normally.
    my $http = Net::Async::HTTP->new(fail_on_error => 0);
    $loop->add($http);
    my $ok = $http->GET("http://127.0.0.1:$port/ok")->get;
    is($ok->code, 200, 'server survives -- next request on a fresh connection works');
    $loop->remove($http);

    $server->shutdown->get;
    $loop->remove($server);
};

# Design section 15.3: a promised-but-unsent trailers event counts as an
# incomplete response. On h1 the declared trailers defer the chunked
# terminator to the trailers write, so the truncation is visible on the wire.
# The h2 twin is t/http2/24-incomplete-response.t's /promised-trailers.
subtest '/trailers-promised: declared trailers never sent is an incomplete response' => sub {
    my $loop = IO::Async::Loop->new;

    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };

    my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0, quiet => 1);
    $loop->add($server);
    $server->listen->get;
    my $port = $server->port;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1',
        PeerPort => $port,
        Proto    => 'tcp',
        Timeout  => 5,
    ) or die "connect failed: $!";

    print $sock "GET /trailers-promised HTTP/1.1\r\n";
    print $sock "Host: 127.0.0.1:$port\r\n";
    print $sock "\r\n";

    my $response = $read_until_eof->($loop, $sock);

    ok($Inc::TR_SENT, 'app sent the terminal body and returned');
    like($response, qr/^HTTP\/1\.1 200/, '/trailers-promised: status line 200');
    like($response, qr/Transfer-Encoding:\s*chunked/i, '/trailers-promised: chunked framing');
    like($response, qr/\r\n\r\n8\r\npromised\r\n\z/,
        '/trailers-promised: ends with the body chunk -- no terminator, no trailer section');
    unlike($response, qr/0\r\n\r\n\z/,
        '/trailers-promised: no chunked terminator was synthesized');

    ok((scalar grep { /returned with an incomplete response/i } @warnings),
        'the incomplete response is logged') or diag("warnings: @warnings");

    $loop->loop_once(0.1) for 1..3;

    is($Inc::TR_REASON, 'server_error',
        '/trailers-promised: on_disconnect fired with server_error');
    ok(!$Inc::TR_COMPLETED, '/trailers-promised: on_complete never fired');

    close $sock;
    $server->shutdown->get;
    $loop->remove($server);
};

subtest '/ok control: unaffected -- on_complete fires, connection completes normally' => sub {
    my $loop = IO::Async::Loop->new;

    my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0, quiet => 1);
    $loop->add($server);
    $server->listen->get;

    my $http = Net::Async::HTTP->new(fail_on_error => 0);
    $loop->add($http);
    my $base = 'http://127.0.0.1:' . $server->port;

    my $ok = $http->GET("$base/ok")->get;
    is($ok->code, 200, '/ok returns 200');
    is($ok->content, 'ok', '/ok body delivered');

    $loop->loop_once(0.1) for 1..3;
    ok($Inc::OK_COMPLETED, '/ok: on_complete fired');

    $server->shutdown->get;
    $loop->remove($http);
    $loop->remove($server);
};

done_testing;
