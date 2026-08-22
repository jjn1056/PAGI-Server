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

    while (1) {
        my $e = await $receive->();
        last if $e->{type} ne 'http.request';
        last unless $e->{more};
    }

    my $path = $scope->{path} // '/';

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
