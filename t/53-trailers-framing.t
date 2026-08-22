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

# h1 trailers on non-chunked responses fail loudly (Phase-1 carry M6). Today
# the h1 send closure silently no-ops http.response.trailers when the
# response used content-length framing (return unless $chunked), AFTER the
# sequence machine already recorded 'complete' -- lying to the app that
# trailers went out. Three paths, one app, routed by path:
#
#   /cl-trailers      content-length framing + trailers=>1: the trailers
#                      send must fail the Future (RFC 7230 -- trailers ride
#                      chunked framing only), and the body already delivered
#                      via Content-Length must still reach the client intact.
#   /chunked-trailers  control: chunked framing + trailers=>1: trailers must
#                      still be transmitted on the wire, unchanged from
#                      before this fix.
#   /head-trailers     HEAD request against the same trailers-declaring app:
#                      accept-and-discard, per PAGI Www.pod's HEAD rule --
#                      empty body, no error, connection stays usable.
my $app = async sub {
    my ($scope, $receive, $send) = @_;
    die "unsupported scope" if $scope->{type} ne 'http';
    while (1) {
        my $e = await $receive->();
        last if $e->{type} ne 'http.request' || !$e->{more};
    }
    my $path = $scope->{path} // '/';

    if ($path eq '/cl-trailers') {
        # Content-length framing: the body is fully framed by Content-Length
        # already, so there is no place left to carry trailers.
        await $send->({ type => 'http.response.start', status => 200,
                         trailers => 1,
                         headers => [['content-type', 'text/plain'],
                                     ['content-length', '2']] });
        await $send->({ type => 'http.response.body', body => 'ok', more => 0 });
        # CROSS-PHASE NOTE (Phase 3, incomplete-response work): the die in
        # the send closure fires before advance_http would have recorded
        # 'complete', so it rolls the machine's $seq back to
        # 'awaiting_trailers' instead. This app just captures the error and
        # returns -- the machine state is left at 'awaiting_trailers'
        # un-inspected. Phase 3 should decide whether a send loop that dies
        # here (app returns without another send attempt) needs any further
        # handling, or whether "app returned with $seq != complete" is
        # already covered by existing incomplete-response rules.
        my $err = do { local $@; eval { await $send->({ type => 'http.response.trailers', headers => [['x-t', '1']] }) }; $@ };
        $T::CL_ERR = $err;
        return;
    }

    if ($path eq '/chunked-trailers' || $path eq '/head-trailers') {
        # No content-length header => chunked framing; trailers ride along.
        await $send->({ type => 'http.response.start', status => 200,
                         trailers => 1,
                         headers => [['content-type', 'text/plain']] });
        await $send->({ type => 'http.response.body', body => 'ok', more => 0 });
        await $send->({ type => 'http.response.trailers', headers => [['x-t', '1']] });
        return;
    }

    # /still-sane: plain control response, used to prove a pooled connection
    # is still usable after a HEAD+trailers request on it.
    await $send->({ type => 'http.response.start', status => 200,
                     headers => [['content-type', 'text/plain']] });
    await $send->({ type => 'http.response.body', body => 'sane', more => 0 });
};

my $loop = IO::Async::Loop->new;
my $server = PAGI::Server->new(
    app => $app, host => '127.0.0.1', port => 0, quiet => 1,
);
$loop->add($server);
$server->listen->get;
my $port = $server->port;

my $http = Net::Async::HTTP->new;
$loop->add($http);

# --- /cl-trailers: content-length response, trailers fail loudly -----------

my $cl_res = $http->GET("http://127.0.0.1:$port/cl-trailers")->get;
is( $cl_res->content, 'ok',
    '/cl-trailers still delivers the content-length-framed body' );
like( $T::CL_ERR, qr/requires chunked framing/,
    'trailers on content-length response fail the Future' );

# --- /chunked-trailers: control -- raw socket, trailers actually on the wire

# Net::Async::HTTP doesn't expose trailers (see t/02-streaming.t), so read
# the raw chunked framing off the wire ourselves. Socket-pump idiom lifted
# from t/52-mandatory-validation.t's $ws_handshake_and_drain; the chunk
# grammar being asserted against (hex chunk-size line, CRLF-terminated
# chunks, "0\r\n" + trailer headers + CRLF as the terminator) is the same
# grammar t/16-chunked-validation.t exercises against the parser.
my $raw_request = sub {
    my ($port, $method, $path) = @_;
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1',
        PeerPort => $port,
        Proto    => 'tcp',
        Timeout  => 5,
    ) or return undef;

    print $sock "$method $path HTTP/1.1\r\n";
    print $sock "Host: 127.0.0.1:$port\r\n";
    print $sock "Connection: close\r\n";
    print $sock "\r\n";

    $sock->blocking(0);
    my $response = '';
    my $deadline = time + 5;
    while (time < $deadline) {
        my $buf;
        my $n = sysread($sock, $buf, 4096);
        if (defined $n) {
            last if $n == 0;   # EOF: server closed after Connection: close
            $response .= $buf;
        }
        $loop->loop_once(0.1);
    }
    close $sock;
    return $response;
};

my $chunked_raw = $raw_request->($port, 'GET', '/chunked-trailers');
ok( defined $chunked_raw, '/chunked-trailers: connected' ) or diag('connection failed');
like( $chunked_raw, qr/Transfer-Encoding:\s*chunked/i,
    '/chunked-trailers uses chunked framing' );
like( $chunked_raw, qr/\r\n2\r\nok\r\n/,
    '/chunked-trailers body chunk "ok" present' );
like( $chunked_raw, qr/0\r\nx-t: 1\r\n\r\n/,
    '/chunked-trailers trailer header actually transmitted after the final chunk (control, unchanged)' );

# --- /head-trailers: HEAD accept-and-discard --------------------------------

my $head_res = $http->HEAD("http://127.0.0.1:$port/head-trailers")->get;
is( $head_res->code, 200, '/head-trailers HEAD response status is 200' );
is( $head_res->content, '', '/head-trailers HEAD response body is empty' );

# Connection sane: the pooled client can still make a normal request after.
my $sane_res = $http->GET("http://127.0.0.1:$port/still-sane")->get;
is( $sane_res->content, 'sane', 'connection remains usable after HEAD+trailers' );

$server->shutdown->get;

done_testing;
