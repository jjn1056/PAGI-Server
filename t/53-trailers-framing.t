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
#                      The app then returns without another send attempt, so
#                      per Phase 3 Task 2 the connection closes abnormally
#                      (server_error, no keep-alive) even though the body
#                      was fully delivered.
#   /chunked-trailers  control: chunked framing + trailers=>1: trailers must
#                      still be transmitted on the wire, unchanged from
#                      before this fix.
#   /head-trailers     HEAD request against the same trailers-declaring app:
#                      accept-and-discard, per PAGI Www.pod's HEAD rule --
#                      empty body, no error, connection stays usable.
#   /empty-trailers    chunked framing + trailers=>1, terminal body, then
#                      http.response.trailers with headers=>[] (explicit
#                      empty list): the terminator on the wire must be a
#                      bare "0\r\n\r\n" -- identical to the no-trailers
#                      chunked end -- on_complete must fire, and the
#                      connection must stay keep-alive-usable.
#   /absent-trailers   same as /empty-trailers but the trailers event omits
#                      the 'headers' key entirely. _validate_http_response_trailers
#                      treats headers as OPTIONAL and the h1 writer defaults
#                      via `$event->{headers} // []`, so this must produce
#                      byte-identical wire output to /empty-trailers.
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
        my $conn = $scope->{'pagi.connection'};
        $conn->on_disconnect(sub { $T::CL_DISC_REASON = $_[0] });
        await $send->({ type => 'http.response.start', status => 200,
                         trailers => 1,
                         headers => [['content-type', 'text/plain'],
                                     ['content-length', '2']] });
        await $send->({ type => 'http.response.body', body => 'ok', more => 0 });
        # The die in the send closure fires before advance_http would have
        # recorded 'complete', so it rolls the machine's $seq back to
        # 'awaiting_trailers' instead. This app just captures the error and
        # returns without another send attempt. Per Phase 3 Task 2, the
        # app-return path treats a started-but-not-'complete' $seq as an
        # incomplete response: it forces an abnormal closure (server_error,
        # no keep-alive) even though the Content-Length-framed body above
        # was already delivered in full.
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

    if ($path eq '/empty-trailers' || $path eq '/absent-trailers') {
        my $conn = $scope->{'pagi.connection'};
        $conn->on_complete(sub { $T::EMPTY_TRAILERS_COMPLETED++ });
        await $send->({ type => 'http.response.start', status => 200,
                         trailers => 1,
                         headers => [['content-type', 'text/plain']] });
        await $send->({ type => 'http.response.body', body => 'ok', more => 0 });
        if ($path eq '/empty-trailers') {
            await $send->({ type => 'http.response.trailers', headers => [] });
        }
        else {
            await $send->({ type => 'http.response.trailers' });
        }
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

# --- /cl-trailers: content-length response, trailers fail loudly, and the --
# app-return afterward now closes the connection abnormally (Phase 3 Task 2).
# Raw socket, not $http: we need to observe the connection actually close
# (EOF with no further bytes) rather than have a pooled client's transparent
# reconnect hide it. Deliberately no "Connection: close" request header --
# HTTP/1.1 keep-alive is the client's default; the server must veto it
# anyway. $http itself is left untouched here, so its first use below (for
# /head-trailers) is naturally a fresh connection.
my @cl_warnings;
{
    local $SIG{__WARN__} = sub { push @cl_warnings, $_[0] };

    my $cl_sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1',
        PeerPort => $port,
        Proto    => 'tcp',
        Timeout  => 5,
    ) or die "connect failed: $!";

    print $cl_sock "GET /cl-trailers HTTP/1.1\r\n";
    print $cl_sock "Host: 127.0.0.1:$port\r\n";
    print $cl_sock "\r\n";

    $cl_sock->blocking(0);
    my $cl_response = '';
    my $deadline = time + 5;
    while (time < $deadline) {
        my $buf;
        my $n = sysread($cl_sock, $buf, 4096);
        if (defined $n) {
            last if $n == 0;   # EOF: server closed the connection
            $cl_response .= $buf;
        }
        $loop->loop_once(0.1);
    }
    close $cl_sock;

    like( $cl_response, qr/^HTTP\/1\.1 200/, '/cl-trailers: status line 200' );
    like( $cl_response, qr/\r\n\r\nok\z/,
        '/cl-trailers: content-length-framed body "ok" fully delivered, then EOF (connection closed, no more bytes)' );
}

like( $T::CL_ERR, qr/requires chunked framing/,
    'trailers on content-length response fail the Future' );

$loop->loop_once(0.1) for 1..3;

is( $T::CL_DISC_REASON, 'server_error',
    '/cl-trailers: app-return after the failed trailers send reports on_disconnect(server_error)' );

ok(
    (scalar grep { /returned with an incomplete response/i } @cl_warnings),
    '/cl-trailers: incomplete-response warning logged'
);

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

# --- /empty-trailers, /absent-trailers: empty/absent trailer headers yield --
# the same bare "0\r\n\r\n" terminator as the no-trailers chunked end
# (validator: headers OPTIONAL, empty list validates via a zero-iteration
# loop; h1 writer: `$event->{headers} // []` makes absent and explicit-empty
# byte-identical on the wire).
for my $path (qw(/empty-trailers /absent-trailers)) {
    my $raw = $raw_request->($port, 'GET', $path);
    ok( defined $raw, "$path: connected" ) or diag('connection failed');
    like( $raw, qr/Transfer-Encoding:\s*chunked/i, "$path uses chunked framing" );
    like( $raw, qr/\r\n2\r\nok\r\n0\r\n\r\n\z/,
        "$path terminates with a bare 0\\r\\n\\r\\n (no trailer header lines) immediately after the final chunk" );
}

# --- /head-trailers: HEAD accept-and-discard --------------------------------

my $head_res = $http->HEAD("http://127.0.0.1:$port/head-trailers")->get;
is( $head_res->code, 200, '/head-trailers HEAD response status is 200' );
is( $head_res->content, '', '/head-trailers HEAD response body is empty' );

# Connection sane: the pooled client can still make a normal request after.
my $sane_res = $http->GET("http://127.0.0.1:$port/still-sane")->get;
is( $sane_res->content, 'sane', 'connection remains usable after HEAD+trailers' );

# --- /empty-trailers: keep-alive policy + on_complete, via the pooled client
# (the raw-socket checks above use "Connection: close" to observe exact
# bytes, so the keep-alive path needs a separate pooled-client round trip).

$T::EMPTY_TRAILERS_COMPLETED = 0;
my $empty_res = $http->GET("http://127.0.0.1:$port/empty-trailers")->get;
is( $empty_res->content, 'ok', '/empty-trailers via pooled client: body delivered' );

my $sane_res2 = $http->GET("http://127.0.0.1:$port/still-sane")->get;
is( $sane_res2->content, 'sane',
    'connection remains usable after empty-trailers (keep-alive honored, not treated as an error)' );

is( $T::EMPTY_TRAILERS_COMPLETED, 1, '/empty-trailers: on_complete fired exactly once' );

$server->shutdown->get;

done_testing;
