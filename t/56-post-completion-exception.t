use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";

use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# Www.pod:1156-1165 -- "Delivery defines completion": once the terminal
# response event has gone out, the response is complete regardless of what
# the application does afterward. An app that throws AFTER delivering a
# complete response must not be treated as an abnormal disconnect: on_complete
# already fired (or fires here), on_disconnect must NOT fire, and
# disconnect_reason() stays undef. The server still logs (SHOULD) and still
# closes the connection (MAY) -- h2 already behaves this way
# (Connection.pm:826-830, marked complete by _h2_on_close before the app's
# exception is even observed).

package Post;
our ($CS, $COMPLETED, $DISCONNECT_REASON, $DISCONNECT_FIRED);
package main;

my $app = async sub {
    my ($scope, $receive, $send) = @_;
    die "Unsupported scope type: $scope->{type}" if $scope->{type} ne 'http';

    my $conn = $scope->{'pagi.connection'};
    $Post::CS = $conn;
    $conn->on_complete(sub { $Post::COMPLETED = 1 });
    $conn->on_disconnect(sub { $Post::DISCONNECT_FIRED = 1; $Post::DISCONNECT_REASON = $_[0] });

    await $send->({ type => 'http.response.start', status => 200,
                     headers => [['content-type', 'text/plain'], ['content-length', '2']] });
    await $send->({ type => 'http.response.body', body => 'ok', more => 0 });

    die "post-completion boom\n";
};

# Raw-socket helper (lifted from t/http-incomplete-response.t): read from a
# non-blocking socket, pumping the shared loop, until the peer closes
# (sysread returns 0) or a deadline passes.
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

subtest 'app throwing after a complete h1 response: on_complete fires, on_disconnect does not' => sub {
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

    print $sock "GET / HTTP/1.1\r\n";
    print $sock "Host: 127.0.0.1:$port\r\n";
    print $sock "\r\n";

    my $response = $read_until_eof->($loop, $sock);

    # The bytes on the wire are unaffected: the response was complete before
    # the app threw.
    like($response, qr/^HTTP\/1\.1 200/, 'the complete response was delivered');
    like($response, qr/\r\n\r\nok\z/, 'the full body was delivered');

    ok((scalar grep { /PAGI application error \(after response complete\)/ } @warnings),
        'the post-completion exception is logged with the completion-specific text')
        or diag("warnings: @warnings");
    ok((scalar grep { /post-completion boom/ } @warnings),
        'the logged warning includes the actual exception text')
        or diag("warnings: @warnings");

    $loop->loop_once(0.1) for 1..3;

    ok($Post::CS, 'app captured a connection_state');
    is($Post::COMPLETED, 1, 'on_complete fired (the response did complete before the throw)');
    is($Post::DISCONNECT_FIRED, undef, 'on_disconnect did NOT fire');
    is($Post::CS->disconnect_reason, undef, 'disconnect_reason() stays undef');

    # MAY close (Www.pod:1156-1165): the server still closes the connection
    # after a post-completion exception -- a second request on the same
    # socket gets nothing back.
    print $sock "GET / HTTP/1.1\r\n";
    print $sock "Host: 127.0.0.1:$port\r\n";
    print $sock "\r\n";
    my $second = $read_until_eof->($loop, $sock);
    is($second, '', 'a second request on the same (already-closed) socket gets nothing');

    close $sock;
    $server->shutdown->get;
    $loop->remove($server);
};

done_testing;
