use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";

eval { require Net::Async::HTTP; 1 }
    or plan skip_all => 'Net::Async::HTTP required';
plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

use PAGI::Server;

# Two PAGI::Server instances in one process must coexist: the second
# server's startup must not disturb the first server's listeners. The
# PAGI_REUSE hot-restart mechanism publishes listener fds in the
# environment; a sibling server collecting that entry as "inherited" and
# reaping it as unmatched closes a live listener out from under the first
# server (and out from under the event loop -- IO::Async::Loop::Epoll
# turns the stale watch into a croak on fd reuse, seen on CPAN smokers).

my $loop = IO::Async::Loop->new;

sub make_server {
    my ($body) = @_;
    my $server = PAGI::Server->new(
        app => async sub {
            my ($scope, $receive, $send) = @_;
            return unless $scope->{type} eq 'http';
            await $receive->();
            await $send->({ type => 'http.response.start', status => 200,
                            headers => [ ['content-type', 'text/plain'],
                                         ['content-length', length $body] ] });
            await $send->({ type => 'http.response.body', body => $body, more => 0 });
        },
        host => '127.0.0.1', port => 0, quiet => 1, shutdown_timeout => 1,
    );
    $loop->add($server);
    $server->listen->get;
    return $server;
}

my $http = Net::Async::HTTP->new;
$loop->add($http);

my $server_one = make_server('one');
my $port_one   = $server_one->port;

is( $http->GET("http://127.0.0.1:$port_one/")->get->content, 'one',
    'first server serves before the second exists' );

my $server_two = make_server('two');
my $port_two   = $server_two->port;

is( $http->GET("http://127.0.0.1:$port_two/")->get->content, 'two',
    'second server serves' );

# The heart of the test: server one must still ACCEPT. A fresh raw
# connection, not the pooled Net::Async::HTTP client -- a pooled keep-alive
# connection rides its already-accepted socket and never touches the
# listener, masking a dead one.
sub fresh_get {
    my ($port) = @_;
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 3,
    ) or return "connect failed: $!";
    print $sock "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n";
    $sock->blocking(0);
    my ($wire, $deadline) = ('', time + 5);
    while (time < $deadline) {
        my $n = sysread($sock, my $c, 4096);
        if    (defined $n && $n > 0) { $wire .= $c }
        elsif (defined $n && $n == 0) { last }
        $loop->loop_once(0.05);
    }
    close $sock;
    my ($body) = $wire =~ /\r\n\r\n(.*)\z/s;
    return $body // "no response body in: $wire";
}

is( fresh_get($port_one), 'one',
    "first server still accepts fresh connections after the second server's startup" );

like( $ENV{PAGI_REUSE} // '', qr/:\Q$port_one\E:/,
    "first server's PAGI_REUSE registration survives the second server's startup" );

$server_two->shutdown->get;

is( fresh_get($port_one), 'one',
    "first server still accepts after the second server's shutdown" );

$server_one->shutdown->get;
$loop->remove($http);

done_testing;
