use strict;
use warnings;
use Test2::V0;
use Future::AsyncAwait;
use IO::Async::Loop;
use IO::Socket::INET;
use FindBin;
use lib "$FindBin::Bin/../lib";

use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# The connection maintains its own outbound byte count (_stream_write adds,
# the shared on_write callback subtracts) so buffered_amount and the
# backpressure checks never walk IO::Async::Stream's internal write queue.
# Invariant: the counter equals what a walk of the real queue reports, at
# every observation point, including a forced backlog and the final drain.
# The walk below intentionally duplicates the old internals-reach: tests may
# depend on IO::Async internals so the library doesn't have to.

sub walk_writequeue {
    my ($stream) = @_;
    my $total = 0;
    for my $writer (@{ $stream->{writequeue} // [] }) {
        my $data = $writer->data;
        $total += length($data) if defined $data && !ref $data;
    }
    return $total;
}

my $loop = IO::Async::Loop->new;

my $body = 'x' x (512 * 1024);   # far beyond the kernel socket buffer

my $app = async sub {
    my ($scope, $receive, $send) = @_;
    die "Unsupported scope type: $scope->{type}\n" if $scope->{type} ne 'http';

    await $send->({
        type    => 'http.response.start',
        status  => 200,
        headers => [ [ 'content-type', 'application/octet-stream' ],
                     [ 'content-length', length $body ] ],
    });
    await $send->({ type => 'http.response.body', body => $body, more => 0 });
};

my $server = PAGI::Server->new(
    app => $app, host => '127.0.0.1', port => 0, quiet => 1,
);
$loop->add($server);
$server->listen->get;
my $port = $server->port;

# Raw client that sends the request but reads nothing yet, forcing the
# response to back up in the kernel buffer and then the userspace queue.
my $client = IO::Socket::INET->new(
    PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp',
) or die "connect: $!";
$client->blocking(0);
syswrite($client, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");

my ($conn) = values %{ $server->{connections} };
my $mismatches = 0;
my $saw_backlog = 0;

for (1 .. 40) {
    $loop->loop_once(0.02);
    $conn //= (values %{ $server->{connections} })[0];
    next unless $conn && $conn->{stream};
    my $walk    = walk_writequeue($conn->{stream});
    my $counter = $conn->_get_write_buffer_size;
    $mismatches++ if $walk != $counter;
    $saw_backlog = 1 if $counter > 0;
}

ok($saw_backlog, 'unread client forced a userspace write backlog');
is($mismatches, 0, 'counter matched a live walk of the write queue at every tick');

# Drain: read the whole response, then the queue and the counter must both
# reach exactly zero.
my $received = 0;
my $deadline = time + 10;
while ($received < length($body) && time < $deadline) {
    $loop->loop_once(0.02);
    while ((my $n = sysread($client, my $buf, 65536) // 0) > 0) {
        $received += $n;
    }
}

for (1 .. 20) { $loop->loop_once(0.02) }

ok($received >= length($body), 'client eventually received the full body');
is(walk_writequeue($conn->{stream}), 0, 'write queue fully drained');
is($conn->_get_write_buffer_size, 0, 'byte counter returned to exactly zero');

close $client;
$server->shutdown->get;
$loop->remove($server);

done_testing;
