use strict;
use warnings;
use Test2::V0;
use Future::AsyncAwait;
use IO::Async::Loop;
use Errno qw(EAGAIN);
use Time::HiRes qw(time);
use FindBin;
use lib "$FindBin::Bin/../../lib";

use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    eval { require Net::Async::WebSocket::Client; 1 }
        or plan skip_all => 'Net::Async::WebSocket::Client required';
}

# Force a real backlog after the handshake, then let it drain to the client.
# A large send alone need not engage backpressure: immediate writes may flush
# it before watermark evaluation (PAGI transport-flow-control clarification).

my $loop = IO::Async::Loop->new;

my ($hit_high, $hit_drain, $second_sent) = (0, 0, 0);
my $allow_send = $loop->new_future;
my @frames;
my $transport;

my $app = async sub {
    my ($scope, $receive, $send) = @_;

    if ($scope->{type} eq 'lifespan') {
        while (1) {
            my $e = await $receive->();
            if    ($e->{type} eq 'lifespan.startup')  { await $send->({ type => 'lifespan.startup.complete' }); }
            elsif ($e->{type} eq 'lifespan.shutdown') { await $send->({ type => 'lifespan.shutdown.complete' }); last; }
        }
        return;
    }

    return unless $scope->{type} eq 'websocket';

    await $send->({ type => 'websocket.accept' });

    $transport = $scope->{'pagi.transport'};
    $transport->on_high_water(sub { $hit_high++ });
    $transport->on_drain(sub     { $hit_drain++ });

    await $allow_send;

    # 50 KB: over the 1 KB high-water mark configured below, but under the
    # ~64 KB WebSocket frame-size limit.
    await $send->({ type => 'websocket.send', bytes => ('x' x (50 * 1024)) });
    await $send->({ type => 'websocket.send', bytes => 'after drain' });
    $second_sent = 1;

    while (1) {
        my $e = await $receive->();
        last if $e->{type} eq 'websocket.disconnect';
    }
};

my $server = PAGI::Server->new(
    app => $app, host => '127.0.0.1', port => 0, quiet => 1,
    write_high_watermark => 1024,
    write_low_watermark  => 256,
);
$loop->add($server);
$server->listen->get;
my $port = $server->port;

# A client that reads incoming frames (so the server's write buffer drains).
my $client = Net::Async::WebSocket::Client->new(on_binary_frame => sub { push @frames, $_[1] });
$loop->add($client);
$client->connect(url => "ws://127.0.0.1:$port/")->get;

my ($conn) = values %{$server->{connections}};
my $blocked = 1;
$conn->{stream}->configure(writer => sub {
    if ($blocked) { $! = EAGAIN; return undef; }
    my $n = syswrite($_[1], $_[2], $_[3]);
    substr($_[2], 0, $n) = '' if defined $n;
    return $n;
});
$allow_send->done;

is($hit_high, 1, 'blocked output fires on_high_water once');
is($hit_drain, 0, 'no drain while bytes remain blocked');
ok(!$second_sent, 'next application send waits for drain');
ok($transport->buffered_amount >= $transport->high_water_mark, 'actual output backlog exceeds high mark');

$blocked = 0;
my $deadline = time + 3;
$loop->loop_once(0.02) while (@frames < 2 || !$hit_drain) && time < $deadline;

is($hit_drain, 1, 'draining the backlog fires on_drain once');
ok($second_sent, 'parked application send resumes');
is(\@frames, [('x' x (50 * 1024)), 'after drain'], 'client receives both complete frames in order');

eval { $client->close };
eval { $loop->remove($client) };
$server->shutdown->get;
$loop->remove($server);

done_testing;
