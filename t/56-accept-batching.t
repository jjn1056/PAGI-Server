use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
use FindBin;
use lib "$FindBin::Bin/../lib";

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

use PAGI::Server::Listener;

# Contract: a storm of pending connections drains in bounded batches per
# readiness event instead of one per event (the stock IO::Async::Listener
# behavior, which caps the accept rate at the event-loop iteration rate).

my $loop = IO::Async::Loop->new;

my @streams;
my $listener = PAGI::Server::Listener->new(
    on_stream => sub { push @streams, $_[1] },
);
$loop->add($listener);
$listener->listen(
    addr => {
        family => 'inet', socktype => 'stream', ip => '127.0.0.1', port => 0,
    },
    # Room for the whole storm below (the kernel clamps to somaxconn, which
    # is >= 128 everywhere this suite runs)
    queuesize => 128,
)->get;
my $port = $listener->read_handle->sockport;

# Open 100 connections without running the loop: the kernel completes the
# handshakes into the accept queue (100 < the somaxconn clamp).
my @clients = map {
    IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp')
        or die "connect: $!";
} 1 .. 100;

$loop->loop_once(0.1);   # one readiness dispatch
is(scalar @streams, PAGI::Server::Listener::ACCEPT_BATCH,
    'first readiness event accepted a full batch (fairness bound respected)');

$loop->loop_once(0.1);
is(scalar @streams, 100, 'second event drained the remainder');

$loop->loop_once(0.1);
is(scalar @streams, 100, 'no phantom accepts once the queue is empty');

done_testing;
