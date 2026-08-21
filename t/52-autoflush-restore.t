use strict;
use warnings;
use Test2::V0;
use Future::AsyncAwait;
use IO::Async::Loop;
use FindBin;
use lib "$FindBin::Bin/../lib";

use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    eval { require Net::Async::HTTP; 1 }
        or plan skip_all => 'Net::Async::HTTP required';
}

# A response write that crosses the write high-water mark takes the deferred
# path (so the pagi.transport watermark callbacks can observe the crossing),
# which suspends inline write flushing (stream autoflush). Once the queue
# drains, the connection must restore inline flushing: a connection stuck in
# deferred mode silently reverts every later write on that keep-alive
# connection to the one-loop-cycle-per-write path. White-box regression test
# for the restore.

my $loop = IO::Async::Loop->new;

my $body = 'x' x 4096;  # over the 1KB high-water mark configured below

my $app = async sub {
    my ($scope, $receive, $send) = @_;
    die "Unsupported scope type: $scope->{type}\n" if $scope->{type} ne 'http';

    await $send->({
        type    => 'http.response.start',
        status  => 200,
        headers => [ [ 'content-type', 'text/plain' ],
                     [ 'content-length', length $body ] ],
    });
    await $send->({ type => 'http.response.body', body => $body, more => 0 });
};

my $server = PAGI::Server->new(
    app => $app, host => '127.0.0.1', port => 0, quiet => 1,
    write_high_watermark => 1024,
    write_low_watermark  => 256,
);
$loop->add($server);
$server->listen->get;
my $port = $server->port;

my $http = Net::Async::HTTP->new;
$loop->add($http);

my $resp = $http->do_request(uri => "http://127.0.0.1:$port/")->get;
is($resp->code, 200, 'response larger than the high-water mark served');

my ($conn) = values %{ $server->{connections} };
ok($conn, 'keep-alive connection still tracked');

my $deadline = time + 3;
$loop->loop_once(0.05) while $conn->{_autoflush_suspended} && time < $deadline;

ok(!$conn->{_autoflush_suspended}, 'deferred-write suspension released after the queue drained');
ok($conn->{stream}{autoflush}, 'stream autoflush restored for subsequent writes');

$server->shutdown->get;
$loop->remove($server);

done_testing;
