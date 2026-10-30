use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use Net::Async::HTTP;
use File::Temp qw(tempdir);
use Future::AsyncAwait;
use Time::HiRes qw(time);
use FindBin;
use lib "$FindBin::Bin/../lib";
use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# A file larger than sync_file_threshold is read in PAGI::Server::AsyncFile's
# worker processes, one pool per event loop. The last PAGI::Server on a loop
# to shut down stops that pool, so no worker outlives the servers that used
# it; a server still running on the loop keeps its workers.

my $loop = IO::Async::Loop->new;
my $http = Net::Async::HTTP->new(max_connections_per_host => 0);
$loop->add($http);

my $dir = tempdir(CLEANUP => 1);
my $big = "$dir/big.txt";
{ open my $fh, '>', $big or die $!; print $fh 'x' x 200_000; close $fh }

my $app = async sub {
    my ($scope, $receive, $send) = @_;
    if ($scope->{type} eq 'lifespan') {
        while (1) {
            my $event = await $receive->();
            if ($event->{type} eq 'lifespan.startup') {
                await $send->({ type => 'lifespan.startup.complete' });
            }
            else {
                await $send->({ type => 'lifespan.shutdown.complete' });
                return;
            }
        }
    }
    die "unsupported\n" unless $scope->{type} eq 'http';
    await $send->({ type => 'http.response.start', status => 200,
                    headers => [['content-type', 'text/plain']] });
    await $send->({ type => 'http.response.body', file => $big });
};

sub start_server {
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, quiet => 1,
        access_log => undef, shutdown_timeout => 2,
    );
    $loop->add($server);
    $server->listen->get;
    return $server;
}

sub fetch_big {
    my ($server) = @_;
    my $response = $http->GET("http://127.0.0.1:" . $server->port . "/")->get;
    return length $response->content;
}

# This process's children: the pool's worker processes.
sub workers {
    my @pids = sort { $a <=> $b } split ' ', `pgrep -P $$`;
    return @pids;
}

sub pump_until {
    my ($cond, $secs) = @_;
    my $deadline = time + $secs;
    $loop->loop_once(0.05) until $cond->() || time > $deadline;
    return $cond->();
}

subtest 'the workers that read a large file stop when the server shuts down' => sub {
    my $server = start_server();
    is(fetch_big($server), 200_000, 'the file was served');
    ok(scalar(workers()) >= 1, 'it was read in a worker process');

    $server->shutdown->get;
    ok(pump_until(sub { !workers() }, 10), 'no worker is left after shutdown')
        or diag 'still running: ' . join ' ', workers();
    $loop->remove($server);
};

subtest 'a server still running on the loop keeps its workers' => sub {
    my $first  = start_server();
    my $second = start_server();
    is(fetch_big($second), 200_000, 'the second server served the file');
    my @before = workers();
    ok(scalar(@before) >= 1, 'through a worker process');

    $first->shutdown->get;
    $loop->loop_once(0.05) for 1 .. 20;
    is([workers()], \@before, 'the first server shutting down left the workers running');
    is(fetch_big($second), 200_000, 'and the second server still serves the file');

    $second->shutdown->get;
    ok(pump_until(sub { !workers() }, 10), 'the last server shutting down stops them');
    $loop->remove($_) for $first, $second;
};

subtest 'a server started after the workers stopped reads large files again' => sub {
    my $server = start_server();
    is(fetch_big($server), 200_000, 'the file was served');
    $server->shutdown->get;
    $loop->remove($server);
};

done_testing;
