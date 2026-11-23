use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";
use PAGI::Server;
use IO::Socket::INET;
use POSIX qw(:sys_wait_h);
use Time::HiRes qw(time sleep);

plan skip_all => "Fork not available on this platform" if $^O eq 'MSWin32';
plan skip_all => "Forks a multi-worker server: requires RELEASE_TESTING=1" unless $ENV{RELEASE_TESTING};

# max_sends_per_turn in a multi-worker server. The parent builds each worker's
# server and hands it the options; one it did not hand on would leave the
# workers at the default without any error. With a limit of 1 a response of a
# hundred awaited body sends has about one in six held (one counted, four
# unheld, then one held, each time round); at the default of 384 it has none.
# The application reports its own count in the last body send.

my $app = async sub {
    my ($scope, $receive, $send) = @_;
    die "unsupported scope type\n" unless $scope->{type} eq 'http';
    await $send->({ type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] });
    my $held = 0;
    for my $n (1 .. 100) {
        my $f = $send->({ type => 'http.response.body', body => ('x' x 99) . "\n", more => 1 });
        $held++ unless $f->is_ready;
        await $f;
    }
    await $send->({ type => 'http.response.body', body => "held=$held\n" });
};

# Forks a process serving $app with one worker and the given options; returns
# its pid once the port accepts connections.
sub serve {
    my ($port, %opts) = @_;
    my $pid = fork() // die "fork: $!";
    if (!$pid) {
        my $loop = IO::Async::Loop->new;
        my $server = PAGI::Server->new(
            app => $app, host => '127.0.0.1', port => $port, workers => 1,
            quiet => 1, access_log => undef, shutdown_timeout => 1, %opts,
        );
        $loop->add($server);
        $server->listen->get;
        $loop->run;
        exit 0;
    }
    my $deadline = time + 10;
    while (time < $deadline) {
        my $sock = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 0.5);
        if ($sock) { close $sock; return $pid }
        sleep 0.1;
    }
    kill 'KILL', $pid;
    waitpid($pid, 0);
    die "the server on port $port never accepted a connection\n";
}

sub stop {
    my ($pid) = @_;
    kill 'TERM', $pid;
    my $deadline = time + 8;
    while (time < $deadline) {
        return if waitpid($pid, WNOHANG) == $pid;
        sleep 0.1;
    }
    kill 'KILL', $pid;
    waitpid($pid, 0);
    return;
}

# How many of the response's hundred sends the worker's application saw held.
sub held_sends {
    my ($port) = @_;
    my $sock = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5)
        or die "connect: $!";
    print {$sock} "GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n";
    my $response = do { local $/; <$sock> } // '';
    close $sock;
    my ($held) = $response =~ /held=(\d+)\n/;
    return $held;
}

subtest 'a worker runs with the configured value' => sub {
    my $port = 5900 + int(rand(90));
    my $pid = serve($port, max_sends_per_turn => 1);
    my $held = held_sends($port);
    stop($pid);
    ok(defined $held, 'the worker answered') or return;
    cmp_ok($held, '>=', 10, "with max_sends_per_turn => 1, $held of a hundred sends were held");
};

subtest 'and with the default when none is configured' => sub {
    my $port = 5900 + int(rand(90));
    my $pid = serve($port);
    my $held = held_sends($port);
    stop($pid);
    is($held, 0, 'at the default of 384 a hundred sends are not held');
};

done_testing;
