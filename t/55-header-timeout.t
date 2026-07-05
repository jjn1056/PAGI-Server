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

# header_timeout: the complete request head must arrive within N seconds of
# its first byte (total elapsed, so trickling bytes cannot extend it, unlike
# the activity-reset idle/stall timers). Expiry answers 408 and closes.

my $factor = $ENV{PERL_TEST_TIME_OUT_FACTOR};
$factor = 1 unless defined $factor && $factor =~ /^\d+$/ && $factor >= 1;

my $loop = IO::Async::Loop->new;

my $app = async sub {
    my ($scope, $receive, $send) = @_;
    die "Unsupported scope type: $scope->{type}\n" if $scope->{type} ne 'http';
    await $send->({
        type    => 'http.response.start',
        status  => 200,
        headers => [ [ 'content-type', 'text/plain' ], [ 'content-length', 2 ] ],
    });
    await $send->({ type => 'http.response.body', body => 'ok', more => 0 });
};

sub new_server {
    my (%opts) = @_;
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, quiet => 1, %opts,
    );
    $loop->add($server);
    $server->listen->get;
    return $server;
}

sub raw_client {
    my ($port) = @_;
    my $c = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp',
    ) or die "connect: $!";
    $c->blocking(0);
    return $c;
}

# Drain readable bytes into $$bufref; returns 1 once EOF is seen, 0 otherwise.
sub drain {
    my ($c, $bufref) = @_;
    while (1) {
        my $n = sysread($c, my $b, 8192);
        return 0 unless defined $n;   # EAGAIN - nothing more right now
        return 1 if $n == 0;          # EOF
        $$bufref .= $b;
    }
}

sub pump_until {
    my ($cond, $secs) = @_;
    my $deadline = time + $secs;
    while (time < $deadline) {
        return 1 if $cond->();
        $loop->loop_once(0.05);
    }
    return $cond->();
}

subtest 'trickled head gets 408 and close' => sub {
    my $server = new_server(header_timeout => 1);
    my $c = raw_client($server->port);
    syswrite($c, "GET / HTTP/1.1\r\nHo");   # partial head, never completed

    my $resp = '';
    my $closed = pump_until(sub { drain($c, \$resp) }, 10 * $factor);

    ok($closed, 'server closed the connection');
    like($resp, qr{^HTTP/1\.1 408 Request Timeout\r\n}, 'got 408 Request Timeout');

    close $c;
    $server->shutdown->get;
    $loop->remove($server);
};

subtest 'complete request is unaffected' => sub {
    my $server = new_server(header_timeout => 1);
    my $c = raw_client($server->port);
    syswrite($c, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");

    my $resp = '';
    pump_until(sub { drain($c, \$resp); $resp =~ /\r\n\r\nok/ }, 10 * $factor);

    like($resp, qr{^HTTP/1\.1 200 OK\r\n}, 'served 200 normally');

    close $c;
    $server->shutdown->get;
    $loop->remove($server);
};

subtest 'keep-alive: deadline re-arms per request head' => sub {
    my $server = new_server(header_timeout => 1);
    my $c = raw_client($server->port);
    syswrite($c, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");

    my $resp = '';
    pump_until(sub { drain($c, \$resp); $resp =~ /\r\n\r\nok/ }, 10 * $factor);
    like($resp, qr{200 OK}, 'first request served');

    # Second request: partial head on the same connection
    syswrite($c, "GET / HTTP/1.1\r\nHo");
    my $resp2 = '';
    my $closed = pump_until(sub { drain($c, \$resp2) }, 10 * $factor);

    ok($closed, 'connection closed after second head stalled');
    like($resp2, qr{^HTTP/1\.1 408 }, 'second head got 408');

    close $c;
    $server->shutdown->get;
    $loop->remove($server);
};

subtest 'header_timeout => 0 disables the deadline' => sub {
    my $server = new_server(header_timeout => 0);
    my $c = raw_client($server->port);
    syswrite($c, "GET / HTTP/1.1\r\nHo");

    my $resp = '';
    my $closed = pump_until(sub { drain($c, \$resp) }, 3);
    ok(!$closed, 'partial head survives past the would-be deadline');

    # Completing the head still works
    syswrite($c, "st: localhost\r\n\r\n");
    pump_until(sub { drain($c, \$resp); $resp =~ /\r\n\r\nok/ }, 10 * $factor);
    like($resp, qr{200 OK}, 'request completed and served after the wait');

    close $c;
    $server->shutdown->get;
    $loop->remove($server);
};

subtest 'default is 30 seconds' => sub {
    my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0, quiet => 1);
    is($server->{header_timeout}, 30, 'header_timeout defaults to 30');
};

done_testing;
