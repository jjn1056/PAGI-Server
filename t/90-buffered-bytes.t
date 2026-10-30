use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";
use PAGI::Server;
use IO::Socket::INET;
use Time::HiRes qw(time);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# The bytes a connection has queued for its client and not yet written are
# counted as they are queued and as IO::Async writes them, so measuring the
# buffer -- which every send does -- costs the same however many writes are
# queued, and stays exact: it is what PAGI::Spec::Www 0.7's buffered_amount
# reports.

my $loop = IO::Async::Loop->new;

sub start_server {
    my (%opts) = @_;
    my $server = PAGI::Server->new(
        host => '127.0.0.1', port => 0, quiet => 1, access_log => undef, shutdown_timeout => 1, %opts,
    );
    $loop->add($server);
    $server->listen->get;
    return $server;
}

# An application that sends $n chunks of $size bytes without awaiting them.
sub burst_app {
    my ($n, $size) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported\n" unless $scope->{type} eq 'http';
        await $receive->();
        await $send->({ type => 'http.response.start', status => 200,
                        headers => [ ['content-type', 'text/plain'] ] });
        my @sent = map { $send->({ type => 'http.response.body', body => 'x' x $size, more => 1 }) } 1 .. $n;
        push @sent, $send->({ type => 'http.response.body', body => '', more => 0 });
        await Future->wait_all(@sent);
        return;
    };
}

# What the old measure computed: a walk over IO::Async::Stream's write queue.
sub walked {
    my ($conn) = @_;
    my $total = 0;
    for my $writer (@{ $conn->{stream}{writequeue} // [] }) {
        my $data = $writer->data;
        $total += length $data if defined $data && !ref $data;
    }
    return $total;
}

sub pump_until {
    my ($cond, $secs) = @_;
    my $deadline = time + ($secs // 10);
    $loop->loop_once(0.01) until $cond->() || time > $deadline;
    return $cond->();
}

# Connects, sends a GET, reads nothing; returns the socket and the connection.
sub open_request {
    my ($server) = @_;
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $server->port, Proto => 'tcp', Timeout => 2,
    ) or die "connect: $!";
    $sock->blocking(0);
    syswrite($sock, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
    my $conn;
    pump_until(sub { ($conn) = values %{ $server->{connections} }; $conn }, 5)
        or die "no connection";
    return ($sock, $conn);
}

sub read_some {
    my ($sock, $bytes) = @_;
    my $got = 0;
    while ($got < $bytes) {
        my $n = sysread($sock, my $chunk, 65536);
        last unless defined $n && $n > 0;
        $got += $n;
    }
    return $got;
}

subtest 'the count matches the write queue while it drains, and ends at 0' => sub {
    my $server = start_server(app => burst_app(400, 10_000), write_high_watermark => 1 << 30);
    my ($sock, $conn) = open_request($server);
    ok(pump_until(sub { walked($conn) > 1_000_000 }, 10), 'a backlog built up while the client read nothing');

    my $checks = 0;
    while (walked($conn) > 0) {
        is($conn->_get_write_buffer_size, walked($conn), 'the count is the queued bytes')
            or last;
        $checks++;
        read_some($sock, 200_000);
        $loop->loop_once(0.01);
    }
    ok($checks >= 3, "checked at $checks points while draining");
    read_some($sock, 1 << 30);
    ok(pump_until(sub { walked($conn) == 0 }, 10), 'the queue drained');
    is($conn->_get_write_buffer_size, 0, 'and the count is 0');

    close $sock;
    $server->shutdown->get;
    $loop->remove($server);
};

subtest 'measuring costs the same with 10 or many thousands of queued writes' => sub {
    my $server = start_server(app => burst_app(20_000, 32), write_high_watermark => 1 << 30);
    my ($sock, $conn) = open_request($server);
    ok(pump_until(sub { @{ $conn->{stream}{writequeue} // [] } >= 5000 }, 20),
        'thousands of writes are queued') or diag scalar @{ $conn->{stream}{writequeue} // [] };

    my $time = sub {
        my $t0 = time;
        $conn->_get_write_buffer_size for 1 .. 100;
        return time - $t0;
    };
    my $many = $time->();

    read_some($sock, 1 << 30);
    pump_until(sub { @{ $conn->{stream}{writequeue} // [] } <= 10 }, 20);
    my $few = $time->();
    ok($many < 10 * $few + 0.001,
        sprintf('100 measures: %.4fs with a long queue, %.4fs with a short one', $many, $few));

    close $sock;
    $server->shutdown->get;
    $loop->remove($server);
};

subtest 'over TLS the count is plaintext bytes, matching the write queue' => sub {
    my $cert_dir = "$FindBin::Bin/certs";
    skip_all 'TLS modules not installed (optional)'
        unless eval { require IO::Async::SSL; require IO::Socket::SSL; 1 };
    skip_all 'Test certificates not found'
        unless -f "$cert_dir/server.crt" && -f "$cert_dir/server.key";

    my $server = start_server(app => burst_app(400, 10_000), write_high_watermark => 1 << 30,
        ssl => { cert_file => "$cert_dir/server.crt", key_file => "$cert_dir/server.key" });
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $server->port, Proto => 'tcp', Timeout => 2,
    ) or die "connect: $!";
    $sock->blocking(0);
    IO::Socket::SSL->start_SSL($sock, SSL_verify_mode => 0, SSL_startHandshake => 0)
        or die "start_SSL: $IO::Socket::SSL::SSL_ERROR";
    ok(pump_until(sub { $sock->connect_SSL }, 10), 'TLS handshake done');
    syswrite($sock, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
    my $conn;
    pump_until(sub { ($conn) = values %{ $server->{connections} }; $conn }, 5);
    ok(pump_until(sub { walked($conn) > 1_000_000 }, 10), 'a backlog built up');

    my $checks = 0;
    while (walked($conn) > 0) {
        is($conn->_get_write_buffer_size, walked($conn), 'the count is the queued plaintext bytes')
            or last;
        $checks++;
        read_some($sock, 200_000);
        $loop->loop_once(0.01);
    }
    ok($checks >= 3, "checked at $checks points while draining");
    read_some($sock, 1 << 30);
    ok(pump_until(sub { walked($conn) == 0 }, 10), 'the queue drained');
    is($conn->_get_write_buffer_size, 0, 'and the count is 0');

    close $sock;
    $server->shutdown->get;
    $loop->remove($server);
};

subtest 'a client that disconnects mid-burst leaves the count at 0' => sub {
    my $server = start_server(app => burst_app(400, 10_000), write_high_watermark => 1 << 30);
    my ($sock, $conn) = open_request($server);
    ok(pump_until(sub { walked($conn) > 1_000_000 }, 10), 'a backlog built up');
    close $sock;
    ok(pump_until(sub { $conn->{closed} }, 10), 'the server saw the disconnect');
    is($conn->_get_write_buffer_size, 0, 'the count is 0');

    $server->shutdown->get;
    $loop->remove($server);
};

done_testing;
