use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use Future;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";
use PAGI::Server;
use IO::Socket::INET;
use Socket qw(SOL_SOCKET SO_RCVBUF pack_sockaddr_in inet_aton);
use Time::HiRes qw(time);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# write_timeout: while bytes are queued for a client, if none are written for
# write_timeout seconds the connection is closed with reason write_timeout
# (PAGI::Spec::Www). A client that reads -- however slowly -- extends the
# clock; a connection with nothing queued is never timed out by it.

my $loop = IO::Async::Loop->new;
my @log;

sub start_server {
    my ($app, %opts) = @_;
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, log_level => 'warn', access_log => undef,
        shutdown_timeout => 1, logger => sub { push @log, $_[0] }, %opts,
    );
    $loop->add($server);
    $server->listen->get;
    return $server;
}

sub stop_server {
    my ($server) = @_;
    $server->shutdown->get;
    $loop->remove($server);
}

sub pump_until {
    my ($cond, $secs) = @_;
    my $deadline = time + ($secs // 10);
    $loop->loop_once(0.01) until $cond->() || time > $deadline;
    return $cond->();
}

sub pump_for {
    my ($secs, $each) = @_;
    my $until = time + $secs;
    while (time < $until) { $loop->loop_once(0.01); $each->() if $each }
}

# A client that asks for a small receive buffer -- a hint only: macOS loopback
# keeps its own (~290 KB) -- so a stall leaves bytes queued in the server soon.
sub small_window_connect {
    my ($server) = @_;
    my $sock = IO::Socket::INET->new(Proto => 'tcp') or die "socket: $!";
    setsockopt($sock, SOL_SOCKET, SO_RCVBUF, 4096) or die "SO_RCVBUF: $!";
    $sock->connect(pack_sockaddr_in($server->port, inet_aton('127.0.0.1'))) or die "connect: $!";
    $sock->blocking(0);
    return $sock;
}

# 8 MB of sends the application does not await, then it waits for the end.
sub flood_app {
    my ($seen) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        my $conn = $scope->{'pagi.connection'};
        my @sent;
        if ($scope->{type} eq 'sse') {
            await $send->({ type => 'sse.start' });
            @sent = map { $send->({ type => 'sse.send', data => 'x' x 65_000 }) } 1 .. 128;
        }
        else {
            die "unsupported\n" unless $scope->{type} eq 'http';
            await $send->({ type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] });
            @sent = map { $send->({ type => 'http.response.body', body => 'x' x 65_536, more => 1 }) } 1 .. 128;
        }
        $seen->{pending} = grep { !$_->is_ready } @sent;
        my $event;
        do { $event = await $receive->() } until $event->{type} =~ /disconnect\z/;
        $seen->{event}   = $event->{type};
        $seen->{reason}  = $conn->disconnect_reason;
        $seen->{closed_at} = time;
        await Future->wait_all(@sent);
        $seen->{all_resolved} = (grep { $_->is_done } @sent) == @sent ? 1 : 0;
    };
}

subtest 'a client that stops reading is disconnected with write_timeout' => sub {
    my %seen;
    @log = ();
    my $server = start_server(flood_app(\%seen), write_timeout => 0.5);
    my $sock = small_window_connect($server);
    my $t0 = time;
    syswrite($sock, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
    ok(pump_until(sub { exists $seen{all_resolved} }, 10), 'the application saw the connection end');
    ok($seen{pending} > 0, "$seen{pending} sends were still queued");
    is($seen{event}, 'http.disconnect', 'it received http.disconnect');
    is($seen{reason}, 'write_timeout', 'with reason write_timeout');
    ok($seen{closed_at} - $t0 < 3, sprintf('after %.2fs', $seen{closed_at} - $t0));
    is($seen{all_resolved}, 1, 'every pending send resolved');
    my @warns = grep { ($_->{level} // '') eq 'warn' && ($_->{message} // '') =~ /Write timeout \(0\.5s\)/ } @log;
    is(scalar @warns, 1, 'one warn line names the timeout');
    close $sock;
    stop_server($server);
};

# Socket progress comes in steps: TCP reopens a window only once a large part
# of it is free (~300 KB on macOS loopback). A reader taking 128 KB every
# 0.1s makes progress every ~0.25s -- well inside a 1s write_timeout -- while
# still far slower than the 8 MB the application sends.
subtest 'a client reading slowly but steadily is not cut off' => sub {
    my %seen;
    my $server = start_server(flood_app(\%seen), write_timeout => 1);
    my $sock = small_window_connect($server);
    syswrite($sock, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
    my ($read, $next) = (0, time);
    pump_for(3, sub {
        return if time < $next;
        my $n = sysread($sock, my $buf, 131_072);
        $read += $n if $n;
        $next = time + 0.1;
    });
    ok($read > 0, "the client read $read bytes over 3s");
    ok(!exists $seen{reason}, 'and the connection is still open');
    close $sock;
    stop_server($server);
};

# One send of 8 MB keeps the buffer from ever emptying while the client reads;
# only the client taking bytes extends the clock.
subtest 'a steady reader of one large send is not cut off' => sub {
    @log = ();
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported\n" unless $scope->{type} eq 'http';
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] });
        await $send->({ type => 'http.response.body', body => 'x' x 8_000_000, more => 0 });
    };
    my $server = start_server($app, write_timeout => 1);
    my $sock = small_window_connect($server);
    syswrite($sock, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
    my ($read, $next, $eof) = (0, time, 0);
    pump_for(3, sub {
        return if time < $next;
        my $n = sysread($sock, my $buf, 131_072);
        $read += $n if $n;
        $eof = 1 if defined $n && $n == 0;
        $next = time + 0.1;
    });
    ok($read > 600_000 && $read < 8_000_000, "the client read $read of 8,000,000 bytes over 3s");
    is($eof, 0, 'and the server has not closed the connection');
    is([grep { ($_->{message} // '') =~ /Write timeout/ } @log], [], 'nor timed it out');
    close $sock;
    stop_server($server);
};

subtest 'a connection with nothing queued is not timed out' => sub {
    my %seen;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        my $conn = $scope->{'pagi.connection'};
        if ($scope->{type} eq 'sse') {
            await $send->({ type => 'sse.start' });
            my $event;
            do { $event = await $receive->() } until $event->{type} =~ /disconnect\z/;
            $seen{sse_reason} = $conn->disconnect_reason;
            return;
        }
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] });
        await $send->({ type => 'http.response.body', body => 'OK', more => 0 });
    };
    my $server = start_server($app, write_timeout => 0.5);
    my $keepalive = small_window_connect($server);
    syswrite($keepalive, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
    my $sse = small_window_connect($server);
    syswrite($sse, "GET /events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n");
    my ($eof_keepalive, $eof_sse) = (0, 0);
    pump_for(3, sub {
        for ([$keepalive, \$eof_keepalive], [$sse, \$eof_sse]) {
            my ($s, $eof) = @$_;
            my $n = sysread($s, my $buf, 65536);
            $$eof = 1 if defined $n && $n == 0;
        }
    });
    is($eof_keepalive, 0, 'an idle keep-alive connection is still open after 3s');
    is($eof_sse, 0, 'an SSE stream with nothing to send is still open after 3s');
    ok(!exists $seen{sse_reason}, 'and the SSE application has not seen an end');
    close $_ for $keepalive, $sse;
    stop_server($server);
};

subtest 'write_timeout => 0 never times a client out' => sub {
    my %seen;
    my $server = start_server(flood_app(\%seen), write_timeout => 0);
    my $sock = small_window_connect($server);
    syswrite($sock, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
    pump_for(3);
    ok($seen{pending}, 'sends were queued');
    ok(!exists $seen{reason}, 'and a stalled client is still connected after 3s');
    close $sock;
    stop_server($server);
};

done_testing;
