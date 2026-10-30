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
use Protocol::WebSocket::Frame;
use File::Temp qw(tempdir);
use Socket qw(SOL_SOCKET SO_RCVBUF pack_sockaddr_in inet_aton);
use Time::HiRes qw(time);
use POSIX ();

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

# Whether the server has closed this connection's socket.
sub socket_closed {
    my ($conn) = @_;
    my $stream = $conn && $conn->{stream};
    my $handle = $stream && $stream->write_handle;
    return !$handle || !defined fileno $handle;
}

sub first_connection {
    my ($server) = @_;
    my $conn;
    pump_until(sub { ($conn) = values %{ $server->{connections} }; $conn }, 5);
    return $conn;
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

subtest 'write_timeout is on by default, at 60 seconds' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'http';
        await $send->({ type => 'http.response.start', status => 200, headers => [] });
        await $send->({ type => 'http.response.body', body => 'OK', more => 0 });
    };
    my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0, quiet => 1, access_log => undef);
    is($server->{write_timeout}, 60, 'write_timeout defaults to 60');
    $loop->add($server);
    $server->listen->get;
    my $sock = small_window_connect($server);
    syswrite($sock, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
    ok(pump_until(sub { $server->{_write_sweep} }, 5), 'and its sweep runs once a client connects');
    close $sock;
    stop_server($server);
};

subtest 'a client that stops reading is disconnected with write_timeout' => sub {
    my %seen;
    @log = ();
    my $server = start_server(flood_app(\%seen), write_timeout => 0.5);
    my $sock = small_window_connect($server);
    my $t0 = time;
    syswrite($sock, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
    my $conn = first_connection($server);
    ok(pump_until(sub { exists $seen{all_resolved} }, 10), 'the application saw the connection end');
    # The socket itself is closed at once, not left waiting to drain to a
    # client that is not reading.
    ok(pump_until(sub { socket_closed($conn) }, 2), 'and the server closed the socket');
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
    my $server = start_server(flood_app(\%seen), write_timeout => 2);
    my $sock = small_window_connect($server);
    syswrite($sock, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
    my ($read, $next) = (0, time);
    pump_for(5, sub {
        return if time < $next;
        my $n = sysread($sock, my $buf, 131_072);
        $read += $n if $n;
        $next = time + 0.1;
    });
    ok($read > 0, "the client read $read bytes over 5s");
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
    my $server = start_server($app, write_timeout => 2);
    my $sock = small_window_connect($server);
    syswrite($sock, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
    my ($read, $late, $next, $eof) = (0, 0, time, 0);
    my $limit_passed = time + 2.5;   # write_timeout, and a sweep
    pump_for(5, sub {
        return if time < $next;
        my $n = sysread($sock, my $buf, 131_072);
        $read += $n if $n;
        $late += $n if $n && time > $limit_passed;
        $eof = 1 if defined $n && $n == 0;
        $next = time + 0.1;
    });
    # How much arrives depends on the platform's buffers (Linux honours the
    # small receive buffer, macOS does not); what matters is that bytes kept
    # arriving after write_timeout had passed, while the one send went on.
    ok($late > 0, "the client still received bytes after write_timeout ($late of $read)");
    ok($read < 8_000_000, "while the one send was still being written ($read of 8,000,000 bytes)");
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

subtest 'a stalled SSE client is disconnected with write_timeout' => sub {
    my %seen;
    my $server = start_server(flood_app(\%seen), write_timeout => 0.5);
    my $sock = small_window_connect($server);
    syswrite($sock, "GET /events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n");
    my $conn = first_connection($server);
    ok(pump_until(sub { exists $seen{'all_resolved'} }, 10), 'the stream ended');
    ok(pump_until(sub { socket_closed($conn) }, 2), 'the server closed the socket');
    ok($seen{pending} > 0, "$seen{pending} events were queued");
    is($seen{event}, 'sse.disconnect', 'with sse.disconnect');
    is($seen{reason}, 'write_timeout', 'and reason write_timeout');
    close $sock;
    stop_server($server);
};

subtest 'a stalled WebSocket client is disconnected: code 1006, write_timeout' => sub {
    my %seen;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported\n" unless $scope->{type} eq 'websocket';
        my $conn = $scope->{'pagi.connection'};
        await $receive->();
        await $send->({ type => 'websocket.accept' });
        my @sent = map { $send->({ type => 'websocket.send', bytes => 'W' x 65_000 }) } 1 .. 128;
        $seen{pending} = grep { !$_->is_ready } @sent;
        my $event;
        do { $event = await $receive->() } until $event->{type} eq 'websocket.disconnect';
        $seen{code} = $event->{code};
        $seen{reason} = $conn->disconnect_reason;
    };
    my $server = start_server($app, write_timeout => 0.5);
    my $sock = small_window_connect($server);
    syswrite($sock, join "\r\n", 'GET /ws HTTP/1.1', 'Host: localhost', 'Upgrade: websocket',
        'Connection: Upgrade', 'Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==', 'Sec-WebSocket-Version: 13', '', '');
    my $conn = first_connection($server);
    ok(pump_until(sub { exists $seen{reason} }, 10), 'the application saw the disconnect');
    ok(pump_until(sub { socket_closed($conn) }, 2), 'the server closed the socket');
    ok($seen{pending} > 0, "$seen{pending} frames were queued");
    is($seen{code}, 1006, 'code 1006: no closing handshake');
    is($seen{reason}, 'write_timeout', 'reason write_timeout');
    close $sock;
    stop_server($server);
};

subtest 'over TLS a stalled client is disconnected with write_timeout' => sub {
    my $cert_dir = "$FindBin::Bin/certs";
    skip_all 'TLS modules not installed (optional)'
        unless eval { require IO::Async::SSL; require IO::Socket::SSL; 1 };
    my %seen;
    my $server = start_server(flood_app(\%seen), write_timeout => 0.5,
        ssl => { cert_file => "$cert_dir/server.crt", key_file => "$cert_dir/server.key" });
    my $sock = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $server->port, Proto => 'tcp')
        or die "connect: $!";
    $sock->blocking(0);
    IO::Socket::SSL->start_SSL($sock, SSL_verify_mode => 0, SSL_startHandshake => 0)
        or die "start_SSL: $IO::Socket::SSL::SSL_ERROR";
    ok(pump_until(sub { $sock->connect_SSL }, 10), 'TLS handshake done');
    syswrite($sock, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
    my $conn = first_connection($server);
    ok(pump_until(sub { exists $seen{all_resolved} }, 10), 'the application saw the connection end');
    ok(pump_until(sub { socket_closed($conn) }, 2), 'the server closed the socket');
    is($seen{reason}, 'write_timeout', 'with reason write_timeout');
    close $sock;
    stop_server($server);
};

subtest 'each server stops its own sweep; another on the loop keeps its own' => sub {
    my (%seen_a, %seen_b);
    my $a = start_server(flood_app(\%seen_a), write_timeout => 0.5);
    my $b = start_server(flood_app(\%seen_b), write_timeout => 0.5);
    my $sock_a = small_window_connect($a);
    syswrite($sock_a, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
    pump_until(sub { $seen_a{pending} }, 5);
    ok($a->{_write_sweep}, 'a server sweeps once it has a connection');
    ok(!$b->{_write_sweep}, 'one without a connection does not yet');
    close $sock_a;
    stop_server($a);
    ok(!$a->{_write_sweep}, 'the first stopped its sweep at shutdown');
    my $sock_b = small_window_connect($b);
    syswrite($sock_b, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
    ok(pump_until(sub { exists $seen_b{reason} }, 10), "the other's sweep still times out a stalled client");
    is($seen_b{reason}, 'write_timeout', 'with write_timeout');
    close $sock_b;
    stop_server($b);
};

subtest 'workers mode: a stalled client of a worker is disconnected' => sub {
    skip_all 'set RELEASE_TESTING=1 for the workers-mode test' unless $ENV{RELEASE_TESTING};
    my $dir = tempdir(CLEANUP => 1);
    my $port_file = "$dir/port";
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'http';
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] });
        my @sent = map { $send->({ type => 'http.response.body', body => 'x' x 65_536, more => 1 }) } 1 .. 128;
        my $event;
        do { $event = await $receive->() } until $event->{type} =~ /disconnect\z/;
    };
    my $master = fork() // die "fork: $!";
    if ($master == 0) {
        my $child_loop = IO::Async::Loop->new;
        my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0, workers => 1,
            quiet => 1, access_log => undef, write_timeout => 0.5);
        $child_loop->add($server);
        eval {
            $server->listen->get;
            open my $out, '>', $port_file or die $!;
            print $out $server->port;
            close $out;
            $child_loop->run;
        };
        POSIX::_exit(0);
    }
    my $port;
    my $deadline = time + 15;
    until ($port || time > $deadline) { $port = -s $port_file ? do { open my $in, '<', $port_file; <$in> } : undef; Time::HiRes::sleep(0.1) }
    my $sock;
    $deadline = time + 10;
    until ($sock || time > $deadline) {
        $sock = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 1);
        Time::HiRes::sleep(0.1) unless $sock;
    }
    ok($sock, 'connected to the worker') or do { kill 'KILL', $master; waitpid $master, 0; return };
    syswrite($sock, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
    Time::HiRes::sleep(3);   # read nothing for six timeouts
    $sock->blocking(0);
    my ($read, $ended) = (0, 0);
    $deadline = time + 5;
    until ($ended || time > $deadline) {
        my $n = sysread($sock, my $buf, 1 << 20);
        if (defined $n && $n > 0) { $read += $n }
        elsif (defined $n || !$!{EAGAIN}) { $ended = 1 }
        else { Time::HiRes::sleep(0.05) }
    }
    ok($ended, "the worker had closed the connection ($read bytes were buffered)");
    ok($read < 128 * 65_536, 'before the response was complete');
    close $sock;
    kill 'TERM', $master;
    my $gone = time + 10;
    Time::HiRes::sleep(0.1) while kill(0, $master) && time < $gone && !waitpid($master, POSIX::WNOHANG());
    kill 'KILL', $master if kill 0, $master;
    waitpid $master, 0;
};

done_testing;
