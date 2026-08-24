use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
use Future::AsyncAwait;
use Scalar::Util qw(refaddr weaken);
use FindBin;
use lib "$FindBin::Bin/../lib";

use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

my $loop = IO::Async::Loop->new;

# =============================================================================
# Test 3.1: WebSocket Frame Parser Cleanup
# =============================================================================

subtest 'WebSocket frame parser cleaned up on close (3.1)' => sub {
    my $ws_connection;
    my $app = async sub  {
        my ($scope, $receive, $send) = @_;
        if ($scope->{type} eq 'lifespan') {
            while (1) {
                my $event = await $receive->();
                if ($event->{type} eq 'lifespan.startup') {
                    await $send->({ type => 'lifespan.startup.complete' });
                }
                elsif ($event->{type} eq 'lifespan.shutdown') {
                    await $send->({ type => 'lifespan.shutdown.complete' });
                    last;
                }
            }
            return;
        }

        if ($scope->{type} eq 'websocket') {
            await $send->({ type => 'websocket.accept' });
            # Just accept and wait for disconnect
            while (1) {
                my $event = await $receive->();
                last if $event->{type} eq 'websocket.disconnect';
            }
        }
    };

    my $server = PAGI::Server->new(
        app   => $app,
        host  => '127.0.0.1',
        port  => 0,
        quiet => 1,
    );

    $loop->add($server);
    $server->listen->get;
    my $port = $server->port;

    # Connect as WebSocket client
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1',
        PeerPort => $port,
        Proto    => 'tcp',
        Timeout  => 2,
    ) or die "Cannot connect: $!";

    # Send WebSocket upgrade request
    my $key = 'dGhlIHNhbXBsZSBub25jZQ==';
    print $sock "GET / HTTP/1.1\r\n";
    print $sock "Host: localhost\r\n";
    print $sock "Upgrade: websocket\r\n";
    print $sock "Connection: Upgrade\r\n";
    print $sock "Sec-WebSocket-Key: $key\r\n";
    print $sock "Sec-WebSocket-Version: 13\r\n";
    print $sock "\r\n";

    # Read upgrade response
    my $response = '';
    $sock->blocking(0);
    my $deadline = time + 2;
    while (time < $deadline) {
        $loop->loop_once(0.1);
        my $data;
        my $bytes = sysread($sock, $data, 4096);
        if (defined $bytes && $bytes > 0) {
            $response .= $data;
        }
        last if $response =~ /\r\n\r\n/;
    }

    like($response, qr/HTTP\/1\.1 101/, "WebSocket upgrade successful");

    # Get reference to connection object
    my @conns = values %{$server->{connections}};
    is(scalar @conns, 1, "One connection tracked");
    $ws_connection = $conns[0];
    ok($ws_connection->{websocket_frame}, "WebSocket frame parser exists");

    # Close the socket
    close($sock);

    # Let server process the close
    $loop->loop_once(0.2);

    # After close, websocket_frame should be cleaned up
    ok(!$ws_connection->{websocket_frame}, "WebSocket frame parser cleaned up after close");

    $server->shutdown->get;
    eval { $loop->remove($server) };
};

# =============================================================================
# Test 3.2: Connection Closed After App Exception
# =============================================================================

subtest 'Connection closed after application exception (3.2)' => sub {
    my $exception_thrown = 0;
    my $app = async sub  {
        my ($scope, $receive, $send) = @_;
        if ($scope->{type} eq 'lifespan') {
            while (1) {
                my $event = await $receive->();
                if ($event->{type} eq 'lifespan.startup') {
                    await $send->({ type => 'lifespan.startup.complete' });
                }
                elsif ($event->{type} eq 'lifespan.shutdown') {
                    await $send->({ type => 'lifespan.shutdown.complete' });
                    last;
                }
            }
            return;
        }

        # Throw exception for specific path
        if ($scope->{path} eq '/throw') {
            $exception_thrown = 1;
            die "Application exception for testing!";
        }

        await $send->({
            type => 'http.response.start',
            status => 200,
            headers => [['content-type', 'text/plain']],
        });
        await $send->({
            type => 'http.response.body',
            body => 'OK',
            more => 0,
        });
    };

    my $server = PAGI::Server->new(
        app   => $app,
        host  => '127.0.0.1',
        port  => 0,
        quiet => 1,
    );

    $loop->add($server);
    $server->listen->get;
    my $port = $server->port;

    # Send request that will cause exception
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1',
        PeerPort => $port,
        Proto    => 'tcp',
        Timeout  => 2,
    ) or die "Cannot connect: $!";

    # Use keep-alive to verify connection is closed due to exception, not due to Connection: close
    print $sock "GET /throw HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n";

    my $response = '';
    $sock->blocking(0);
    my $deadline = time + 2;
    while (time < $deadline) {
        $loop->loop_once(0.1);
        my $data;
        my $bytes = sysread($sock, $data, 4096);
        if (defined $bytes && $bytes > 0) {
            $response .= $data;
        }
        elsif (defined $bytes && $bytes == 0) {
            last;  # EOF - connection closed by server
        }
    }
    close($sock);

    ok($exception_thrown, "Exception was thrown");
    like($response, qr/HTTP\/1\.1 500/, "Server returned 500 error");

    # Give server time to process
    $loop->loop_once(0.1);

    # Connection should be removed from tracking
    my $conn_count = keys %{$server->{connections}};
    is($conn_count, 0, "Connection removed from server after exception");

    $server->shutdown->get;
    eval { $loop->remove($server) };
};

# =============================================================================
# Test 3.17: Error After Response Started
# =============================================================================

subtest 'Exception after response started handled properly (3.17)' => sub {
    my $exception_thrown = 0;
    my $app = async sub  {
        my ($scope, $receive, $send) = @_;
        if ($scope->{type} eq 'lifespan') {
            while (1) {
                my $event = await $receive->();
                if ($event->{type} eq 'lifespan.startup') {
                    await $send->({ type => 'lifespan.startup.complete' });
                }
                elsif ($event->{type} eq 'lifespan.shutdown') {
                    await $send->({ type => 'lifespan.shutdown.complete' });
                    last;
                }
            }
            return;
        }

        # Send response headers first
        await $send->({
            type => 'http.response.start',
            status => 200,
            headers => [['content-type', 'text/plain']],
        });

        # Then throw exception (after response started)
        if ($scope->{path} eq '/throw-after-start') {
            $exception_thrown = 1;
            die "Exception after response started!";
        }

        await $send->({
            type => 'http.response.body',
            body => 'OK',
            more => 0,
        });
    };

    my $server = PAGI::Server->new(
        app   => $app,
        host  => '127.0.0.1',
        port  => 0,
        quiet => 1,
    );

    $loop->add($server);
    $server->listen->get;
    my $port = $server->port;

    # Send request that will throw after response started
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1',
        PeerPort => $port,
        Proto    => 'tcp',
        Timeout  => 2,
    ) or die "Cannot connect: $!";

    # Use keep-alive to verify connection is closed due to exception, not due to Connection: close
    print $sock "GET /throw-after-start HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n";

    my $response = '';
    $sock->blocking(0);
    my $deadline = time + 2;
    while (time < $deadline) {
        $loop->loop_once(0.1);
        my $data;
        my $bytes = sysread($sock, $data, 4096);
        if (defined $bytes && $bytes > 0) {
            $response .= $data;
        }
        elsif (defined $bytes && $bytes == 0) {
            last;  # EOF - connection closed
        }
    }
    close($sock);

    ok($exception_thrown, "Exception was thrown after response started");
    # Response should start with 200 (the original response, not 500)
    like($response, qr/HTTP\/1\.1 200/, "Original 200 response preserved (can't change after started)");
    # Connection should be closed (not left hanging)

    $loop->loop_once(0.1);
    my $conn_count = keys %{$server->{connections}};
    is($conn_count, 0, "Connection closed after exception (even when response started)");

    $server->shutdown->get;
    eval { $loop->remove($server) };
};

# =============================================================================
# Test: Multiple Connections Cleanup
# =============================================================================

subtest 'Multiple connections with exceptions all cleaned up' => sub {
    my $request_count = 0;
    my $exception_count = 0;
    my $app = async sub  {
        my ($scope, $receive, $send) = @_;
        if ($scope->{type} eq 'lifespan') {
            while (1) {
                my $event = await $receive->();
                if ($event->{type} eq 'lifespan.startup') {
                    await $send->({ type => 'lifespan.startup.complete' });
                }
                elsif ($event->{type} eq 'lifespan.shutdown') {
                    await $send->({ type => 'lifespan.shutdown.complete' });
                    last;
                }
            }
            return;
        }

        $request_count++;
        # Every other request throws
        if ($request_count % 2 == 0) {
            $exception_count++;
            die "Exception on request $request_count";
        }

        await $send->({
            type => 'http.response.start',
            status => 200,
            headers => [['content-type', 'text/plain']],
        });
        await $send->({
            type => 'http.response.body',
            body => 'OK',
            more => 0,
        });
    };

    my $server = PAGI::Server->new(
        app   => $app,
        host  => '127.0.0.1',
        port  => 0,
        quiet => 1,
    );

    $loop->add($server);
    $server->listen->get;
    my $port = $server->port;

    my @sockets;
    # Send 10 requests (5 will throw exceptions)
    for my $i (1..10) {
        my $sock = IO::Socket::INET->new(
            PeerAddr => '127.0.0.1',
            PeerPort => $port,
            Proto    => 'tcp',
            Timeout  => 2,
        ) or die "Cannot connect: $!";

        # Use keep-alive to verify exception connections are closed, not due to Connection: close
        print $sock "GET /$i HTTP/1.1\r\nHost: localhost\r\nConnection: keep-alive\r\n\r\n";

        my $response = '';
        $sock->blocking(0);
        my $deadline = time + 2;
        while (time < $deadline) {
            $loop->loop_once(0.1);
            my $data;
            my $bytes = sysread($sock, $data, 4096);
            if (defined $bytes && $bytes > 0) {
                $response .= $data;
            }
            elsif (defined $bytes && $bytes == 0) {
                last;  # Server closed connection (exception case)
            }
            # For keep-alive success case, response ends with body
            last if $response =~ /OK$/;
        }
        push @sockets, $sock;  # Keep socket open to simulate keep-alive
    }

    # Let server process
    $loop->loop_once(0.2);

    is($request_count, 10, "All 10 requests processed");
    is($exception_count, 5, "5 exceptions thrown");

    # Exception connections (5) should be closed immediately
    # Keep-alive successful connections (5) should still be tracked (waiting for more requests)
    my $conn_count = keys %{$server->{connections}};
    is($conn_count, 5, "Exception connections closed, keep-alive connections still tracked");

    # Clean up: close client sockets
    close($_) for @sockets;

    $server->shutdown->get;
    eval { $loop->remove($server) };
};

# =============================================================================
# Test: h1 teardown with the outbound buffer above the high mark must NOT
# fire on_drain (the connection is going away, not draining -- h2 already
# gets this right via its separate transport_drain_fires list; h1's arm_drain
# used to piggyback directly on the same _drain_waiters Futures that
# blocking producers await, so tearing down resolved both indiscriminately
# and fired on_drain with the buffer nowhere near the low mark). A producer
# genuinely parked on a blocking backpressure await must still resume (no
# coroutine leak) -- only the app-facing on_drain callback must be dropped.
#
# Exercises the real Connection-level machinery (_h1_transport_state's
# arm_drain, _wait_for_drain, _cancel_drain_waiters) directly against a
# fake stream that reports a controlled, non-draining buffer size -- a
# unit-level handler invocation, not a real socket, so the buffer-stays-full
# precondition is exact rather than timing-dependent.
# =============================================================================

package Local::FakeWriter {
    sub new { my ($c, $d) = @_; return bless { data => $d }, $c }
    sub data { return $_[0]->{data} }
}

package Local::FakeStream {
    sub new { return bless { writequeue => [] }, shift }
    sub configure { my $self = shift; my %args = @_; %$self = (%$self, %args); return }
}

package Local::FakeServer {
    sub new { my ($c, $loop) = @_; return bless { loop => $loop }, $c }
    sub loop { return $_[0]->{loop} }
}

subtest 'h1 teardown with buffer above the high mark drops on_drain but resumes a parked wait' => sub {
    my $fake_loop   = IO::Async::Loop->new;
    my $fake_server = Local::FakeServer->new($fake_loop);
    my $fake_stream = Local::FakeStream->new;

    my $conn = PAGI::Server::Connection->new(
        stream => $fake_stream, server => $fake_server, app => sub { },
        write_high_watermark => 10, write_low_watermark => 2,
    );

    # Buffer is (and stays, for this test) well above the high mark.
    $fake_stream->{writequeue} = [ Local::FakeWriter->new('x' x 100) ];

    my $transport = $conn->_h1_transport_state;
    my $drain_fired = 0;
    $transport->on_drain(sub { $drain_fired++ });

    # Crossing the high mark arms drain detection -- h1's arm_drain fires
    # into $conn->{_drain_fires} (post-fix) rather than reusing the blocking
    # producer queue.
    $transport->_check_watermarks;

    # A genuinely parked producer, awaiting the buffer to drain before its
    # next write -- must still resume when the connection tears down.
    my $parked = $conn->_wait_for_drain;
    ok(!$parked->is_ready, 'parked wait is pending before teardown (buffer still above low mark)');

    # Teardown: the buffer is still full (nothing has actually drained).
    $conn->_cancel_drain_waiters('connection closing');

    is($drain_fired, 0, 'on_drain did NOT fire on teardown (connection going away, not draining)');
    ok($parked->is_ready, 'the parked blocking wait resumed anyway (no coroutine leak)');
};

subtest 'h1 on_drain still fires normally when the buffer genuinely drains (not torn down)' => sub {
    my $fake_loop   = IO::Async::Loop->new;
    my $fake_server = Local::FakeServer->new($fake_loop);
    my $fake_stream = Local::FakeStream->new;

    my $conn = PAGI::Server::Connection->new(
        stream => $fake_stream, server => $fake_server, app => sub { },
        write_high_watermark => 10, write_low_watermark => 2,
    );

    $fake_stream->{writequeue} = [ Local::FakeWriter->new('x' x 100) ];

    my $transport = $conn->_h1_transport_state;
    my $drain_fired = 0;
    $transport->on_drain(sub { $drain_fired++ });
    $transport->_check_watermarks;

    my $parked = $conn->_wait_for_drain;

    # The buffer actually falls back below the low mark -- a real drain, the
    # same event on_outgoing_empty reports on a live stream.
    $fake_stream->{writequeue} = [];
    $conn->_check_drain_waiters;

    is($drain_fired, 1, 'on_drain fired on a genuine drain');
    ok($parked->is_ready, 'the parked blocking wait also resolved');
};

done_testing;
