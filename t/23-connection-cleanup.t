use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use IO::Socket::INET;
use Future::AsyncAwait;
use Scalar::Util qw(refaddr weaken);
use Socket qw(AF_UNIX SOCK_STREAM);
use FindBin;
use lib "$FindBin::Bin/../lib";

use PAGI::Server;
use PAGI::Server::Connection;

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

# =============================================================================
# Test: socket-error handlers (spec tokens read_error/write_error).
#
# IO::Async::Stream's own contract (perldoc IO::Async::Stream, "on_read_error"
# / "on_write_error"): "If an error occurs when the corresponding error
# callback is not supplied, ... the close method is called instead" --
# confirmed in source (_do_read / _do_write):
#   $self->maybe_invoke_event(on_read_error => $errno) or $self->close_now;
# maybe_invoke_event always returns a (truthy) arrayref once ANY handler is
# registered, regardless of what that handler itself returns, so simply
# registering on_read_error/on_write_error is sufficient by itself to
# suppress IO::Async's own close_now -- there is no risk of double-teardown
# from IO::Async's side; our handler becomes solely responsible for tearing
# the connection down, via the same _handle_disconnect_and_close every other
# reason already uses (whose _disconnect_handled guard is independently
# idempotent against any other path -- e.g. on_closed -- that might also
# fire afterward).
#
# A live EPIPE/ECONNRESET provocation (RST-closing the client socket via
# SO_LINGER=>0, then forcing a server write) was attempted first, per the
# audit brief, using several variants (immediate release, delayed release,
# large single write, many small writes). On this platform/sandbox the
# server's on_read handler's EOF/close detection reliably wins the race
# against any write-side failure -- every variant tried settled on
# client_closed (or a same-tick 200, meaning the write itself succeeded
# despite the peer RST) rather than a genuine write-side error, so this pins
# the fix with a unit-level handler invocation instead, per the brief's own
# fallback allowance: a real Connection, with start() actually called (so
# on_read_error/on_write_error are registered by the real production code,
# not hand-rolled by the test), against a real IO::Async::Stream backed by a
# socketpair -- invoke_event() then fires the exact registered handler with
# a given errno, deterministically.
# =============================================================================

sub _start_real_connection_for_error_test {
    my $fake_loop = IO::Async::Loop->new;
    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $sock_a->blocking(0);
    $sock_b->blocking(0);
    my $stream = IO::Async::Stream->new(
        read_handle => $sock_a, write_handle => $sock_a, on_read => sub { 0 },
    );
    $fake_loop->add($stream);

    my $fake_server = Local::FakeServer->new($fake_loop);
    my $conn = PAGI::Server::Connection->new(
        stream => $stream, server => $fake_server, app => sub { },
        transport_type => 'unix',   # skip the TCP_NODELAY/peerhost probing start() does for tcp
        timeout => 0,               # skip idle-timer setup (no add_child($timer) needed)
    );
    $conn->start;

    return ($conn, $stream, $sock_b);
}

subtest 'on_read_error is registered and reports read_error' => sub {
    my ($conn, $stream, $peer) = _start_real_connection_for_error_test();

    my @captured;
    no warnings 'redefine';
    my $orig = \&PAGI::Server::Connection::_handle_disconnect_and_close;
    local *PAGI::Server::Connection::_handle_disconnect_and_close = sub {
        my ($self, $reason) = @_;
        push @captured, $reason;
        return $orig->(@_);
    };

    ok($stream->can_event('on_read_error'), 'start() registered on_read_error on the real stream');

    my $result = eval { $stream->invoke_event('on_read_error', 104); 1 };   # 104 = ECONNRESET-like errno
    ok($result, 'invoking on_read_error does not throw') or diag("error: $@");
    is($captured[0], 'read_error', 'on_read_error reports the read_error reason');
};

subtest 'on_write_error is registered and reports write_error' => sub {
    my ($conn, $stream, $peer) = _start_real_connection_for_error_test();

    my @captured;
    no warnings 'redefine';
    my $orig = \&PAGI::Server::Connection::_handle_disconnect_and_close;
    local *PAGI::Server::Connection::_handle_disconnect_and_close = sub {
        my ($self, $reason) = @_;
        push @captured, $reason;
        return $orig->(@_);
    };

    ok($stream->can_event('on_write_error'), 'start() registered on_write_error on the real stream');

    my $result = eval { $stream->invoke_event('on_write_error', 32); 1 };   # 32 = EPIPE-like errno
    ok($result, 'invoking on_write_error does not throw') or diag("error: $@");
    is($captured[0], 'write_error', 'on_write_error reports the write_error reason');
};

subtest 'a write_error is not overwritten by a subsequent on_closed (first reason wins, no double-teardown)' => sub {
    my ($conn, $stream, $peer) = _start_real_connection_for_error_test();
    my $conn_state = PAGI::Server::ConnectionState->new(connection => $conn);
    $conn->{current_connection_state} = $conn_state;

    my @captured;
    no warnings 'redefine';
    my $orig = \&PAGI::Server::Connection::_handle_disconnect_and_close;
    local *PAGI::Server::Connection::_handle_disconnect_and_close = sub {
        my ($self, $reason) = @_;
        push @captured, $reason;
        return $orig->(@_);
    };

    $stream->invoke_event('on_write_error', 32);

    # Simulate IO::Async also delivering on_closed once the socket actually
    # finishes closing (the same harmless double-call pattern every other
    # disconnect reason already tolerates, per _disconnect_handled's
    # idempotency guard) -- it must not clobber the real reason.
    $stream->invoke_event('on_closed') if $stream->can_event('on_closed');

    is($captured[0], 'write_error', 'the write_error call is the first _handle_disconnect_and_close call');
    is($conn_state->disconnect_reason, 'write_error',
        'connection state still reports write_error -- the later on_closed call was a no-op (idempotency guard held)');
};

done_testing;
