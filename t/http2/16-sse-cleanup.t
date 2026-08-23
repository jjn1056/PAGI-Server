use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../../lib";
use Socket qw(AF_UNIX SOCK_STREAM);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.008+ required)');
}

# ============================================================
# Test: SSE Cleanup and Disconnect Handling over HTTP/2
# ============================================================
# Verifies that client disconnect, connection close, and stream
# close during SSE are handled cleanly without crashes.

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;

my $loop = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# ============================================================
# Helpers
# ============================================================

sub create_test_server {
    my (%args) = @_;
    my $server = PAGI::Server->new(
        app   => $args{app} // sub { },
        host  => '127.0.0.1',
        port  => 0,
        quiet => 1,
        http2 => 1,
        %args,
    );
    $loop->add($server);
    return $server;
}

sub create_h2c_connection {
    my (%overrides) = @_;

    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0)
        or die "socketpair: $!";
    $sock_a->blocking(0);
    $sock_b->blocking(0);

    my $app = $overrides{app} // sub { };
    my $server = $overrides{server} // create_test_server(app => $app, %overrides);

    my $stream = IO::Async::Stream->new(
        read_handle  => $sock_a,
        write_handle => $sock_a,
        on_read => sub { 0 },
    );

    my $conn = PAGI::Server::Connection->new(
        stream           => $stream,
        app              => $app,
        protocol         => $protocol,
        server           => $server,
        h2_protocol      => $server->{http2_protocol},
        h2c_enabled      => $server->{h2c_enabled},
        max_body_size    => $server->{max_body_size},
        sse_idle_timeout => $server->{sse_idle_timeout},
    );

    $server->add_child($stream);
    $conn->start;

    return ($conn, $stream, $sock_b, $server);
}

sub create_client {
    my (%overrides) = @_;
    require Net::HTTP2::nghttp2::Session;
    return Net::HTTP2::nghttp2::Session->new_client(
        callbacks => {
            on_begin_headers   => $overrides{on_begin_headers}   // sub { 0 },
            on_header          => $overrides{on_header}          // sub { 0 },
            on_frame_recv      => $overrides{on_frame_recv}      // sub { 0 },
            on_data_chunk_recv => $overrides{on_data_chunk_recv} // sub { 0 },
            on_stream_close    => $overrides{on_stream_close}    // sub { 0 },
        },
    );
}

sub h2c_handshake {
    my ($client, $client_sock) = @_;
    $client->send_connection_preface;
    my $data = $client->mem_send;
    $client_sock->syswrite($data);
    for (1..5) {
        $loop->loop_once(0.1);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length($out);
    }
}

sub exchange_frames {
    my ($client, $client_sock, $rounds) = @_;
    $rounds //= 10;
    for (1..$rounds) {
        $loop->loop_once(0.1);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length($out);
    }
}

# ============================================================
# Client disconnect during SSE -> sse.disconnect
# ============================================================
subtest 'client disconnect during SSE delivers sse.disconnect' => sub {
    my $sse_started = 0;
    my $disconnect_event;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();

        await $send->({ type => 'sse.start', status => 200 });
        await $send->({ type => 'sse.send', data => 'hello' });
        $sse_started = 1;

        # Wait for disconnect
        my $event = await $receive->();
        $disconnect_event = $event;
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(app => $app);

    my $client = create_client();
    h2c_handshake($client, $client_sock);

    $client->submit_request(
        method    => 'GET',
        path      => '/events',
        scheme    => 'http',
        authority => 'localhost',
        headers   => [['accept', 'text/event-stream']],
    );
    $client_sock->syswrite($client->mem_send);

    # Wait for SSE to start
    for (1..20) {
        $loop->loop_once(0.1);
        last if $sse_started;
    }

    # Close client side to simulate disconnect
    close($client_sock);

    # Let event loop process the disconnection
    for (1..10) {
        $loop->loop_once(0.1);
    }

    is($disconnect_event->{type}, 'sse.disconnect', 'Got sse.disconnect event');
    is($disconnect_event->{reason}, 'client_closed', 'Reason is client_closed');

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# Connection close during SSE does not crash
# ============================================================
subtest 'connection close during SSE does not crash' => sub {
    my $sse_started = 0;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();

        await $send->({ type => 'sse.start', status => 200 });
        await $send->({ type => 'sse.send', data => 'first' });
        $sse_started = 1;

        # The connection will be closed by the test.
        # Subsequent sends should not crash.
        eval {
            await $send->({ type => 'sse.send', data => 'second' });
        };
        eval {
            await $send->({ type => 'sse.send', data => 'third' });
        };
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(app => $app);

    my $client = create_client();
    h2c_handshake($client, $client_sock);

    $client->submit_request(
        method    => 'GET',
        path      => '/events',
        scheme    => 'http',
        authority => 'localhost',
        headers   => [['accept', 'text/event-stream']],
    );
    $client_sock->syswrite($client->mem_send);

    # Wait for SSE to start
    for (1..20) {
        $loop->loop_once(0.1);
        last if $sse_started;
    }

    # Close connection
    close($client_sock);

    # Let event loop process
    for (1..10) {
        $loop->loop_once(0.1);
    }

    pass('No crash on connection close during SSE');

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# SSE with keepalive + disconnect: timers cleaned up
# ============================================================
subtest 'keepalive timers cleaned up on disconnect' => sub {
    my $sse_started = 0;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();

        await $send->({ type => 'sse.start', status => 200 });
        await $send->({
            type     => 'sse.keepalive',
            interval => 0.1,
            comment  => 'ka',
        });
        await $send->({ type => 'sse.send', data => 'with-keepalive' });
        $sse_started = 1;

        # Wait for disconnect
        await $receive->();
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(app => $app);

    my $client = create_client();
    h2c_handshake($client, $client_sock);

    $client->submit_request(
        method    => 'GET',
        path      => '/events',
        scheme    => 'http',
        authority => 'localhost',
        headers   => [['accept', 'text/event-stream']],
    );
    $client_sock->syswrite($client->mem_send);

    # Wait for SSE to start with keepalive
    for (1..20) {
        $loop->loop_once(0.1);
        last if $sse_started;
    }

    # Let a few keepalive ticks fire
    exchange_frames($client, $client_sock, 5);

    # Close connection
    close($client_sock);

    # Let event loop process cleanup
    for (1..10) {
        $loop->loop_once(0.1);
    }

    # If we got here without a crash, timers were cleaned up properly
    pass('No crash after disconnect with active keepalive timer');

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# max_body_size 413 during active SSE: per-stream timers must not leak
# ============================================================
# Regression test: the h2 max_body_size (413) path in _h2_on_body deletes
# $self->{h2_streams}{$stream_id} without first stopping the stream's SSE
# keepalive (or idle) timers. Those timers are add_child'ed to the SERVER,
# not to the per-stream state, so once the hash entry is gone nothing else
# reclaims them: _h2_on_close never runs for this path (no h2-level stream
# close event fires here), and the connection's own close-time sweep
# iterates h2_streams, which by then no longer lists this stream. Left
# unfixed, the timer keeps firing for the life of the process.
subtest 'max_body_size 413 during active SSE stops the stream keepalive timer' => sub {
    my $sse_started = 0;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;

        # Deliberately does NOT call receive() first: the h2 SSE receive()
        # contract only resolves its first call once body_complete is true
        # (see _h2_create_sse_receive), and this test's client body stays
        # open (streamed via submit_data below, never EOF) so that the
        # server is still accumulating body bytes when the overrun hits.
        # Calling receive() first would block the app forever and the
        # keepalive would never get armed.
        await $send->({ type => 'sse.start', status => 200 });
        await $send->({
            type     => 'sse.keepalive',
            interval => 0.05,
            comment  => 'ka',
        });
        $sse_started = 1;

        # Nothing else to do: the client will overrun max_body_size and the
        # server tears the stream down without this app ever calling
        # receive() at all.
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(
        app           => $app,
        max_body_size => 40,
    );

    my $client = create_client();
    h2c_handshake($client, $client_sock);

    my $stream_id = $client->submit_request(
        method    => 'GET',
        path      => '/events',
        scheme    => 'http',
        authority => 'localhost',
        headers   => [['accept', 'text/event-stream']],
        body      => sub { return undef },  # streaming: keep open, fed manually below
    );
    $client_sock->syswrite($client->mem_send);

    # Wait for SSE to start with keepalive armed.
    for (1..20) {
        $loop->loop_once(0.1);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length($out);
        last if $sse_started;
    }
    ok($sse_started, 'SSE started with keepalive armed before body overruns max_body_size');

    my $ss = $conn->{h2_streams}{$stream_id};
    ok($ss, 'server holds per-stream state for the SSE stream');

    my $timer = $ss->{sse_ka_timer};
    ok($timer && $timer->is_running, 'per-stream keepalive timer is armed before the overrun');

    # Push body data past max_body_size (40 bytes) while the SSE response is
    # already active.
    $client->submit_data($stream_id, ('X' x 100), 0);
    $client_sock->syswrite($client->mem_send);

    for (1..10) {
        $loop->loop_once(0.1);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length($out);
    }

    ok(!exists $conn->{h2_streams}{$stream_id},
        'stream entry reclaimed after max_body_size 413');
    ok(!$timer->is_running,
        'the stream keepalive timer was stopped, not leaked, by the 413 teardown');

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# max_body_size 413 with an SSE app parked on receive() BEFORE any send:
# no spurious synthesized-500 warning
# ============================================================
# SSE (like WebSocket) never attaches a connection_state, so the dispatch
# wrapper's client-already-gone carve-out for these scopes keys off a
# liveness fact (entry-exists-AND-not-h2_closed), not $cs->disconnect_reason.
# Regression: the 413 overrun branch in _h2_on_body used to wake a parked
# receive() (and, transitively, let the app's async sub resume synchronously
# and return) before marking the stream h2_closed -- so by the time the
# dispatch wrapper's liveness check ran (nested inside that very wake), it
# still saw a "live" entry and, since this app never called send() (so
# response_started is false), warned "returned without starting a response"
# and tried to synthesize a 500 over a stream the client already lost via
# 413.
subtest 'max_body_size 413 with SSE app parked on receive() before any send: no spurious 500 warn' => sub {
    my ($receive_resolved, $receive_event);
    my @app_warnings;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        # Calls receive() first, with no prior send() -- response_started
        # stays false, which is what exercises the dispatch wrapper's
        # synthesize-a-500 branch if client_gone is computed wrong.
        $receive_event = await $receive->();
        $receive_resolved = 1;
        # Returns without ever calling send().
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(
        app           => $app,
        max_body_size => 40,
    );

    my $client = create_client();
    h2c_handshake($client, $client_sock);

    my $stream_id = $client->submit_request(
        method    => 'GET',
        path      => '/events-early-receive',
        scheme    => 'http',
        authority => 'localhost',
        headers   => [['accept', 'text/event-stream']],
        body      => sub { return undef },  # streaming: keep open, fed manually below
    );
    $client_sock->syswrite($client->mem_send);

    local $SIG{__WARN__} = sub { push @app_warnings, $_[0] };

    # Poll (bounded) until the app is dispatched and parked on receive()
    # (body_pending armed but not yet resolved).
    my $dispatched = 0;
    for (1..20) {
        $loop->loop_once(0.1);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length($out);
        my $ss = $conn->{h2_streams}{$stream_id};
        if ($ss && $ss->{body_pending} && !$ss->{body_pending}->is_ready) {
            $dispatched = 1;
            last;
        }
    }
    ok($dispatched, 'app dispatched and parked on receive() before the overrun')
        or diag('app never reached the parked-receive state -- cannot exercise wake-on-overrun');

    # Push body data past max_body_size (40 bytes).
    $client->submit_data($stream_id, ('X' x 100), 0);
    $client_sock->syswrite($client->mem_send);

    # Bounded wait for the parked receive() to resolve.
    my $settled = 0;
    for (1..20) {
        $loop->loop_once(0.1);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length($out);
        if ($receive_resolved) {
            $settled = 1;
            last;
        }
    }
    ok($settled, 'pending receive() resolved within the bounded wait')
        or diag('receive_resolved=' . ($receive_resolved // 0));

    is($receive_event->{type}, 'sse.disconnect', 'receive resolved to sse.disconnect');
    ok(!(grep { /returned without starting a response/ } @app_warnings),
        'dispatch wrapper did not synthesize a spurious 500 warning for this client-gone (413) SSE stream')
        or diag(join('', @app_warnings));

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# Server-initiated SSE idle timeout -> sse.disconnect reason=idle_timeout
# ============================================================
# Distinguishes a server-initiated teardown from a client one: the client
# never sends or closes anything here, so the ONLY thing that can end the
# stream is the server's own per-stream idle timer. The stream ends via a
# clean END_STREAM (design section 11.3's "end THIS stream only"), which
# _h2_on_close's error-code-0 arms must attribute to 'idle_timeout' via
# server_close_reason, not the generic 'client_closed' fallback.
subtest 'server-initiated SSE idle timeout delivers sse.disconnect reason=idle_timeout' => sub {
    my $sse_started = 0;
    my $disconnect_event;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();

        await $send->({ type => 'sse.start', status => 200 });
        $sse_started = 1;

        # Never send again -- the per-stream idle timer, not the client,
        # ends this stream.
        my $event = await $receive->();
        $disconnect_event = $event;
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(
        app              => $app,
        sse_idle_timeout => 0.3,
    );

    my $client = create_client();
    h2c_handshake($client, $client_sock);

    $client->submit_request(
        method    => 'GET',
        path      => '/events',
        scheme    => 'http',
        authority => 'localhost',
        headers   => [['accept', 'text/event-stream']],
    );
    $client_sock->syswrite($client->mem_send);

    for (1..20) {
        $loop->loop_once(0.1);
        last if $sse_started;
    }
    ok($sse_started, 'SSE stream started before the idle timer can expire');

    # Idle timeout is 0.3s; each exchange_frames round is ~0.1s, so ~3
    # rounds are expected. 30 rounds (~3s) is a 10x margin.
    for (1..30) {
        exchange_frames($client, $client_sock, 1);
        last if $disconnect_event;
    }

    ok($disconnect_event, 'sse.disconnect delivered within the bounded window');
    is($disconnect_event->{type}, 'sse.disconnect', 'Got sse.disconnect event')
        if $disconnect_event;
    is($disconnect_event->{reason}, 'idle_timeout',
        "Reason is 'idle_timeout', not misattributed to 'client_closed'")
        if $disconnect_event;

    $stream_io->close_now;
    $loop->remove($server);
};

done_testing;
