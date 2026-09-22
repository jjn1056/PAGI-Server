#!/usr/bin/env perl

# =============================================================================
# Test: a parked accepted-WebSocket app is told the scope ended, on h1, even
# though it never returns and never drains receive() again.
#
# Www.pod "Observing the end of a scope" / "Callback invocation context": the
# terminal notification (on_complete / on_disconnect) is delivered on every
# ending "even if the transport is torn down first", and it does not depend on
# the application consuming the receive() queue. A completed WebSocket closing
# handshake is a clean end (on_complete; is_connected() false;
# disconnect_reason() undef); an abrupt drop with no handshake is an abnormal
# end (on_disconnect with the standard token).
#
# The app here accepts, registers both callbacks on pagi.connection, then parks
# on a Future that never resolves -- it never calls receive() again and never
# returns while the test observes it. The only thing that can deliver its
# terminal notification is the server's own frame/transport handling.
#
# Case 1 (peer Close while parked) pins the server-owned close (RFC 6455 7.1.1,
# WS-CLOSE-TRUTH-3): the peer's Close plus the server's reciprocal Close is a
# completed handshake, and the SERVER then closes the transport; on_complete
# fires for the parked app AT that server-driven transport closure, with the
# client never closing TCP. Case 1b is the same for a handler that returns
# immediately after the disconnect (rather than parking). Case 2 (abrupt drop
# while parked) shares the harness and guards the abnormal path against
# regressing -- an abrupt drop with no handshake is on_disconnect / the standard
# token.
# =============================================================================

use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
use Future;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";

use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

my $loop = IO::Async::Loop->new;

# A masked WebSocket frame (client -> server frames MUST be masked, RFC 6455).
# Lifted from t/59-ws-disconnect-exactly-once.t.
sub make_websocket_frame {
    my ($opcode, $payload) = @_;

    my $frame = chr(0x80 | $opcode);

    my $len = length($payload);
    if ($len < 126) {
        $frame .= chr(0x80 | $len);
    }
    else {
        $frame .= chr(0x80 | 126) . pack('n', $len);
    }

    my $mask = pack('N', int(rand(0xFFFFFFFF)));
    $frame .= $mask;

    my $masked_payload = '';
    for my $i (0 .. length($payload) - 1) {
        $masked_payload .= chr(ord(substr($payload, $i, 1)) ^ ord(substr($mask, $i % 4, 1)));
    }
    $frame .= $masked_payload;

    return $frame;
}

# A short shutdown timeout: the app under test deliberately parks on a Future
# that never resolves, so a graceful drain would otherwise wait out the default
# 30s. The test resolves the park Future itself once its assertions are made,
# so this is only a backstop.
sub start_server {
    my ($app) = @_;
    my $server = PAGI::Server->new(
        app             => $app,
        host            => '127.0.0.1',
        port            => 0,
        quiet           => 1,
        access_log      => undef,
        shutdown_timeout => 1,
    );
    $loop->add($server);
    $server->listen->get;
    return $server;
}

sub connect_client {
    my ($port) = @_;
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1',
        PeerPort => $port,
        Proto    => 'tcp',
        Timeout  => 2,
    ) or die "Cannot connect: $!";
    $sock->blocking(0);
    return $sock;
}

# Drive the loop until $cond is true or $timeout expires; returns its final
# truth.
sub pump_until {
    my ($cond, $timeout) = @_;
    $timeout //= 10;
    my $deadline = time + $timeout;
    while (time < $deadline) {
        return 1 if $cond->();
        $loop->loop_once(0.05);
    }
    return $cond->() ? 1 : 0;
}

sub shutdown_server {
    my ($server) = @_;
    eval { $server->shutdown->get };
    eval { $loop->remove($server) };
}

# Drain any server bytes on $sock and report whether the SERVER has closed its
# end (a defined sysread of 0 bytes is EOF). Proves server-owned closure
# (RFC 6455 7.1.1): the client only reads and never closes TCP itself.
sub server_closed_transport {
    my ($sock) = @_;
    my $buf;
    my $n = sysread($sock, $buf, 4096);
    return (defined $n && $n == 0) ? 1 : 0;
}

# Perform the h1 WebSocket upgrade on $sock and pump until the 101 is seen.
sub ws_upgrade {
    my ($sock) = @_;
    my $key = 'dGhlIHNhbXBsZSBub25jZQ==';
    syswrite($sock,
          "GET / HTTP/1.1\r\n"
        . "Host: localhost\r\n"
        . "Upgrade: websocket\r\n"
        . "Connection: Upgrade\r\n"
        . "Sec-WebSocket-Key: $key\r\n"
        . "Sec-WebSocket-Version: 13\r\n"
        . "\r\n");

    my $got = '';
    pump_until(sub {
        my $buf;
        my $n = sysread($sock, $buf, 4096);
        $got .= $buf if defined $n && $n > 0;
        return $got =~ /HTTP\/1\.1 101/;
    }, 5);
    return $got;
}

# Build the parked app and the observation hash + park Future it closes over.
# One instance per subtest (the server takes one app per connection).
#   drain_return => 1  after the callbacks are registered, do ONE receive()
#                      (drains the peer's websocket.disconnect) then return
#                      immediately, instead of parking forever. Exercises a
#                      handler that returns right after the disconnect.
sub build_parked_app {
    my (%opt) = @_;
    my %obs;
    my $park = $loop->new_future;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        my $conn = $scope->{'pagi.connection'};

        await $receive->();                          # websocket.connect
        await $send->({ type => 'websocket.accept' });

        # Read the object's own state at the instant each notification fires,
        # so the assertions pin the transition, not a later read.
        $conn->on_complete(sub {
            $obs{complete}++;
            $obs{complete_connected} = $conn->is_connected ? 1 : 0;
            $obs{complete_reason}    = $conn->disconnect_reason;
        });
        $conn->on_disconnect(sub {
            my ($reason, $detail) = @_;
            $obs{disconnect}++;
            $obs{disconnect_connected} = $conn->is_connected ? 1 : 0;
            $obs{disconnect_reason}    = $reason;
        });

        if ($opt{drain_return}) {
            my $d = await $receive->();              # the peer's disconnect
            $obs{drained_type} = $d->{type};
            $obs{app_returned} = 1;
            return;
        }

        $obs{parked} = 1;
        await $park;          # never calls receive() again; never drains
        $obs{app_returned} = 1;
        return;
    };

    return ($app, \%obs, $park);
}

# =============================================================================
# 1. h1 peer Close while parked -> on_complete at the SERVER-driven transport
#    closure, without the app returning and without the client closing TCP.
#    RED on base (nothing fires); GREEN after the fix. Load-bearing.
# =============================================================================

subtest 'h1: a parked app is notified on_complete when the peer sends a Close' => sub {
    my ($app, $obs, $park) = build_parked_app();

    my $server = start_server($app);
    my $sock   = connect_client($server->port);

    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'websocket upgrade succeeded');
    ok(pump_until(sub { $obs->{parked} }, 5), 'app accepted and parked');

    # Peer sends a clean Close (code 1000, reason 'bye') then only reads: it
    # never closes TCP. The completed handshake makes the SERVER close the
    # transport (RFC 6455 7.1.1, WS-CLOSE-TRUTH-3), and on_complete fires at that
    # server-driven closure.
    syswrite($sock, make_websocket_frame(8, pack('n', 1000) . 'bye'));

    ok(pump_until(sub { $obs->{complete} }, 5),
        'on_complete fired for the parked app without it returning');
    is($obs->{complete}, 1, 'on_complete fired exactly once');
    is($obs->{complete_connected}, 0, 'is_connected() was false when on_complete fired');
    is($obs->{complete_reason}, undef,
        'disconnect_reason() was undef -- a completed handshake is a clean end');
    ok(!$obs->{disconnect}, 'on_disconnect did NOT fire (clean end, not abnormal)');
    ok(!$obs->{app_returned},
        'the app never returned -- the notice arrived without it draining receive()');
    # Drain the server's reciprocal Close frame, then observe its FIN: the client
    # never closed TCP, so a server EOF proves the server owns the closure.
    ok(pump_until(sub { server_closed_transport($sock) }, 3),
        'the SERVER closed the transport (client saw EOF, never closed TCP)');

    # Let the parked app return so the connection tears down cleanly.
    $park->done unless $park->is_ready;
    pump_until(sub { $obs->{app_returned} }, 3);

    close($sock);
    shutdown_server($server);
};

# =============================================================================
# 1b. h1 peer Close, handler RETURNS immediately after draining the disconnect
#     (does not park) -> on_complete at the server-driven transport closure.
#     The returns-immediately twin of case 1 on the peer-initiated path.
# =============================================================================

subtest 'h1: a handler that returns right after the peer Close reaches a clean end' => sub {
    my ($app, $obs, $park) = build_parked_app(drain_return => 1);

    my $server = start_server($app);
    my $sock   = connect_client($server->port);

    # The 101 means the app has accepted and is now awaiting the peer disconnect.
    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'websocket upgrade succeeded');

    syswrite($sock, make_websocket_frame(8, pack('n', 1000) . 'bye'));

    pump_until(sub { $obs->{complete} || $obs->{disconnect} }, 5);

    is($obs->{drained_type}, 'websocket.disconnect', 'the handler drained the peer disconnect');
    ok($obs->{app_returned}, 'the handler returned immediately after the disconnect');
    ok($obs->{complete}, 'on_complete fired -- clean end at the server-driven closure');
    is($obs->{complete_reason}, undef, 'disconnect_reason() undef -- clean');
    ok(!$obs->{disconnect}, 'on_disconnect did NOT fire');
    ok(pump_until(sub { server_closed_transport($sock) }, 3),
        'the SERVER closed the transport (client saw EOF)');

    $park->done unless $park->is_ready;
    close($sock);
    shutdown_server($server);
};

# =============================================================================
# 2. h1 abrupt drop while parked -> on_disconnect with the standard token,
#    without the app returning. Expected to pass on base and after (this path
#    is the transport close, not the Close parser); guards the fix against
#    regressing the abnormal path.
# =============================================================================

subtest 'h1: a parked app is notified on_disconnect when the peer drops abruptly' => sub {
    my ($app, $obs, $park) = build_parked_app();

    my $server = start_server($app);
    my $sock   = connect_client($server->port);

    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'websocket upgrade succeeded');
    ok(pump_until(sub { $obs->{parked} }, 5), 'app accepted and parked');

    # Peer drops the TCP connection with no Close frame at all.
    close($sock);

    ok(pump_until(sub { $obs->{disconnect} }, 5),
        'on_disconnect fired for the parked app without it returning');
    is($obs->{disconnect}, 1, 'on_disconnect fired exactly once');
    is($obs->{disconnect_connected}, 0, 'is_connected() was false when on_disconnect fired');
    like($obs->{disconnect_reason}, qr/^(client_closed|read_error|write_error)$/,
        'on_disconnect carries a standard abnormal-end token');
    ok(!$obs->{complete}, 'on_complete did NOT fire (abnormal end, not clean)');
    ok(!$obs->{app_returned},
        'the app never returned -- the notice arrived without it draining receive()');

    $park->done unless $park->is_ready;
    pump_until(sub { $obs->{app_returned} }, 3);

    shutdown_server($server);
};

done_testing;
