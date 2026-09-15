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
# Case 1 (peer Close while parked) is the load-bearing regression: on h1 the
# Close parser cached the disconnect event and set the disconnect-handled guard
# but never marked the scope's connection state, so on_complete never fired for
# a parked app. Case 2 (abrupt drop while parked) shares the harness and guards
# the abnormal path against regressing -- it takes the transport-close route,
# not the Close parser, so it is expected to pass on base as well as after.
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
sub build_parked_app {
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

        $obs{parked} = 1;
        await $park;          # never calls receive() again; never drains
        $obs{app_returned} = 1;
        return;
    };

    return ($app, \%obs, $park);
}

# =============================================================================
# 1. h1 peer Close while parked -> on_complete, without the app returning.
#    RED on base (nothing fires); GREEN after the fix. Load-bearing.
# =============================================================================

subtest 'h1: a parked app is notified on_complete when the peer sends a Close' => sub {
    my ($app, $obs, $park) = build_parked_app();

    my $server = start_server($app);
    my $sock   = connect_client($server->port);

    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'websocket upgrade succeeded');
    ok(pump_until(sub { $obs->{parked} }, 5), 'app accepted and parked');

    # Peer sends a clean Close (code 1000, reason 'bye'). TCP is left open, so
    # the ONLY thing that can deliver the terminal notification is the Close
    # parser's mark -- not any later transport close.
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

    # Let the parked app return so the connection tears down cleanly.
    $park->done unless $park->is_ready;
    pump_until(sub { $obs->{app_returned} }, 3);

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
