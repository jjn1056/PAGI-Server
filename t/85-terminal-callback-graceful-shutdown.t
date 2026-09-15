#!/usr/bin/env perl

# =============================================================================
# Test: terminal callbacks are delivered during a graceful shutdown, not dropped.
#
# L<PAGI::Spec::Www>, "Callback invocation context" / "Observing the end of a
# scope": on_disconnect / on_end (and disconnect_future / end_future) are
# delivered on EVERY ending, "even if the transport is torn down first". The
# server-driven teardown of a graceful shutdown -- the drain closing idle and
# long-lived connections, and the shutdown_timeout force-closing a still-busy
# one -- is such an ending. It runs from the server's own stack (never inside an
# application's $send/$receive) and the loop stops right after, so those marks
# deliver their terminal callbacks SYNCHRONOUSLY: they must have fired by the
# time $server->shutdown->get returns, with no further pumping.
#
# Regression (S5): terminal delivery had moved onto loop->later unconditionally.
# On this teardown path loop->later never runs before the loop stops, so the
# callbacks were silently dropped. This test pins the fix on both a
# websocket scope closed by the drain and an http scope cut short by the
# force-close, and guards the app-path deferral (an abort) against regressing.
#
# Bounded pumps, no wall-clock sleeps; the only wait is the server's own short
# shutdown_timeout on the force-close case.
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

sub start_server {
    my (%args) = @_;
    my $server = PAGI::Server->new(
        app              => $args{app},
        host             => '127.0.0.1',
        port             => 0,
        quiet            => 1,
        access_log       => undef,
        shutdown_timeout => $args{shutdown_timeout} // 1,
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

# Drive the loop until $cond is true or $timeout expires; returns its final truth.
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

# =============================================================================
# 1. Graceful drain, websocket scope: the drain closes the long-lived
#    connection, and on_disconnect / on_end (and disconnect_future /
#    end_future) have all fired by the time shutdown->get returns -- no extra
#    pump. RED on base (deferred via loop->later, dropped); GREEN after.
# =============================================================================

subtest 'graceful drain (websocket scope) delivers the terminal callbacks synchronously' => sub {
    my %obs;
    my $park = $loop->new_future;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        my $conn = $scope->{'pagi.connection'};

        await $receive->();                          # websocket.connect
        await $send->({ type => 'websocket.accept' });

        $conn->on_disconnect(sub { $obs{disconnect}++; $obs{disconnect_reason} = $_[0]; });
        $conn->on_end(sub { $obs{end}++; });
        $obs{df} = $conn->disconnect_future;
        $obs{ef} = $conn->end_future;

        $obs{parked} = 1;
        await $park;                                 # never drains receive() again
        return;
    };

    my $server = start_server(app => $app, shutdown_timeout => 1);
    my $sock   = connect_client($server->port);

    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'websocket upgrade succeeded');
    ok(pump_until(sub { $obs{parked} }, 5), 'app accepted and parked');

    # The load-bearing step: shut down and then read the observation with NO
    # further pumping. Pre-fix the callbacks were scheduled on loop->later and
    # the loop stopped before it ran, so nothing below would be set.
    $server->shutdown->get;

    is($obs{disconnect}, 1, 'on_disconnect fired by the time shutdown returned (no extra pump)');
    is($obs{end},        1, 'on_end fired by the time shutdown returned (no extra pump)');
    like($obs{disconnect_reason}, qr/^server_shutdown$/, 'on_disconnect carries server_shutdown');
    ok($obs{df} && $obs{df}->is_ready, 'disconnect_future resolved by the time shutdown returned');
    ok($obs{ef} && $obs{ef}->is_ready, 'end_future resolved by the time shutdown returned');

    $park->done unless $park->is_ready;
    eval { $loop->remove($server) };
    close($sock);
};

# =============================================================================
# 2. shutdown_timeout force-close, http scope: a still-busy request is force
#    closed when the drain times out, and its terminal callbacks fire
#    synchronously on that path too -- fired by the time shutdown->get returns.
# =============================================================================

subtest 'shutdown_timeout force-close (http scope) delivers the terminal callbacks synchronously' => sub {
    my %obs;
    my $park = $loop->new_future;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'http';
        my $conn = $scope->{'pagi.connection'};

        $conn->on_disconnect(sub { $obs{disconnect}++; $obs{disconnect_reason} = $_[0]; });
        $conn->on_end(sub { $obs{end}++; });
        $obs{df} = $conn->disconnect_future;
        $obs{ef} = $conn->end_future;

        # Never finish the response: the connection stays "in flight" so the
        # drain leaves it to the timeout force-close rather than closing it idle.
        $obs{parked} = 1;
        await $park;
        return;
    };

    my $server = start_server(app => $app, shutdown_timeout => 0.3);
    my $sock   = connect_client($server->port);
    syswrite($sock, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");

    ok(pump_until(sub { $obs{parked} }, 5), 'app began handling the request and parked');

    # shutdown->get blocks out the short shutdown_timeout, then force-closes the
    # still-busy connection synchronously; the callbacks must have fired by the
    # time it returns, again with no extra pump.
    $server->shutdown->get;

    is($obs{disconnect}, 1, 'on_disconnect fired by the time shutdown returned (no extra pump)');
    is($obs{end},        1, 'on_end fired by the time shutdown returned (no extra pump)');
    like($obs{disconnect_reason}, qr/^server_shutdown$/, 'on_disconnect carries server_shutdown');
    ok($obs{df} && $obs{df}->is_ready, 'disconnect_future resolved by the time shutdown returned');
    ok($obs{ef} && $obs{ef}->is_ready, 'end_future resolved by the time shutdown returned');

    $park->done unless $park->is_ready;
    eval { $loop->remove($server) };
    close($sock);
};

# =============================================================================
# 3. S5 guard: a NORMAL (non-shutdown) abnormal end still defers to a LATER loop
#    turn. The app aborts its own scope and reads on_disconnect synchronously
#    right after -- it must NOT have fired yet (the app-path deferral is intact),
#    then it fires on a later turn. This is the behavior the fix must not
#    regress: only the server-driven teardown delivers synchronously.
# =============================================================================

subtest 'app-path abnormal end (abort) still delivers on a later loop turn' => sub {
    my %obs;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'http';
        my $conn = $scope->{'pagi.connection'};

        $conn->on_disconnect(sub { $obs{disconnect}++; $obs{disconnect_reason} = $_[0]; });

        $conn->abort('boom');

        # Read synchronously, right after abort() returns: the fact is immediate,
        # the callback delivery is deferred to a later loop turn.
        $obs{connected_after_abort} = $conn->is_connected ? 1 : 0;
        $obs{reason_after_abort}    = $conn->disconnect_reason;
        $obs{fired_after_abort}     = $obs{disconnect} // 0;
        $obs{observed}              = 1;
        return;
    };

    my $server = start_server(app => $app, shutdown_timeout => 1);
    my $sock   = connect_client($server->port);
    syswrite($sock, "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");

    ok(pump_until(sub { $obs{observed} }, 5), 'app aborted and made its synchronous observation');

    # The fact is immediate at the transition.
    is($obs{connected_after_abort}, 0, 'is_connected() was false synchronously after abort');
    is($obs{reason_after_abort}, 'app_abort', 'disconnect_reason() was app_abort synchronously after abort');

    # Delivery is deferred: the callback had not run at that synchronous instant.
    is($obs{fired_after_abort}, 0, 'on_disconnect had NOT fired synchronously inside the abort call');

    # It runs on a later loop turn.
    ok(pump_until(sub { $obs{disconnect} }, 5), 'on_disconnect fired on a later loop turn');
    is($obs{disconnect}, 1, 'on_disconnect fired exactly once');
    is($obs{disconnect_reason}, 'app_abort', 'on_disconnect carried the app_abort token');

    eval { $server->shutdown->get };
    eval { $loop->remove($server) };
    close($sock);
};

done_testing;
