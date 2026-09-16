#!/usr/bin/env perl
use strict;
use warnings;
use Test2::V0;
use FindBin;
use lib "$FindBin::Bin/../lib";
use IO::Async::Loop;
use IO::Async::Timer::Countdown;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

use PAGI::Server;
use PAGI::Server::Connection;

# ============================================================================
# _enter_ws_closing_phase -- entering the WebSocket closing phase.
# ============================================================================
# When the application initiates a Close on an accepted socket, the scope
# enters a PENDING closing phase governed by a single finite close deadline
# (ws_close_timeout). Entering the phase ARMS that one deadline and then
# cancels the scope's activity timers (WebSocket keepalive/pong and idle), so
# the deadline is the sole bound and a shorter activity timer cannot
# short-circuit the wait with the wrong reason.
#
# The load-bearing safety property (design spec "Security invariant"): the
# deadline is armed BEFORE the activity timers are cancelled (arm-before-cancel).
# If arming ever failed after the activity timers were gone, the scope would be
# left waiting with no bound at all -- the unbounded wait the deadline exists to
# prevent. A guard makes "pending without a live deadline" a hard error rather
# than a silent leak.
#
# Task 2 builds the ENTRY only. The h1/h2 call sites that trigger the phase and
# the deadline RESOLUTION (clean / close_timeout / transport-loss) are Tasks
# 3-4, so these tests drive the helper directly with test-supplied
# cancel_timers / on_deadline callbacks (the h1 shape: scope == connection).

my $loop = IO::Async::Loop->new;

sub build_conn {
    my (%extra) = @_;
    my $server = PAGI::Server->new(
        app        => sub { },
        host       => '127.0.0.1',
        port       => 0,
        quiet      => 1,
        access_log => undef,
        %{ $extra{server} // {} },
    );
    $loop->add($server);
    my $conn = PAGI::Server::Connection->new(
        app      => sub { },
        protocol => undef,
        server   => $server,
        timeout  => $extra{timeout} // 60,
        ws_idle_timeout  => $extra{ws_idle_timeout} // 30,
        ws_close_timeout => $extra{ws_close_timeout} // 10,
    );
    return ($server, $conn);
}

subtest 'entry arms a live deadline, then cancels the activity timers' => sub {
    my ($server, $conn) = build_conn();

    # Start the scope's activity timers (h1 shape: all on the connection).
    $conn->_start_idle_timer;
    $conn->_start_ws_idle_timer;
    $conn->_start_ws_keepalive(30, 10);
    $conn->{ws_keepalive_timeout} = 10;
    $conn->_start_ws_pong_timeout;

    ok($conn->{idle_timer},         'idle timer live before the closing phase');
    ok($conn->{ws_idle_timer},      'ws idle timer live before the closing phase');
    ok($conn->{ws_keepalive_timer}, 'ws keepalive timer live before the closing phase');
    ok($conn->{ws_pong_timeout},    'ws pong timeout live before the closing phase');

    my $deadline_live_when_cancelling;
    $conn->_enter_ws_closing_phase($conn,
        on_deadline   => sub { },
        cancel_timers => sub {
            # arm-before-cancel: by the time the activity timers are cancelled,
            # the close deadline MUST already be governing the scope.
            $deadline_live_when_cancelling = $conn->_ws_close_deadline_live($conn);
            $conn->_stop_ws_keepalive;
            $conn->_stop_ws_idle_timer;
            $conn->_stop_idle_timer;
        },
    );

    ok($conn->{ws_closing}, 'the scope is marked as in the closing phase');
    ok($conn->_ws_close_deadline_live($conn), 'a live close deadline governs the phase');
    ok($deadline_live_when_cancelling,
        'the deadline was armed BEFORE the activity timers were cancelled');

    ok(!$conn->{idle_timer},         'idle timer cancelled on entering the phase');
    ok(!$conn->{ws_idle_timer},      'ws idle timer cancelled');
    ok(!$conn->{ws_keepalive_timer}, 'ws keepalive timer cancelled');
    ok(!$conn->{ws_pong_timeout},    'ws pong timeout cancelled');

    $loop->remove($server);
};

subtest 're-entering is a no-op: the deadline is armed exactly once' => sub {
    my ($server, $conn) = build_conn();

    my $arms = 0;
    my $orig = \&PAGI::Server::Connection::_arm_ws_close_deadline;
    no warnings 'redefine';
    local *PAGI::Server::Connection::_arm_ws_close_deadline = sub { $arms++; goto $orig };
    use warnings 'redefine';

    $conn->_enter_ws_closing_phase($conn, on_deadline => sub { }, cancel_timers => sub { });
    $conn->_enter_ws_closing_phase($conn, on_deadline => sub { }, cancel_timers => sub { });

    is($arms, 1, 'the close deadline is armed exactly once across re-entry');
    ok($conn->{ws_closing}, 'the scope stays in the closing phase');

    $loop->remove($server);
};

subtest 'an arm that throws leaves no scope pending-without-deadline' => sub {
    my ($server, $conn) = build_conn();
    $conn->_start_idle_timer;
    $conn->_start_ws_keepalive(30, 10);
    ok($conn->{idle_timer} && $conn->{ws_keepalive_timer}, 'activity timers live before entry');

    my $cancel_called = 0;
    no warnings 'redefine';
    local *PAGI::Server::Connection::_arm_ws_close_deadline = sub { die "arm boom\n" };
    use warnings 'redefine';

    my $err = dies {
        $conn->_enter_ws_closing_phase($conn,
            on_deadline   => sub { },
            cancel_timers => sub { $cancel_called = 1 });
    };

    like($err, qr/boom/, 'the arm failure propagated out of entry');
    ok(!$conn->{ws_closing}, 'the scope is NOT marked pending after a failed arm');
    ok(!$cancel_called, 'the activity timers were NOT cancelled (arm-before-cancel)');
    ok($conn->{idle_timer} && $conn->{ws_keepalive_timer},
        'the activity timers are still live -- the scope is still bounded');

    $conn->_stop_ws_keepalive;
    $conn->_stop_idle_timer;
    $loop->remove($server);
};

subtest 'a non-live deadline trips the guard before any timer is cancelled' => sub {
    my @logs;
    my ($server, $conn) = build_conn(server => {
        logger    => sub { push @logs, $_[0] },
        log_level => 'error',
    });
    $conn->_start_idle_timer;

    my $cancel_called = 0;
    no warnings 'redefine';
    # Arm returns a Countdown that was never added to a loop or started, so it
    # is not a live deadline: the guard must catch this.
    local *PAGI::Server::Connection::_arm_ws_close_deadline = sub {
        return IO::Async::Timer::Countdown->new(delay => 10, on_expire => sub { });
    };
    use warnings 'redefine';

    my $err = dies {
        $conn->_enter_ws_closing_phase($conn,
            on_deadline   => sub { },
            cancel_timers => sub { $cancel_called = 1 });
    };

    like($err, qr/without a live close deadline/, 'the guard fired and died');
    ok(!$conn->{ws_closing}, 'the scope is NOT marked pending when the deadline is not live');
    ok(!$cancel_called, 'the activity timers were NOT cancelled');
    ok($conn->{idle_timer}, 'the idle timer is still live -- the scope is still bounded');

    ok(scalar(@logs), 'the invariant violation was logged');
    is($logs[0]{level}, 'error', 'logged at error level');
    like($logs[0]{message}, qr/without a live close deadline/, 'the log names the invariant');

    $conn->_stop_idle_timer;
    $loop->remove($server);
};

done_testing;
