#!/usr/bin/env perl

# =============================================================================
# Test: PAGI::Server::ConnectionState
#
# Verifies the connection state tracking object:
# 1. Initial state is connected
# 2. Disconnect transitions state correctly
# 3. on_disconnect callbacks work as expected
# 4. disconnect_future resolves correctly
# 5. Error handling in callbacks
# =============================================================================

use strict;
use warnings;
use Test2::V0;
use Future;

require PAGI::Server::ConnectionState;
require PAGI::Server::Connection;

# =============================================================================
# Test: Initial state
# =============================================================================

subtest 'initial state' => sub {
    my $conn = PAGI::Server::ConnectionState->new();

    ok($conn->is_connected, 'initially connected');
    is($conn->disconnect_reason, undef, 'no reason while connected');
    # disconnect_future is lazily created - always returns a Future
    my $future = $conn->disconnect_future;
    ok($future, 'disconnect_future returns a Future');
    ok(!$future->is_ready, 'future not ready while connected');
};

# =============================================================================
# Test: Lazy Future is cached
# =============================================================================

subtest 'lazy future is cached' => sub {
    my $conn = PAGI::Server::ConnectionState->new();

    my $future1 = $conn->disconnect_future;
    my $future2 = $conn->disconnect_future;

    ok($future1, 'first call returns Future');
    ok($future2, 'second call returns Future');
    is($future1, $future2, 'same Future returned on subsequent calls');
};

# =============================================================================
# Test: Disconnect transitions state
# =============================================================================

subtest 'disconnect transitions state' => sub {
    my $conn = PAGI::Server::ConnectionState->new();

    $conn->_mark_disconnected('client_closed');

    ok(!$conn->is_connected, 'not connected after disconnect');
    is($conn->disconnect_reason, 'client_closed', 'reason is set');
};

# =============================================================================
# Test: Multiple disconnect calls are no-op
# =============================================================================

subtest 'multiple disconnects are no-op' => sub {
    my $conn = PAGI::Server::ConnectionState->new();
    my $cb_count = 0;

    $conn->on_disconnect(sub { $cb_count++ });

    $conn->_mark_disconnected('client_closed');
    $conn->_mark_disconnected('write_error');  # Should be ignored

    is($cb_count, 1, 'callback only invoked once');
    is($conn->disconnect_reason, 'client_closed', 'original reason preserved');
};

# =============================================================================
# Test: on_disconnect callbacks
# =============================================================================

subtest 'on_disconnect callbacks' => sub {
    my $conn = PAGI::Server::ConnectionState->new();
    my @calls;

    $conn->on_disconnect(sub { push @calls, ['cb1', @_] });
    $conn->on_disconnect(sub { push @calls, ['cb2', @_] });

    is(scalar @calls, 0, 'no calls while connected');

    $conn->_mark_disconnected('timeout');

    is(scalar @calls, 2, 'both callbacks invoked');
    is($calls[0], ['cb1', 'timeout'], 'cb1 called with reason');
    is($calls[1], ['cb2', 'timeout'], 'cb2 called with reason');
};

# =============================================================================
# Test: on_disconnect after already disconnected
# =============================================================================

subtest 'on_disconnect after already disconnected' => sub {
    my $conn = PAGI::Server::ConnectionState->new();
    $conn->_mark_disconnected('client_closed');

    my $called = 0;
    my $reason;
    $conn->on_disconnect(sub { $called = 1; $reason = $_[0] });

    ok($called, 'callback invoked immediately');
    is($reason, 'client_closed', 'reason passed');
};

# =============================================================================
# Test: disconnect_future resolves
# =============================================================================

subtest 'disconnect_future resolves' => sub {
    my $conn = PAGI::Server::ConnectionState->new();

    my $future = $conn->disconnect_future;
    ok(!$future->is_ready, 'future pending initially');

    $conn->_mark_disconnected('write_error');

    ok($future->is_ready, 'future resolved after disconnect');
    is($future->get, 'write_error', 'future resolved with reason');
};

# =============================================================================
# Test: disconnect_future called after disconnect resolves immediately
# =============================================================================

subtest 'disconnect_future called after disconnect resolves immediately' => sub {
    my $conn = PAGI::Server::ConnectionState->new();

    # Disconnect first, before calling disconnect_future
    $conn->_mark_disconnected('client_closed');

    # Now get the future - should be created and immediately resolved
    my $future = $conn->disconnect_future;
    ok($future, 'future created even after disconnect');
    ok($future->is_ready, 'future is already resolved');
    is($future->get, 'client_closed', 'future resolved with reason');
};

# =============================================================================
# Test: Callback errors do not break others
# =============================================================================

subtest 'callback errors do not break others' => sub {
    my $conn = PAGI::Server::ConnectionState->new();
    my $cb2_called = 0;

    $conn->on_disconnect(sub { die "error in cb1" });
    $conn->on_disconnect(sub { $cb2_called = 1 });

    # Should not die, should warn
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, @_ };

    $conn->_mark_disconnected('test');

    ok($cb2_called, 'cb2 still called despite cb1 error');
    like($warnings[0], qr/callback error/, 'warning emitted');
};

# =============================================================================
# Test: Callback error when registering after disconnect
# =============================================================================

subtest 'callback error when registering after disconnect' => sub {
    my $conn = PAGI::Server::ConnectionState->new();
    $conn->_mark_disconnected('client_closed');

    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, @_ };

    $conn->on_disconnect(sub { die "error in late callback" });

    like($warnings[0], qr/callback error/, 'warning emitted for late callback');
};

# =============================================================================
# Test: All standard disconnect reasons
# =============================================================================

subtest 'standard disconnect reasons' => sub {
    my @reasons = qw(
        client_closed
        client_timeout
        idle_timeout
        write_timeout
        write_error
        read_error
        protocol_error
        server_shutdown
        body_too_large
    );

    for my $reason (@reasons) {
        my $conn = PAGI::Server::ConnectionState->new();
        $conn->_mark_disconnected($reason);
        is($conn->disconnect_reason, $reason, "reason '$reason' preserved");
    }
};

# =============================================================================
# Test: Disconnect without calling disconnect_future - no Future created
# =============================================================================

subtest 'disconnect without calling disconnect_future' => sub {
    my $conn = PAGI::Server::ConnectionState->new();

    # Disconnect without ever calling disconnect_future
    # This should work and not create any Future
    $conn->_mark_disconnected('test');

    ok(!$conn->is_connected, 'disconnected');
    is($conn->disconnect_reason, 'test', 'reason set');

    # Now if we call disconnect_future, it should be created resolved
    my $future = $conn->disconnect_future;
    ok($future->is_ready, 'late future is already resolved');
};

# =============================================================================
# Test: Completion is distinct from disconnect (on_complete / _mark_complete)
# =============================================================================

subtest 'mark_complete fires on_complete, not on_disconnect' => sub {
    my $conn = PAGI::Server::ConnectionState->new();

    my @complete;
    my $disconnected = 0;
    $conn->on_complete(sub { push @complete, [@_] });
    $conn->on_disconnect(sub { $disconnected = 1 });

    is(scalar @complete, 0, 'on_complete not fired while in-flight');

    $conn->_mark_complete;

    is(scalar @complete, 1, 'on_complete fired exactly once');
    ok(!$disconnected, 'on_disconnect NOT fired on clean completion');
    ok(!$conn->is_connected, 'no longer connected after completion');
    is($conn->disconnect_reason, undef, 'disconnect_reason stays undef on completion');
};

subtest 'disconnect_future does not resolve on completion' => sub {
    my $conn = PAGI::Server::ConnectionState->new();
    my $future = $conn->disconnect_future;

    $conn->_mark_complete;

    ok(!$future->is_ready, 'disconnect_future remains pending after clean completion');
};

subtest 'disconnect_future requested after clean completion stays pending' => sub {
    my $conn = PAGI::Server::ConnectionState->new;
    $conn->_mark_complete;
    my $f = $conn->disconnect_future;
    ok( $f, 'future returned' );
    ok( !$f->is_ready, 'deliberately left pending — completion is not a disconnect' );

    my $conn2 = PAGI::Server::ConnectionState->new;
    $conn2->_mark_disconnected('client_closed');
    my $f2 = $conn2->disconnect_future;
    ok( $f2->is_ready, 'after abnormal disconnect: already resolved' );
    is( $f2->get, 'client_closed', 'carries the reason' );
};

subtest 'on_complete after already completed fires immediately' => sub {
    my $conn = PAGI::Server::ConnectionState->new();
    $conn->_mark_complete;

    my $called = 0;
    $conn->on_complete(sub { $called = 1 });

    ok($called, 'late on_complete callback invoked immediately');
};

subtest 'completion and disconnect are mutually exclusive terminal states' => sub {
    # complete first, then a stray disconnect is a no-op
    my $c1 = PAGI::Server::ConnectionState->new();
    my $disc = 0;
    $c1->on_disconnect(sub { $disc = 1 });
    $c1->_mark_complete;
    $c1->_mark_disconnected('client_closed');   # must be ignored
    ok(!$disc, 'on_disconnect not fired after completion');
    is($c1->disconnect_reason, undef, 'no reason after completion + stray disconnect');

    # disconnect first, then a stray completion is a no-op
    my $c2 = PAGI::Server::ConnectionState->new();
    my $comp = 0;
    $c2->on_complete(sub { $comp = 1 });
    $c2->_mark_disconnected('write_error');
    $c2->_mark_complete;                          # must be ignored
    ok(!$comp, 'on_complete not fired after abnormal disconnect');
    is($c2->disconnect_reason, 'write_error', 'disconnect reason preserved');
};

subtest 'on_disconnect after completion does not fire' => sub {
    my $conn = PAGI::Server::ConnectionState->new();
    $conn->_mark_complete;

    my $called = 0;
    $conn->on_disconnect(sub { $called = 1 });

    ok(!$called, 'on_disconnect registered after clean completion never fires');
};

subtest 'on_complete callback errors do not break others' => sub {
    my $conn = PAGI::Server::ConnectionState->new();
    my $cb2_called = 0;

    $conn->on_complete(sub { die "error in cb1" });
    $conn->on_complete(sub { $cb2_called = 1 });

    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, @_ };

    $conn->_mark_complete;

    ok($cb2_called, 'cb2 still called despite cb1 error');
    like($warnings[0], qr/callback error/, 'warning emitted');
};

# =============================================================================
# Test: _handle_disconnect marks connection state BEFORE resuming parked
# drain waiters. A producer parked on a blocking backpressure await
# (_wait_for_drain) can be resumed synchronously the moment its Future is
# resolved (Future::AsyncAwait resumes inline off ->done, same as ->on_ready
# below); if that resumption races ahead of the connection-state marking, the
# app observes is_connected() == 1 immediately after its own disconnect was
# detected. Exercises the real Connection._handle_disconnect ordering
# directly (unit-level, no live socket needed).
# =============================================================================

subtest 'disconnect marks connection state before resuming parked drain waiters' => sub {
    my $conn = PAGI::Server::Connection->new(app => sub { });
    my $conn_state = PAGI::Server::ConnectionState->new(connection => $conn);
    $conn->{current_connection_state} = $conn_state;

    # A producer parked on a blocking backpressure await -- pushed directly
    # onto _drain_waiters (the same queue _wait_for_drain uses), without
    # needing a real stream/buffer to get there.
    my $parked = Future->new;
    push @{$conn->{_drain_waiters}}, $parked;

    my ($observed_connected, $observed_reason);
    $parked->on_ready(sub {
        # Fires synchronously from within _handle_disconnect below, exactly
        # as an awaiting coroutine resumes -- this is the resumed app's very
        # first chance to look at its own connection state.
        $observed_connected = $conn_state->is_connected;
        $observed_reason    = $conn_state->disconnect_reason;
    });

    $conn->_handle_disconnect('client_closed');

    is($observed_connected, 0,
        'resumed waiter observes is_connected already false (no stale-true window)');
    is($observed_reason, 'client_closed',
        'resumed waiter observes disconnect_reason already set');
};

# =============================================================================
# Test: response_complete accessor (SHOULD-level; unsupported -> undef)
# =============================================================================

subtest 'response_complete is undef (unsupported)' => sub {
    my $conn = PAGI::Server::ConnectionState->new();

    ok($conn->can('response_complete'), 'response_complete method exists');

    my $result = eval { $conn->response_complete };
    ok(!$@, 'response_complete does not throw') or diag("error: $@");
    is($result, undef, 'response_complete returns undef (unsupported)');
};

# =============================================================================
# Test: Server implements disconnect reason code paths (source inspection)
# =============================================================================

subtest 'server implements disconnect reason code paths' => sub {
    # Read the Connection.pm source
    my $source = do {
        open my $fh, '<', 'lib/PAGI/Server/Connection.pm' or die "Cannot read: $!";
        local $/;
        <$fh>;
    };

    # Verify protocol_error is set on parse failures
    like(
        $source,
        qr/_handle_disconnect\('protocol_error'\)/,
        'protocol_error reason used for parse failures'
    );

    # Verify server_shutdown auto-detection exists
    like(
        $source,
        qr/server_shutdown/,
        'server_shutdown reason is referenced'
    );

    like(
        $source,
        qr/\$self->\{server\}\{shutting_down\}/,
        'server shutdown state is checked'
    );
};

# =============================================================================
# Test: disconnect_future is cancellation-isolated (PAGI 0.002007)
#
# The spec's Connection State section: cancelling a returned Future --
# directly, or as the losing component of a combinator such as
# Future->wait_any -- must not affect the server's disconnect processing
# or any Future returned by another call.
# =============================================================================

subtest 'disconnect_future is cancellation-isolated' => sub {
    my $conn    = PAGI::Server::ConnectionState->new();
    my $sibling = $conn->disconnect_future;          # obtained before any race
    my $victim  = $conn->disconnect_future;
    my $work    = Future->new;
    my $race    = Future->wait_any($work, $victim);
    $work->done('won');                              # victim cancelled as loser

    ok($victim->is_cancelled, 'losing observer was cancelled (alone)');
    ok(!$sibling->is_ready,   'sibling observer unaffected by the race');

    $conn->_mark_disconnected('client_closed');
    is($sibling->get, 'client_closed', 'sibling still receives the reason');
    is($conn->disconnect_future->get, 'client_closed',
        'a late caller receives the reason');

    ok(lives { $conn->disconnect_future->cancel },
        'direct cancel of an observer is harmless');
    is($conn->disconnect_future->get, 'client_closed',
        'and the signal survives it');
};

done_testing;
