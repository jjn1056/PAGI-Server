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
use Scalar::Util qw(refaddr);

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
# Test: One cached signal, a fresh observer per call
# =============================================================================

subtest 'each call returns its own observer of one cached signal' => sub {
    my $conn = PAGI::Server::ConnectionState->new();

    my $future1 = $conn->disconnect_future;
    my $future2 = $conn->disconnect_future;

    # Cancellation isolation (spec, Connection State) needs a distinct
    # observer per caller; the signal behind them is created once and
    # resolves them all. (Object identity is not the contract: Test2's is()
    # deep-compares pure-perl Futures and refaddr-compares Future::XS ones.)
    isnt(refaddr($future1), refaddr($future2), 'two calls return two observers');
    $conn->_mark_disconnected('client_closed');
    is([ $future1->get, $future2->get ], [ ('client_closed') x 2 ],
        'both observers resolve from the one signal');
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
    is($calls[0], ['cb1', 'timeout', undef], 'cb1 called with reason');
    is($calls[1], ['cb2', 'timeout', undef], 'cb2 called with reason');
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
# Test: response_complete accessor (boolean; true only after clean completion)
# =============================================================================

subtest 'response_complete is a boolean, true only after clean completion' => sub {
    my $conn = PAGI::Server::ConnectionState->new();

    ok($conn->can('response_complete'), 'response_complete method exists');

    my $result = eval { $conn->response_complete };
    ok(!$@, 'response_complete does not throw') or diag("error: $@");
    is($result, 0, 'response_complete returns 0 while active');

    $conn->_mark_disconnected('client_closed');
    is($conn->response_complete, 0, 'response_complete returns 0 after abnormal end');
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
        qr/_handle_disconnect\('protocol_error',/,
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

subtest 'response_complete is a boolean that becomes true only on clean completion' => sub {
    my $cs = PAGI::Server::ConnectionState->new;
    is($cs->response_complete, 0, 'false while active');
    $cs->_mark_complete;
    is($cs->response_complete, 1, 'true after clean completion');

    my $cs2 = PAGI::Server::ConnectionState->new;
    $cs2->_mark_disconnected('client_closed');
    is($cs2->response_complete, 0, 'false after abnormal end');
};

subtest 'disconnect_detail is recorded and delivered as the second callback argument' => sub {
    my $cs = PAGI::Server::ConnectionState->new;
    my @got;
    $cs->on_disconnect(sub { push @got, [@_] });
    is($cs->disconnect_detail, undef, 'undef while active');
    $cs->_mark_disconnected('protocol_error', 'RSV1 set on data frame');
    is($cs->disconnect_reason, 'protocol_error', 'token');
    is($cs->disconnect_detail, 'RSV1 set on data frame', 'detail accessor');
    is(\@got, [['protocol_error', 'RSV1 set on data frame']], 'callback got (reason, detail)');

    my @late;
    $cs->on_disconnect(sub { push @late, [@_] });
    is(\@late, [['protocol_error', 'RSV1 set on data frame']], 'late registration gets both arguments');
};

subtest 'callbacks are released after terminal delivery' => sub {
    my $cs = PAGI::Server::ConnectionState->new;
    my $cb = sub { 1 };
    $cs->on_disconnect($cb);
    $cs->on_complete($cb);
    $cs->_mark_complete;
    is(scalar @{$cs->{_callbacks}}, 0, 'on_disconnect list emptied');
    is(scalar @{$cs->{_complete_callbacks}}, 0, 'on_complete list emptied');
};

subtest 'abort calls the teardown hook once, marks app_abort with detail, and is idempotent' => sub {
    my @hook;
    my $cs = PAGI::Server::ConnectionState->new(on_abort => sub { push @hook, [@_] });
    my @cb;
    $cs->on_disconnect(sub { push @cb, [@_] });
    my $f = $cs->disconnect_future;

    $cs->abort('quota exceeded after 42 bytes');
    is(scalar @hook, 1, 'hook invoked once');
    isa_ok($hook[0][0], ['PAGI::Server::ConnectionState'], 'hook receives the object');
    is($hook[0][1], 'quota exceeded after 42 bytes', 'hook receives the detail');
    ok(!$cs->is_connected, 'not connected');
    is($cs->disconnect_reason, 'app_abort', 'token is app_abort');
    is($cs->disconnect_detail, 'quota exceeded after 42 bytes', 'detail recorded');
    ok($f->is_ready, 'disconnect_future resolved');
    is($f->get, 'app_abort', 'resolved with the token');
    is(\@cb, [['app_abort', 'quota exceeded after 42 bytes']], 'on_disconnect fired with both');

    $cs->abort('again');
    is(scalar @hook, 1, 'second abort is a no-op');
    is($cs->disconnect_detail, 'quota exceeded after 42 bytes', 'first outcome preserved');
};

subtest 'abort after clean completion is a no-op that preserves completion' => sub {
    my @hook;
    my $cs = PAGI::Server::ConnectionState->new(on_abort => sub { push @hook, [@_] });
    $cs->_mark_complete;
    $cs->abort('too late');
    is(scalar @hook, 0, 'hook not invoked');
    is($cs->response_complete, 1, 'still complete');
    is($cs->disconnect_reason, undef, 'still clean');
};

subtest 'abort without a hook still marks the state' => sub {
    my $cs = PAGI::Server::ConnectionState->new;
    $cs->abort;
    is($cs->disconnect_reason, 'app_abort', 'marked');
    is($cs->disconnect_detail, undef, 'no detail');
};

# =============================================================================
# Test: close_code / close_reason accessors and their setter
#
# Www.pod "Connection State": close_code/close_reason return the peer's
# WebSocket Close code and reason text, undef before any Close has been
# observed and on non-websocket scopes (which never call _set_ws_close). The
# server derives the code at the terminal site; the setter just stores it, and
# only while still connected -- once terminal the record is final, so a stale
# fallback or an abort cannot clobber a peer Close already recorded.
# =============================================================================

subtest 'close_code/close_reason are undef until a Close is recorded' => sub {
    my $cs = PAGI::Server::ConnectionState->new;
    is($cs->close_code,   undef, 'close_code undef on an open scope');
    is($cs->close_reason, undef, 'close_reason undef on an open scope');

    # A scope that never sees a peer Close (http/sse, or a clean end with no
    # Close) is never handed one, so both stay undef through completion.
    $cs->_mark_complete;
    is($cs->close_code,   undef, 'close_code still undef after a Close-less clean end');
    is($cs->close_reason, undef, 'close_reason still undef after a Close-less clean end');
};

subtest '_set_ws_close records the peer code and reason before the terminal mark' => sub {
    my $cs = PAGI::Server::ConnectionState->new;
    $cs->_set_ws_close(1000, 'bye');
    is($cs->close_code,   1000,  'close_code is the recorded peer code');
    is($cs->close_reason, 'bye', 'close_reason is the recorded peer text');

    # A codeless / reasonless Close is recorded as the derived code with undef.
    my $cs2 = PAGI::Server::ConnectionState->new;
    $cs2->_set_ws_close(1005, undef);
    is($cs2->close_code,   1005,  'close_code 1005 for a codeless peer Close');
    is($cs2->close_reason, undef, 'close_reason undef for a codeless peer Close');
};

subtest '_set_ws_close is a no-op once the scope is terminal (no clobber)' => sub {
    my $cs = PAGI::Server::ConnectionState->new;
    $cs->_set_ws_close(1000, 'bye');
    $cs->_mark_complete;

    # A later abnormal-end fallback (or an abort) must not overwrite the peer
    # Close already recorded before the mark.
    $cs->_set_ws_close(1006, undef);
    is($cs->close_code,   1000,  'close_code kept the peer code after terminal');
    is($cs->close_reason, 'bye', 'close_reason kept the peer text after terminal');
};

subtest 'abort does not clobber an already-recorded peer Close' => sub {
    my $cs = PAGI::Server::ConnectionState->new;
    $cs->_set_ws_close(1001, 'going');
    $cs->abort('teardown');
    is($cs->disconnect_reason, 'app_abort', 'abort marked app_abort');
    is($cs->close_code,   1001,    'close_code kept the peer code across abort');
    is($cs->close_reason, 'going', 'close_reason kept the peer text across abort');
};

# =============================================================================
# Test: on_end / end_future -- the all-outcome terminal observer (PAGI 0.6)
#
# on_end fires once for WHICHEVER terminal outcome occurred; end_future
# resolves (never fails) for either, with the reason token on an abnormal end
# and undef on a clean one. Both mirror on_disconnect/disconnect_future.
# =============================================================================

subtest 'on_end fires on a CLEAN end with (undef, undef); end_future resolves undef' => sub {
    my $cs = PAGI::Server::ConnectionState->new;
    my @end;
    my $completed  = 0;
    my $disconnected = 0;
    $cs->on_end(sub { push @end, [@_] });
    $cs->on_complete(sub { $completed = 1 });
    $cs->on_disconnect(sub { $disconnected = 1 });
    my $ef = $cs->end_future;

    $cs->_mark_complete;

    is(\@end, [[undef, undef]], 'on_end fired once with (undef, undef) on a clean end');
    ok($completed,     'on_complete also fired on a clean end');
    ok(!$disconnected, 'on_disconnect did NOT fire on a clean end');
    ok($ef->is_ready,  'end_future resolved on a clean end');
    is($ef->get, undef, 'end_future resolved with undef on a clean end');
};

subtest 'on_end fires on an ABNORMAL end with (token, detail); end_future resolves token' => sub {
    my $cs = PAGI::Server::ConnectionState->new;
    my @end;
    my $completed  = 0;
    my $disconnected = 0;
    $cs->on_end(sub { push @end, [@_] });
    $cs->on_complete(sub { $completed = 1 });
    $cs->on_disconnect(sub { $disconnected = 1 });
    my $ef = $cs->end_future;

    $cs->_mark_disconnected('protocol_error', 'RSV1 set on data frame');

    is(\@end, [['protocol_error', 'RSV1 set on data frame']],
        'on_end fired with (disconnect_reason, disconnect_detail)');
    is($cs->disconnect_reason, 'protocol_error', 'reason matches the on_end token');
    is($cs->disconnect_detail, 'RSV1 set on data frame', 'detail matches the on_end detail');
    ok($disconnected,  'on_disconnect also fired on an abnormal end');
    ok(!$completed,    'on_complete did NOT fire on an abnormal end');
    ok($ef->is_ready,  'end_future resolved on an abnormal end');
    is($ef->get, 'protocol_error', 'end_future resolved with the reason token');
};

subtest 'exactly one on_end per scope; registration-after-terminal fires immediately' => sub {
    # Clean terminal, then a stray disconnect: on_end fires exactly once.
    my $c1 = PAGI::Server::ConnectionState->new;
    my $count = 0;
    $c1->on_end(sub { $count++ });
    $c1->_mark_complete;
    $c1->_mark_disconnected('client_closed');   # no-op, must not re-fire
    is($count, 1, 'on_end fired exactly once across a terminal + stray transition');

    # Late registration on a clean scope fires immediately with (undef, undef).
    my @late_clean;
    $c1->on_end(sub { push @late_clean, [@_] });
    is(\@late_clean, [[undef, undef]], 'late on_end on a clean scope fires immediately with recorded values');

    # Late registration on an abnormal scope fires immediately with the record.
    my $c2 = PAGI::Server::ConnectionState->new;
    $c2->_mark_disconnected('write_error', 'EPIPE');
    my @late_abn;
    $c2->on_end(sub { push @late_abn, [@_] });
    is(\@late_abn, [['write_error', 'EPIPE']], 'late on_end on an abnormal scope fires immediately with the record');
};

subtest 'end_future resolves (never fails) on the abnormal path' => sub {
    my $cs = PAGI::Server::ConnectionState->new;
    my $ef = $cs->end_future;
    $cs->_mark_disconnected('client_closed');
    ok($ef->is_ready,    'end_future is ready after an abnormal end');
    ok(!$ef->is_failed,  'end_future resolved, did not fail');
    my $token = $ef->get;   # must return, not throw
    is($token, 'client_closed', 'awaiting the abnormal end_future returns the token');

    # A caller requesting end_future after the abnormal end also resolves.
    my $late = $cs->end_future;
    ok($late->is_ready, 'late end_future after abnormal end already resolved');
    is($late->get, 'client_closed', 'late end_future carries the token');
};

subtest 'end_future is cancellation-isolated' => sub {
    my $cs      = PAGI::Server::ConnectionState->new;
    my $sibling = $cs->end_future;               # obtained before any race
    my $victim  = $cs->end_future;
    my $work    = Future->new;
    my $race    = Future->wait_any($work, $victim);
    $work->done('won');                          # victim cancelled as loser

    ok($victim->is_cancelled, 'losing end_future observer was cancelled (alone)');
    ok(!$sibling->is_ready,   'sibling end_future observer unaffected by the race');

    # And the terminal signal, and other callbacks, survive intact.
    my @end;
    $cs->on_end(sub { push @end, [@_] });
    $cs->_mark_disconnected('client_closed');

    is($sibling->get, 'client_closed', 'sibling end_future still receives the reason');
    is(\@end, [['client_closed', undef]], 'on_end callback still fired after a losing race');
    is($cs->end_future->get, 'client_closed', 'a late end_future caller receives the reason');

    ok(lives { $cs->end_future->cancel }, 'direct cancel of an end_future observer is harmless');
    is($cs->end_future->get, 'client_closed', 'and the signal survives it');
};

subtest 'on_end callbacks are released after terminal delivery' => sub {
    my $cs = PAGI::Server::ConnectionState->new;
    $cs->on_end(sub { 1 });
    $cs->_mark_complete;
    is(scalar @{$cs->{_end_callbacks}}, 0, 'on_end list emptied after clean terminal');

    my $cs2 = PAGI::Server::ConnectionState->new;
    $cs2->on_end(sub { 1 });
    $cs2->_mark_disconnected('client_closed');
    is(scalar @{$cs2->{_end_callbacks}}, 0, 'on_end list emptied after abnormal terminal');
};

subtest 'one on_end callback failure does not stop the others' => sub {
    my $cs = PAGI::Server::ConnectionState->new;
    my @order;
    $cs->on_end(sub { push @order, 'first' });
    $cs->on_end(sub { die "boom\n" });
    $cs->on_end(sub { push @order, 'third' });

    # The failing callback logs via warn (no server); silence it for pristine output.
    my $warned = '';
    local $SIG{__WARN__} = sub { $warned .= $_[0] };
    $cs->_mark_complete;

    is(\@order, ['first', 'third'], 'callbacks after a failing one still ran');
    like($warned, qr/on_end callback error: boom/, 'the failure was logged');
};

done_testing;
