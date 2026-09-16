#!/usr/bin/env perl

# =============================================================================
# Test: h1 app-initiated WebSocket close waits for the peer, under a finite
# close deadline, instead of deciding a clean end eagerly at the app's send.
#
# Www.pod L857-869 / design spec "WebSocket Close Truthfulness": when the
# application sends websocket.close on an accepted socket, the scope enters a
# PENDING closing phase. A completed handshake -- the peer's Close AND the
# stream/transport then closing -- is the ONLY clean end. The wait is bounded
# by a finite close deadline (ws_close_timeout); its expiry with no peer Close
# is abnormal `close_timeout` / close_code 1006, and a transport drop with no
# peer Close is the transport-loss token / 1006 (NOT close_timeout). A peer
# Close observed before an abnormal outcome keeps its own code/reason.
#
# These cases are all APP-INITIATED (the handler sends websocket.close). The
# peer-initiated close (peer sends Close first) is a completed handshake at the
# reciprocal-Close point and stays clean -- covered by t/82 and t/83, not here.
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
sub make_websocket_frame {
    my ($opcode, $payload) = @_;
    my $frame = chr(0x80 | $opcode);
    my $len = length($payload);
    if ($len < 126) { $frame .= chr(0x80 | $len); }
    else            { $frame .= chr(0x80 | 126) . pack('n', $len); }
    my $mask = pack('N', int(rand(0xFFFFFFFF)));
    $frame .= $mask;
    my $masked = '';
    for my $i (0 .. length($payload) - 1) {
        $masked .= chr(ord(substr($payload, $i, 1)) ^ ord(substr($mask, $i % 4, 1)));
    }
    return $frame . $masked;
}

# ws_close_timeout is the finite bound on the closing wait. Deadline-expiry
# cases want a short one so the loop reaches expiry within a bounded pump; the
# clean / transport-drop cases resolve on a frame or the transport, not the
# deadline, so its value does not gate them.
sub start_server {
    my ($app, %opt) = @_;
    my $server = PAGI::Server->new(
        app              => $app,
        host             => '127.0.0.1',
        port             => 0,
        quiet            => 1,
        access_log       => (exists $opt{access_log} ? $opt{access_log} : undef),
        shutdown_timeout => 1,
        (exists $opt{ws_close_timeout} ? (ws_close_timeout => $opt{ws_close_timeout}) : ()),
        ($opt{logger} ? (logger => $opt{logger}) : ()),
        # An explicit log_level wins over quiet (PAGI::Server config), so a case
        # that wants to observe the close_timeout warn line can lower the
        # threshold below error while still routing lines to its own logger.
        (exists $opt{log_level} ? (log_level => $opt{log_level}) : ()),
    );
    $loop->add($server);
    $server->listen->get;
    return $server;
}

sub connect_client {
    my ($port) = @_;
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 2,
    ) or die "Cannot connect: $!";
    $sock->blocking(0);
    return $sock;
}

# Drive the loop until $cond is true or $timeout (wall-clock) expires; returns
# its final truth. Bounded pump, no sleeps.
sub pump_until {
    my ($cond, $timeout) = @_;
    $timeout //= 10;
    my $deadline = time + $timeout;
    while (time < $deadline) {
        return 1 if $cond->();
        $loop->loop_once(0.02);
    }
    return $cond->() ? 1 : 0;
}

# Turn the loop a bounded number of times so a scheduled loop->later (a terminal
# delivery) would have fired if one were pending. Used to prove a NON-event:
# after the peer Close is processed, no clean mark is scheduled.
sub pump_turns {
    my ($n) = @_;
    $loop->loop_once(0.005) for 1 .. $n;
}

sub shutdown_server {
    my ($server) = @_;
    eval { $server->shutdown->get };
    eval { $loop->remove($server) };
}

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
        my $buf; my $n = sysread($sock, $buf, 4096);
        $got .= $buf if defined $n && $n > 0;
        return $got =~ /HTTP\/1\.1 101/;
    }, 5);
    return $got;
}

# App builder. Options:
#   send_close  => bool (default 1)   the handler sends websocket.close
#   recv_after  => bool               after the close, do one receive() (drains
#                                     the peer's websocket.disconnect) then return
#   park        => bool               after the close, park forever (never return)
# Records terminal observations at the instant each notification fires.
sub build_app {
    my (%o) = @_;
    $o{send_close} //= 1;
    my %obs;
    my $park = $loop->new_future;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        my $conn = $scope->{'pagi.connection'};

        await $receive->();                              # websocket.connect
        await $send->({ type => 'websocket.accept' });

        $conn->on_complete(sub {
            $obs{complete}++;
            $obs{complete_connected} = $conn->is_connected ? 1 : 0;
            $obs{complete_reason}    = $conn->disconnect_reason;
            $obs{complete_code}      = $conn->close_code;
            $obs{complete_creason}   = $conn->close_reason;
        });
        $conn->on_disconnect(sub {
            my ($reason) = @_;
            $obs{disconnect}++;
            $obs{disconnect_reason}  = $reason;
            $obs{disconnect_code}    = $conn->close_code;
            $obs{disconnect_creason} = $conn->close_reason;
        });
        $conn->on_end(sub { $obs{end}++ });
        $conn->end_future->on_ready(sub { $obs{end_future}++ });

        if ($o{send_close}) {
            await $send->({ type => 'websocket.close', code => 1000, reason => 'bye' });
        }
        $obs{sent_close} = 1;

        if ($o{recv_after}) {
            # Snapshot the scope's state the instant the peer's Close is
            # observed: the disconnect event is delivered, but the scope is not
            # yet clean (still connected, on_complete not fired).
            my $d = await $receive->();
            $obs{recv_type}      = $d->{type};
            $obs{recv_connected} = $conn->is_connected ? 1 : 0;
            $obs{recv_complete}  = $obs{complete} // 0;
        }
        if ($o{park}) {
            $obs{parked} = 1;
            await $park;
        }
        $obs{app_returned} = 1;
        return;
    };
    return ($app, \%obs, $park);
}

# =============================================================================
# 1. handler RETURNS after Close; peer answers; transport closes -> CLEAN.
#    Split assertion: after the peer Close but before the transport closes the
#    scope is NOT yet clean; after the transport closes it is, close_code = peer.
# =============================================================================
subtest 'app close, peer Close, then transport close -> clean (two-step)' => sub {
    my ($app, $obs, $park) = build_app(recv_after => 1);
    my $server = start_server($app);
    my $sock   = connect_client($server->port);
    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'upgrade');
    ok(pump_until(sub { $obs->{sent_close} }, 5), 'app sent websocket.close');

    # Peer answers with its Close (code 1000, reason "bye"), TCP still open.
    syswrite($sock, make_websocket_frame(8, pack('n', 1000) . 'peerbye'));
    ok(pump_until(sub { $obs->{recv_type} }, 5), 'peer Close observed by the app');
    is($obs->{recv_type}, 'websocket.disconnect', 'the observed event is the peer disconnect');
    is($obs->{recv_connected}, 1, 'still connected when the peer Close arrived -- NOT yet clean');
    is($obs->{recv_complete}, 0,  'on_complete had NOT fired on the peer Close alone');
    pump_turns(20);
    ok(!$obs->{complete}, 'still not clean while the transport is open');

    # Transport then closes: the completed handshake is now a clean end.
    close($sock);
    ok(pump_until(sub { $obs->{complete} }, 5), 'on_complete fired once the transport closed');
    is($obs->{complete}, 1, 'on_complete fired exactly once');
    is($obs->{complete_connected}, 0, 'is_connected() false at the clean end');
    is($obs->{complete_reason}, undef, 'disconnect_reason() undef -- a clean end');
    is($obs->{complete_code}, 1000, 'close_code is the peer code');
    is($obs->{complete_creason}, 'peerbye', 'close_reason is the peer text');
    ok(!$obs->{disconnect}, 'on_disconnect did NOT fire');
    $park->done unless $park->is_ready;
    shutdown_server($server);
};

# =============================================================================
# 2. handler RETURNS after Close; peer never answers; deadline expires ->
#    close_timeout / 1006 / undef; on_disconnect + on_end + end_future; NOT
#    on_complete; on_end once.
# =============================================================================
subtest 'app close, peer silent, deadline expires -> close_timeout/1006' => sub {
    my ($app, $obs, $park) = build_app();
    my $server = start_server($app, ws_close_timeout => 0.3);
    my $sock   = connect_client($server->port);
    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'upgrade');
    ok(pump_until(sub { $obs->{app_returned} }, 5), 'app sent close and returned');

    ok(pump_until(sub { $obs->{disconnect} }, 5), 'on_disconnect fired at the deadline');
    is($obs->{disconnect}, 1, 'on_disconnect once');
    is($obs->{disconnect_reason}, 'close_timeout', 'disconnect_reason is close_timeout');
    is($obs->{disconnect_code}, 1006, 'close_code is 1006 (no peer Close)');
    is($obs->{disconnect_creason}, undef, 'close_reason undef');
    is($obs->{end}, 1, 'on_end fired exactly once');
    ok($obs->{end_future}, 'end_future resolved');
    ok(!$obs->{complete}, 'on_complete did NOT fire');
    close($sock);
    shutdown_server($server);
};

# =============================================================================
# 3. handler PARKS after Close (never returns, never receives); peer never
#    answers; deadline expires -> same as (2). THE ORIGINAL BUG CASE.
# =============================================================================
subtest 'app close then PARK, peer silent, deadline expires -> close_timeout/1006' => sub {
    my ($app, $obs, $park) = build_app(park => 1);
    my $server = start_server($app, ws_close_timeout => 0.3);
    my $sock   = connect_client($server->port);
    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'upgrade');
    ok(pump_until(sub { $obs->{parked} }, 5), 'app sent close and parked (never returns)');

    ok(pump_until(sub { $obs->{disconnect} }, 5), 'the parked scope still terminated at the deadline');
    is($obs->{disconnect_reason}, 'close_timeout', 'disconnect_reason is close_timeout');
    is($obs->{disconnect_code}, 1006, 'close_code 1006');
    is($obs->{end}, 1, 'on_end once');
    ok(!$obs->{complete}, 'on_complete did NOT fire');
    ok(!$obs->{app_returned}, 'the app never returned -- notice arrived without it draining');
    $park->done unless $park->is_ready;
    close($sock);
    shutdown_server($server);
};

# =============================================================================
# 4. handler PARKS after Close; peer answers; transport closes -> CLEAN.
# =============================================================================
subtest 'app close then PARK, peer Close, transport close -> clean' => sub {
    my ($app, $obs, $park) = build_app(park => 1);
    my $server = start_server($app);
    my $sock   = connect_client($server->port);
    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'upgrade');
    ok(pump_until(sub { $obs->{parked} }, 5), 'app sent close and parked');

    syswrite($sock, make_websocket_frame(8, pack('n', 1000) . 'peerbye'));
    pump_turns(30);   # let the server read + process the peer Close
    ok(!$obs->{complete}, 'not clean on the peer Close alone (transport still open)');

    close($sock);
    ok(pump_until(sub { $obs->{complete} }, 5), 'on_complete fired once the transport closed');
    is($obs->{complete_code}, 1000, 'close_code is the peer code');
    is($obs->{complete_reason}, undef, 'disconnect_reason undef -- clean');
    ok(!$obs->{disconnect}, 'on_disconnect did NOT fire');
    $park->done unless $park->is_ready;
    shutdown_server($server);
};

# =============================================================================
# 5. transport drops during the wait (no peer Close) -> transport-loss token
#    / 1006 (NOT close_timeout).
# =============================================================================
subtest 'app close, transport drops with no peer Close -> transport-loss/1006' => sub {
    my ($app, $obs, $park) = build_app(park => 1);
    my $server = start_server($app);   # default (long) deadline: the drop wins
    my $sock   = connect_client($server->port);
    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'upgrade');
    ok(pump_until(sub { $obs->{parked} }, 5), 'app sent close and parked');

    close($sock);   # abrupt transport drop, no Close frame
    ok(pump_until(sub { $obs->{disconnect} }, 5), 'on_disconnect fired on the drop');
    like($obs->{disconnect_reason}, qr/^(client_closed|read_error|write_error)$/,
        'a standard transport-loss token, NOT close_timeout');
    isnt($obs->{disconnect_reason}, 'close_timeout', 'specifically not close_timeout');
    is($obs->{disconnect_code}, 1006, 'close_code 1006');
    ok(!$obs->{complete}, 'on_complete did NOT fire');
    $park->done unless $park->is_ready;
    shutdown_server($server);
};

# =============================================================================
# 6. handler returns WITHOUT sending Close -> 1011 / server_error (unchanged).
# =============================================================================
subtest 'app returns without a Close -> server_error (unchanged)' => sub {
    my @logs;
    my ($app, $obs, $park) = build_app(send_close => 0);
    # This path deliberately triggers the server_error log; capture it via the
    # server's logger so the test's stderr stays pristine, and assert its text.
    my $server = start_server($app, logger => sub { push @logs, $_[0]->{message} // '' });
    my $sock   = connect_client($server->port);
    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'upgrade');

    ok(pump_until(sub { $obs->{disconnect} }, 5), 'on_disconnect fired');
    is($obs->{disconnect_reason}, 'server_error',
        'app that left an accepted socket without a closing handshake -> server_error');
    ok(!$obs->{complete}, 'on_complete did NOT fire');
    ok(scalar(grep { /returned from an accepted WebSocket without a closing handshake/ } @logs),
        'the expected server_error was logged (captured, not leaked)');
    close($sock);
    shutdown_server($server);
};

# =============================================================================
# 7. valid peer Close observed, then abnormal for another reason -> peer
#    code/reason PRESERVED (category 4). Here: peer Close, then the deadline
#    expires with the transport still open.
# =============================================================================
subtest 'peer Close then deadline expiry -> abnormal but peer code/reason preserved' => sub {
    my ($app, $obs, $park) = build_app(park => 1);
    my $server = start_server($app, ws_close_timeout => 0.4);
    my $sock   = connect_client($server->port);
    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'upgrade');
    ok(pump_until(sub { $obs->{parked} }, 5), 'app sent close and parked');

    # Peer sends a valid Close (1000/"seeya") but holds the transport open.
    syswrite($sock, make_websocket_frame(8, pack('n', 1000) . 'seeya'));
    pump_turns(20);   # process the peer Close; transport stays open
    ok(!$obs->{complete}, 'not clean -- transport never closed');

    # The deadline then expires: abnormal, but the peer values stand.
    ok(pump_until(sub { $obs->{disconnect} }, 5), 'on_disconnect fired at the deadline');
    is($obs->{disconnect_reason}, 'close_timeout', 'reason is close_timeout (abnormal)');
    is($obs->{disconnect_code}, 1000, 'close_code preserved from the peer Close');
    is($obs->{disconnect_creason}, 'seeya', 'close_reason preserved from the peer Close');
    ok(!$obs->{complete}, 'on_complete did NOT fire');
    $park->done unless $park->is_ready;
    close($sock);
    shutdown_server($server);
};

# =============================================================================
# 8. the normal close path never trips the pending-without-deadline guard
#    (Task 2 guard: die + error log). A normal app-initiated close is driven to
#    a clean end with NO invariant-violation logged.
# =============================================================================
subtest 'normal close path never trips the closing-phase guard' => sub {
    my @logs;
    my ($app, $obs, $park) = build_app();
    my $server = start_server($app, logger => sub { push @logs, $_[0]->{message} // '' });
    my $sock   = connect_client($server->port);
    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'upgrade');
    ok(pump_until(sub { $obs->{sent_close} }, 5), 'app sent websocket.close');

    # Peer answers and the transport closes: a normal, clean close.
    syswrite($sock, make_websocket_frame(8, pack('n', 1000) . 'ok'));
    pump_turns(20);
    close($sock);
    pump_until(sub { $obs->{complete} }, 5);

    ok($obs->{complete}, 'the normal close reached a clean end');
    ok(!scalar(grep { /bounded-wait invariant|without a live close deadline/ } @logs),
        'no pending-without-deadline guard was tripped on the normal path');
    $park->done unless $park->is_ready;
    shutdown_server($server);
};

# =============================================================================
# 9. Resolution BEFORE app-return: the closing resolution (here the deadline)
#    runs the access log + request accounting and closes the transport while the
#    handler is still parked; when the handler THEN returns, the app-return tail
#    must NOT run them a second time. Asserts exactly ONE access-log record.
#    Regression for the `ws_closing && !closed` tail guard double-executing.
# =============================================================================
subtest 'resolution before app-return logs the access record exactly once' => sub {
    my $access = '';
    open(my $afh, '>', \$access) or die "cannot open in-memory access log: $!";
    my ($app, $obs, $park) = build_app(park => 1);
    my $server = start_server($app, ws_close_timeout => 0.3, access_log => $afh);
    my $sock   = connect_client($server->port);
    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'upgrade');
    ok(pump_until(sub { $obs->{parked} }, 5), 'app sent close and parked');

    # The deadline resolves FIRST -- while the handler is still parked. The
    # resolution writes the access log and closes the transport (closed=1),
    # with ws_closing still set.
    ok(pump_until(sub { $obs->{disconnect} }, 5), 'resolved at the deadline before the app returned');
    ok(!$obs->{app_returned}, 'the handler had not returned yet');

    # NOW let the handler return: the tail runs with ws_closing=1 AND closed=1,
    # exactly the state the buggy `ws_closing && !closed` guard let through.
    $park->done unless $park->is_ready;
    ok(pump_until(sub { $obs->{app_returned} }, 5), 'handler returned after resolution');
    pump_turns(10);

    my @records = ($access =~ /"GET /g);
    is(scalar(@records), 1, 'exactly ONE access-log record for the connection (no duplicate)');
    close($sock);
    shutdown_server($server);
};

# =============================================================================
# 10. SHUTDOWN PREEMPTS the in-flight close-wait. A scope parked in the
#     app-initiated closing phase when the server begins its graceful drain must
#     resolve with `server_shutdown` (the drain's ending), dispose the close
#     deadline, and NOT wait the full ws_close_timeout. Here ws_close_timeout is
#     far longer than shutdown_timeout, so a resolution that named close_timeout
#     or blocked on the deadline would be the bug. Terminal shape asserted:
#     on_disconnect + on_end (once) + end_future, NOT on_complete.
# =============================================================================
subtest 'server shutdown preempts the in-flight close-wait -> server_shutdown' => sub {
    my ($app, $obs, $park) = build_app(park => 1);
    # ws_close_timeout 30s: far beyond the 1s shutdown_timeout, so only a real
    # preemption (not the deadline, not the force-close) can end this quickly.
    my $server = start_server($app, ws_close_timeout => 30);
    my $sock   = connect_client($server->port);
    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'upgrade');
    ok(pump_until(sub { $obs->{parked} }, 5), 'app sent close and parked (peer silent)');

    # Reach the live connection and its armed deadline BEFORE the drain runs.
    my ($conn) = values %{$server->{connections}};
    ok($conn, 'the closing-phase connection is registered with the server');
    my $deadline = $conn->{ws_close_deadline};
    ok($deadline && $deadline->is_running, 'the close deadline is armed and live pre-shutdown');
    ok(!$obs->{disconnect}, 'still PENDING before the drain (no terminal yet)');

    # Begin the graceful drain: shutdown_server calls $server->shutdown->get,
    # which runs _drain_connections. A closing-phase WebSocket is long-lived, so
    # the drain closes it at once with server_shutdown.
    $park->done unless $park->is_ready;
    shutdown_server($server);

    ok($obs->{disconnect}, 'the pending scope was resolved by the drain');
    is($obs->{disconnect}, 1, 'on_disconnect fired exactly once');
    is($obs->{disconnect_reason}, 'server_shutdown',
        'reason is server_shutdown -- the drain preempted the close-wait, NOT close_timeout');
    isnt($obs->{disconnect_reason}, 'close_timeout', 'specifically not close_timeout');
    is($obs->{disconnect_code}, 1006, 'close_code 1006 (no peer Close)');
    is($obs->{end}, 1, 'on_end fired exactly once');
    ok($obs->{end_future}, 'end_future resolved');
    ok(!$obs->{complete}, 'on_complete did NOT fire');

    # Deadline disposed by the terminal path: the one-shot timer is dequeued
    # from the scope (only _dispose_ws_close_deadline deletes it) and stopped,
    # so it can neither fire late nor leak past the drain.
    ok(!$conn->{ws_close_deadline}, 'the close deadline was disposed (dequeued from the scope)');
    ok(!$deadline->is_running, 'the close deadline timer was stopped (no late fire)');
    close($sock);
};

# =============================================================================
# 11. Observability: the close_timeout resolution emits one structured warn
#     line so an operator can watch the new abnormal-close population. The line
#     is warn level, so a server at the default/quiet (error) threshold stays
#     silent; a server dropped to info routes it to its logger. Asserts the line
#     names close_timeout and the bound.
# =============================================================================
subtest 'close_timeout resolution emits a structured observability log line' => sub {
    my @logs;
    my ($app, $obs, $park) = build_app(park => 1);
    my $server = start_server($app,
        ws_close_timeout => 0.3,
        log_level        => 'info',
        logger           => sub { push @logs, $_[0]->{message} // '' });
    my $sock = connect_client($server->port);
    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'upgrade');
    ok(pump_until(sub { $obs->{parked} }, 5), 'app sent close and parked');

    ok(pump_until(sub { $obs->{disconnect} }, 5), 'the deadline resolved close_timeout');
    is($obs->{disconnect_reason}, 'close_timeout', 'reason is close_timeout');

    my @hits = grep { /close_timeout/ && /close deadline/i } @logs;
    is(scalar(@hits), 1, 'exactly one structured close_timeout log line was emitted');
    like($hits[0], qr/1006/, 'the line records the abnormal close_code 1006');
    like($hits[0], qr/0\.3/, 'the line records the ws_close_timeout bound that elapsed');
    $park->done unless $park->is_ready;
    close($sock);
    shutdown_server($server);
};

done_testing;
