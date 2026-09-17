#!/usr/bin/env perl

# =============================================================================
# Test: h1 app-initiated WebSocket close waits for the peer under a finite close
# deadline, then the SERVER closes the transport (RFC 6455 7.1.1) instead of
# waiting for the client to close TCP; the clean end is marked at that
# server-driven transport closure.
#
# Www.pod L831-834, L857-869 / WS-CLOSE-TRUTH-3: when the application sends
# websocket.close on an accepted socket, the scope enters a PENDING closing
# phase. On the peer's validated Close the server DISPOSES the close deadline
# and closes the transport itself; the completed handshake AND that transport
# closure are the ONLY clean end. A conforming client waits for the server
# (RFC 6455 7.1.1), so waiting for the client deadlocked into a false
# close_timeout -- finding 1, fixed here. The wait is bounded by a finite close
# deadline (ws_close_timeout) reachable ONLY for a genuinely silent peer: its
# expiry with no peer Close is abnormal `close_timeout` / close_code 1006, and a
# transport drop with no peer Close is the transport-loss token / 1006 (NOT
# close_timeout). A peer Close observed before an abnormal outcome keeps its own
# code/reason.
#
# These cases are all APP-INITIATED (the handler sends websocket.close). The
# peer-initiated close (peer sends Close first) also reaches its clean end at
# the server-driven transport closure -- covered by t/82 and t/83.
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
use Socket qw(SOL_SOCKET SO_SNDBUF SO_RCVBUF);

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
        (exists $opt{write_high_watermark} ? (write_high_watermark => $opt{write_high_watermark}) : ()),
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

# Drain any server bytes on $sock and report whether the SERVER has closed its
# end of the transport (a defined sysread of 0 bytes is EOF). Used to prove the
# server -- not the client -- owns the transport close (RFC 6455 7.1.1): the
# client NEVER calls close($sock); it only reads and waits for the server's EOF.
sub server_closed_transport {
    my ($sock) = @_;
    my $buf;
    my $n = sysread($sock, $buf, 4096);
    return (defined $n && $n == 0) ? 1 : 0;
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
#   prime_bytes => int                before the close, send one binary message
#                                     of this size, so the server's write buffer
#                                     is non-empty when the closing handshake
#                                     completes (a peer that stops reading then
#                                     leaves close_when_empty pending forever --
#                                     the transport-finish stall the finish bound
#                                     governs). Www.pod close_incomplete.
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

        if ($o{prime_bytes}) {
            # Fill the server's write queue past the socket buffers with many
            # sub-cap frames (each under Protocol::WebSocket::Frame's payload
            # cap). The test server raises write_high_watermark so these sends
            # are not backpressured; a peer that never reads leaves them
            # undrained, so the later close_when_empty can never finish.
            my $chunk = 'x' x 60000;
            my $n = int($o{prime_bytes} / 60000) + 1;
            for my $i (1 .. $n) {
                await $send->({ type => 'websocket.send', bytes => $chunk });
            }
        }

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

# The Fix-1 (reciprocal-Close-after-peer) app: it does NOT send its own Close
# until AFTER it has received the peer's disconnect, so `close_received` is
# already true when websocket.close is sent -- the 8429 send-handler branch under
# test. Its own Close code (1000/'srvbye') is DISTINCT from the peer's
# (1001/'peerbye'), so any assertion that the peer's code is preserved cannot
# pass on the app's own intent. Options: prime_bytes (an undrainable message, to
# stall the finish); park (never return, so the send path -- not app-return --
# is what governs the close).
sub peer_first_app {
    my (%o) = @_;
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

        if ($o{prime_bytes}) {
            my $chunk = 'x' x 60000;
            my $n = int($o{prime_bytes} / 60000) + 1;
            for my $i (1 .. $n) {
                await $send->({ type => 'websocket.send', bytes => $chunk });
            }
        }

        my $d = await $receive->();                      # the peer's websocket.disconnect
        $obs{recv_type}      = $d->{type};
        $obs{recv_connected} = $conn->is_connected ? 1 : 0;
        # close_received is now true: this reciprocal Close hits the 8429 branch.
        await $send->({ type => 'websocket.close', code => 1000, reason => 'srvbye' });
        $obs{sent_close} = 1;

        if ($o{park}) { $obs{parked} = 1; await $park; }
        $obs{app_returned} = 1;
        return;
    };
    return (\%obs, $park, $app);
}

# =============================================================================
# 0. THE FIX-1 REGRESSION (reciprocal-Close-after-peer). The app receives the
#    peer's Close, THEN sends its own -- so `close_received` is true at the send
#    (the 8429 branch). On base that branch ended the scope EAGERLY
#    (_handle_disconnect_and_close 'client_closed'): terminal marked with the
#    transport still OPEN and NO finish bound. Now it routes through the SAME
#    bounded server-owned closure the parser peer-Close path uses: the scope is
#    clean ONLY at the (bounded) transport closure, never eagerly. Three phases:
#    transport held open -> NOT clean + a finish bound armed; transport closes ->
#    CLEAN with the peer's code; transport stalls past the bound ->
#    close_incomplete with the peer's code. RED on base (eager terminal, no bound).
# =============================================================================
subtest 'Fix 1: reciprocal Close held open -> not clean, finish bound armed' => sub {
    my ($obs, $park, $app) = peer_first_app(park => 1);
    my $server = start_server($app, ws_close_timeout => 5);
    my $sock   = connect_client($server->port);
    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'upgrade');

    my ($conn) = values %{$server->{connections}};
    my $cs     = $conn->{current_connection_state};
    my $stream = $conn->{stream};
    {
        # Hold the server-owned transport close open so the intermediate state is
        # observable: the reciprocal Close must NOT mark the scope clean while the
        # transport is still open, and it MUST arm the finish bound.
        no warnings 'redefine';
        local *IO::Async::Stream::close_when_empty = sub { $obs->{close_requested}++ };
        syswrite($sock, make_websocket_frame(8, pack('n', 1001) . 'peerbye'));
        ok(pump_until(sub { $obs->{sent_close} && $obs->{close_requested} }, 3),
            'app received the peer Close and sent its reciprocal Close');
        is($obs->{recv_type}, 'websocket.disconnect', 'app received the peer Close');
        ok($stream->write_handle, 'the actual transport remains open (close deliberately held)');
        ok(!$cs->response_complete,
            'NOT clean: no eager terminal while the transport is still open');
        ok(!$obs->{complete} && !$obs->{disconnect}, 'no terminal fired yet');
        ok($conn->{ws_close_deadline} && $conn->{ws_close_deadline}->is_running,
            'the finish bound is armed and live (bounded server-owned closure)');
    }
    $stream->close_now;
    $park->done unless $park->is_ready;
    close($sock);
    shutdown_server($server);
};

subtest 'Fix 1: reciprocal Close, transport closes -> clean with the peer code' => sub {
    my ($obs, $park, $app) = peer_first_app(park => 1);
    my $server = start_server($app, ws_close_timeout => 5);
    my $sock   = connect_client($server->port);
    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'upgrade');

    # Peer answers, then only reads and waits for the server's EOF (RFC 6455
    # 7.1.1). The server owns and finishes the transport close -> clean.
    syswrite($sock, make_websocket_frame(8, pack('n', 1001) . 'peerbye'));
    ok(pump_until(sub { $obs->{complete} || $obs->{disconnect} }, 5), 'a terminal fired');
    ok($obs->{complete}, 'on_complete fired -- clean at the server-owned transport closure');
    is($obs->{complete}, 1, 'on_complete fired exactly once');
    is($obs->{complete_reason}, undef, 'disconnect_reason undef -- a clean end');
    is($obs->{complete_connected}, 0, 'is_connected() false at the clean end');
    is($obs->{complete_code}, 1001, 'close_code is the peer code (not the app intent 1000)');
    is($obs->{complete_creason}, 'peerbye', 'close_reason is the peer text');
    ok(!$obs->{disconnect}, 'on_disconnect did NOT fire (no eager client_closed)');
    ok(pump_until(sub { server_closed_transport($sock) }, 3),
        'the SERVER closed the transport -- the client saw EOF without closing TCP');
    $park->done unless $park->is_ready;
    close($sock);
    shutdown_server($server);
};

subtest 'Fix 1: reciprocal Close, transport stalls -> close_incomplete, peer code' => sub {
    my @logs;
    my ($obs, $park, $app) = peer_first_app(park => 1, prime_bytes => 4 * 1024 * 1024);
    my $server = start_server($app,
        ws_close_timeout     => 0.5,
        write_high_watermark => 64 * 1024 * 1024,
        log_level            => 'info',
        logger               => sub { push @logs, $_[0]->{message} // '' });
    my $sock = connect_client($server->port);
    $sock->sockopt(SO_RCVBUF, 4096);   # pin the client's window tiny: nothing drains
    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'upgrade');

    my ($conn) = values %{$server->{connections}};
    ok(pump_until(sub { $conn->_get_write_buffer_size > 0 }, 5),
        'the primed message is stuck in the write queue (the peer is not reading)');
    # Peer answers but never reads: the server-owned close cannot finish draining.
    syswrite($sock, make_websocket_frame(8, pack('n', 1001) . 'peerbye'));
    ok(pump_until(sub { $obs->{sent_close} }, 5), 'app sent its reciprocal Close');
    assert_bounded_close_incomplete($obs, $conn, \@logs, 'Fix1-stall');
    $park->done unless $park->is_ready;
    close($sock);
    shutdown_server($server);
};

# =============================================================================
# 1. handler RETURNS after Close; peer answers then WAITS for the server to
#    close the transport (never closes TCP) -> CLEAN, close_code = peer. THE
#    FINDING-1 REGRESSION: a conforming client waits for the server (RFC 6455
#    7.1.1); on base the server waited for the client, so this deadlocked into a
#    false close_timeout. Now the SERVER closes the transport and the client
#    observes the EOF.
# =============================================================================
subtest 'app close, peer answers, client waits for server EOF -> clean (server-owned close)' => sub {
    my ($app, $obs, $park) = build_app(recv_after => 1);
    # A finite deadline shorter than the pump window: were the server still
    # waiting for the client to close TCP, this would misfire close_timeout.
    my $server = start_server($app, ws_close_timeout => 0.5);
    my $sock   = connect_client($server->port);
    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'upgrade');
    ok(pump_until(sub { $obs->{sent_close} }, 5), 'app sent websocket.close');

    # Peer answers with its Close (code 1000, reason "peerbye"), then leaves TCP
    # open and only reads -- the RFC 6455 7.1.1 conforming client.
    syswrite($sock, make_websocket_frame(8, pack('n', 1000) . 'peerbye'));
    ok(pump_until(sub { $obs->{complete} || $obs->{disconnect} }, 5), 'a terminal fired');

    ok($obs->{complete}, 'on_complete fired -- the server closed the transport and marked clean');
    is($obs->{complete}, 1, 'on_complete fired exactly once');
    is($obs->{complete_connected}, 0, 'is_connected() false at the clean end');
    is($obs->{complete_reason}, undef, 'disconnect_reason() undef -- a clean end');
    is($obs->{complete_code}, 1000, 'close_code is the peer code');
    is($obs->{complete_creason}, 'peerbye', 'close_reason is the peer text');
    ok(!$obs->{disconnect}, 'on_disconnect did NOT fire (no false close_timeout)');
    ok(pump_until(sub { server_closed_transport($sock) }, 3),
        'the SERVER closed the transport -- the client saw EOF without ever closing TCP');
    $park->done unless $park->is_ready;
    close($sock);
    shutdown_server($server);
};

# =============================================================================
# 1b. DISARM ON PEER CLOSE: the instant a validated peer Close arrives the close
#     deadline is disposed, so even a transport completion deferred PAST the
#     (short) deadline can NEVER produce close_timeout. White-box: the deadline
#     timer is dequeued and stopped at the peer Close; pumping well past its
#     expiry yields a clean end, never close_timeout.
# =============================================================================
subtest 'peer Close disposes the deadline -> deferred completion cannot become close_timeout' => sub {
    my ($app, $obs, $park) = build_app(park => 1);
    my $server = start_server($app, ws_close_timeout => 0.3);
    my $sock   = connect_client($server->port);
    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'upgrade');
    ok(pump_until(sub { $obs->{parked} }, 5), 'app sent close and parked (peer silent so far)');

    my ($conn) = values %{$server->{connections}};
    ok($conn, 'the closing-phase connection is registered');
    my $deadline = $conn->{ws_close_deadline};
    ok($deadline && $deadline->is_running, 'the close deadline is armed and live before the peer answers');

    # Peer answers: the deadline must be disposed at once (before any flush).
    syswrite($sock, make_websocket_frame(8, pack('n', 1000) . 'seeya'));
    ok(pump_until(sub { !$conn->{ws_close_deadline} }, 5),
        'the close deadline was disposed the instant the peer Close validated');
    ok(!$deadline->is_running, 'the disposed deadline timer was stopped (no late fire)');

    # Pump WELL past the 0.3s bound: a disposed deadline can never fire, so the
    # only terminal reachable is the clean end from the server-owned close.
    pump_until(sub { $obs->{complete} || $obs->{disconnect} }, 5);
    ok($obs->{complete}, 'reached a clean end after the deadline bound elapsed');
    ok(!$obs->{disconnect}, 'on_disconnect never fired');
    isnt($obs->{disconnect_reason} // '', 'close_timeout',
        'the peer Close made close_timeout unreachable');
    is($obs->{complete_code}, 1000, 'close_code is the peer code');
    $park->done unless $park->is_ready;
    close($sock);
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
# 4. handler PARKS after Close (never returns, never drains); peer answers then
#    waits for server EOF -> CLEAN at the server-driven transport closure. The
#    parked handler is notified without returning and without the client closing
#    TCP.
# =============================================================================
subtest 'app close then PARK, peer answers, server closes transport -> clean' => sub {
    my ($app, $obs, $park) = build_app(park => 1);
    my $server = start_server($app, ws_close_timeout => 0.5);
    my $sock   = connect_client($server->port);
    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'upgrade');
    ok(pump_until(sub { $obs->{parked} }, 5), 'app sent close and parked');

    # Peer answers, then only reads and waits for the server to close.
    syswrite($sock, make_websocket_frame(8, pack('n', 1000) . 'peerbye'));
    ok(pump_until(sub { $obs->{complete} || $obs->{disconnect} }, 5), 'a terminal fired');

    ok($obs->{complete}, 'on_complete fired for the parked handler at the server-driven closure');
    is($obs->{complete_code}, 1000, 'close_code is the peer code');
    is($obs->{complete_reason}, undef, 'disconnect_reason undef -- clean');
    ok(!$obs->{disconnect}, 'on_disconnect did NOT fire');
    ok(!$obs->{app_returned}, 'the handler never returned -- notified without draining receive()');
    ok(pump_until(sub { server_closed_transport($sock) }, 3),
        'the SERVER closed the transport (client saw EOF, never closed TCP)');
    $park->done unless $park->is_ready;
    close($sock);
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
# 7. valid peer Close observed, then a REAL abnormal event (a write_error while
#    the server closes the transport) -> peer code/reason PRESERVED (category 4),
#    never 1006. Re-homed off "close_timeout after a peer Close": under
#    WS-CLOSE-TRUTH-3 a validated peer Close disposes the deadline, so
#    close_timeout-after-reply can no longer happen; the real abnormal that can
#    still co-occur with a recorded peer Close is the server's own close-write
#    failing.
#
#    Genuine fault injection (the pattern t/23-connection-cleanup.t uses): a real
#    server, a real accepted socket, a REAL peer Close recorded by the real
#    parser (ws_peer_closed + the peer's 1000/"seeya"), then the REAL
#    on_write_error handler start() registered is fired with an EPIPE-like errno.
#    _initiate_ws_h1_transport_close is stubbed to a no-op for this case so the
#    server's clean close does not win the race first -- modelling exactly the
#    close whose write fails. Nothing here is a mock asserting itself: the
#    preservation logic (_ws_peer_close_pair -> _set_ws_close) runs for real and
#    is what the assertions read.
# =============================================================================
subtest 'peer Close then a write_error during close -> abnormal but peer code/reason preserved' => sub {
    no warnings 'redefine';
    # Hold the transport open on the peer Close so the injected write_error is
    # the first terminal (models the server's close-write failing).
    local *PAGI::Server::Connection::_initiate_ws_h1_transport_close = sub { };

    my ($app, $obs, $park) = build_app(park => 1);
    my $server = start_server($app, ws_close_timeout => 30);
    my $sock   = connect_client($server->port);
    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'upgrade');
    ok(pump_until(sub { $obs->{parked} }, 5), 'app sent close and parked');

    # Peer sends a valid Close (1000/"seeya"): the real parser records it.
    syswrite($sock, make_websocket_frame(8, pack('n', 1000) . 'seeya'));
    my ($conn) = values %{$server->{connections}};
    ok($conn, 'the connection is registered');
    ok(pump_until(sub { $conn->{ws_peer_closed} }, 5),
        'the real parser recorded the peer Close (ws_peer_closed)');
    ok(!$obs->{complete}, 'not clean -- the server-owned close is held open for this fault');
    ok($conn->{stream}->can_event('on_write_error'),
        'start() registered the real on_write_error handler');

    # Fire the REAL registered write-error handler with an EPIPE-like errno.
    $conn->{stream}->invoke_event('on_write_error', 32);

    ok(pump_until(sub { $obs->{disconnect} }, 5), 'on_disconnect fired on the write_error');
    is($obs->{disconnect_reason}, 'write_error', 'reason is write_error (a REAL abnormal end)');
    is($obs->{disconnect_code}, 1000, 'close_code preserved from the peer Close (not 1006)');
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

# =============================================================================
# 12/13. THE FINISH-BOUND REGRESSIONS (close_incomplete). The closing handshake
#     COMPLETES -- the peer answers (app-initiated) or initiates (peer-initiated)
#     its Close -- and the server owns the transport close (RFC 6455 7.1.1). But
#     the peer then refuses to read, so the server-owned close (close_when_empty)
#     can never finish draining and the scope was left waiting forever (h1
#     INFO-2). Now a finite finish bound (ws_close_timeout) ends the scope
#     abnormally with `close_incomplete`, which PRESERVES the peer's
#     close_code/close_reason (the peer sent a valid Close; category 4,
#     WS-CLOSE-TRUTH-5) -- only disconnect_reason names the outcome. DISTINCT
#     from close_timeout (a silent peer): here the peer DID answer. RED on base
#     (no finish bound -> the scope hangs, no terminal fires).
# =============================================================================

# Build a WebSocket whose closing handshake COMPLETES but whose transport can
# never finish closing: the app primes a large message the peer refuses to read,
# then the handshake completes (app-initiated when send_close, else
# peer-initiated), leaving close_when_empty pending. Sends the peer's Close and
# returns the live pieces so the caller can assert the bound and the terminal.
sub stuck_closing_connection {
    my (%o) = @_;
    my @logs;
    my ($app, $obs, $park) = build_app(
        prime_bytes => (4 * 1024 * 1024),   # exceeds any default socket buffer
        send_close  => $o{send_close},
        park        => 1,
    );
    my $server = start_server($app,
        ws_close_timeout     => $o{ws_close_timeout},
        write_high_watermark => 64 * 1024 * 1024,   # never backpressure the prime
        log_level            => 'info',
        logger               => sub { push @logs, $_[0]->{message} // '' });
    my $sock = connect_client($server->port);
    $sock->sockopt(SO_RCVBUF, 4096);   # pin the client's window tiny: nothing drains
    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'upgrade');

    # The app primed the large message (and, for app-initiated, sent its Close)
    # and is now parked. The prime is stuck in the server's write queue.
    ok(pump_until(sub { $obs->{parked} }, 5), 'app primed the buffer and parked');
    my ($conn) = values %{$server->{connections}};
    ok($conn, 'the connection is registered with the server');
    ok($conn->_get_write_buffer_size > 0,
        'the primed message is stuck in the write queue (the peer is not reading)');

    # The peer answers (app-initiated) or initiates (peer-initiated) its Close,
    # completing the closing handshake -- but it never reads, so the
    # server-owned transport close cannot finish draining. The peer's Close
    # carries a code (1001) DISTINCT from the app's own (1000) and a nonempty
    # reason, so the preserved-peer-code assertions cannot pass on the app's
    # intent; an empty peer Close (1005/undef) is exercised by its own case.
    my $peer_close = exists $o{peer_close_payload}
        ? $o{peer_close_payload}
        : pack('n', 1001) . 'peerbye';
    syswrite($sock, make_websocket_frame(8, $peer_close));
    return ($obs, $conn, \@logs, $sock, $server, $park);
}

# The completed handshake is NOT a clean end (the transport has not finished):
# the scope waits under the finish bound and then resolves close_incomplete,
# which PRESERVES the peer's close_code/close_reason (WS-CLOSE-TRUTH-5): the peer
# sent a valid Close, so its code stands; only disconnect_reason names the
# outcome. Expected peer code/reason default to the distinct 1001/'peerbye'.
sub assert_bounded_close_incomplete {
    my ($obs, $conn, $logs, $label, %exp) = @_;
    $exp{code}   = 1001      unless exists $exp{code};
    $exp{reason} = 'peerbye' unless exists $exp{reason};

    ok(pump_until(sub { $conn->{ws_close_deadline} && $conn->{ws_close_deadline}->is_running }, 3),
        "$label: the finish bound is armed and live after the handshake completed");
    ok(!$obs->{complete} && !$obs->{disconnect},
        "$label: no terminal yet -- the completed handshake alone is not a clean end");

    ok(pump_until(sub { $obs->{disconnect} }, 5),
        "$label: the finish bound resolved the scope");
    is($obs->{disconnect_reason}, 'close_incomplete',
        "$label: reason is close_incomplete (the peer answered; the transport never finished)");
    isnt($obs->{disconnect_reason}, 'close_timeout',
        "$label: specifically NOT close_timeout");
    is($obs->{disconnect_code}, $exp{code},
        "$label: close_code is the PEER's code $exp{code}, preserved (WS-CLOSE-TRUTH-5)");
    is($obs->{disconnect_creason}, $exp{reason},
        "$label: close_reason is the peer's, preserved (WS-CLOSE-TRUTH-5)");
    is($obs->{disconnect}, 1, "$label: on_disconnect fired exactly once");
    is($obs->{end}, 1, "$label: on_end fired exactly once");
    ok($obs->{end_future}, "$label: end_future resolved");
    ok(!$obs->{complete}, "$label: on_complete did NOT fire");

    my @hits = grep { /close_incomplete/ && /finish bound/i } @$logs;
    is(scalar(@hits), 1, "$label: exactly one structured close_incomplete log line");
    unlike($hits[0], qr/close_code 1006/,
        "$label: the log line does not hardcode 1006 (WS-CLOSE-TRUTH-5)");
}

subtest 'app close, peer answers, transport never finishes -> close_incomplete (peer code preserved)' => sub {
    my ($obs, $conn, $logs, $sock, $server, $park) =
        stuck_closing_connection(send_close => 1, ws_close_timeout => 0.5);
    assert_bounded_close_incomplete($obs, $conn, $logs, 'app-initiated');
    $park->done unless $park->is_ready;
    close($sock);
    shutdown_server($server);
};

subtest 'peer close, server reciprocates, transport never finishes -> close_incomplete (peer code preserved)' => sub {
    my ($obs, $conn, $logs, $sock, $server, $park) =
        stuck_closing_connection(send_close => 0, ws_close_timeout => 0.5);
    assert_bounded_close_incomplete($obs, $conn, $logs, 'peer-initiated');
    $park->done unless $park->is_ready;
    close($sock);
    shutdown_server($server);
};

# The empty peer-Close case: the peer's Close carried NO code, so the accessor
# reports 1005/undef (RFC 6455 "no status code"), and close_incomplete preserves
# THAT -- not a forced 1006 (WS-CLOSE-TRUTH-5). Distinguishes "peer code
# preserved" from "no override at all": an empty Close still preserves its own
# (empty) outcome.
subtest 'peer close (no code), transport never finishes -> close_incomplete preserves 1005/undef' => sub {
    my ($obs, $conn, $logs, $sock, $server, $park) =
        stuck_closing_connection(send_close => 0, ws_close_timeout => 0.5,
            peer_close_payload => '');   # empty Close: no status code
    assert_bounded_close_incomplete($obs, $conn, $logs, 'empty-peer-Close',
        code => 1005, reason => undef);
    $park->done unless $park->is_ready;
    close($sock);
    shutdown_server($server);
};

done_testing;
