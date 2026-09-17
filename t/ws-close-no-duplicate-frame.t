#!/usr/bin/env perl

# =============================================================================
# Test: an application's websocket.close never puts a SECOND Close frame on the
# wire when it races the reciprocal Close the server already owes the peer
# (RFC 6455 5.5.1: each endpoint sends exactly one Close frame per direction).
#
# Two h1 orderings race a Close against an already-in-progress closure:
#
#   peer-first  the PEER closes first. The h1 parser sends the server's
#               reciprocal Close (close_sent). The app then receives the
#               disconnect and sends its OWN websocket.close -- the send
#               handler's websocket.close arm. On base that arm writes a Close
#               UNCONDITIONALLY, so a SECOND Close reaches the wire. The guard
#               `unless close_sent` suppresses the duplicate WIRE write; the
#               close-lifecycle branch below it still runs.
#
#   app-first   the APP closes first (send handler writes the Close, close_sent).
#               The peer then closes; the h1 parser's reciprocal arm is already
#               guarded by `if (!close_sent)`, so it emits NO second frame. This
#               cell LOCKS that existing guard -- it passes on base and after.
#
# Each cell drives the application's REAL $send (with its validation), HOLDS the
# server-owned transport close PENDING across the racing send (so the duplicate
# genuinely reaches the wire rather than dying on an already-closed transport),
# counts Close frames (opcode 0x8) on the wire, and asserts exactly ONE; the
# racing websocket.close did NOT fail its Future; a subsequent app send still
# fails; the scope is PENDING while the transport is held, then exactly ONE
# terminal fires when it closes; and the peer's close_code/close_reason are
# preserved on that terminal.
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

# Count server -> client Close frames (opcode 0x8) in a raw byte buffer.
# Server frames are unmasked; a Close carries a payload well under 126 bytes.
sub count_close_frames {
    my ($buf) = @_;
    my $i = 0;
    my $closes = 0;
    my @ops;
    while ($i + 2 <= length $buf) {
        my $b0  = ord substr($buf, $i, 1);
        my $b1  = ord substr($buf, $i + 1, 1);
        my $op  = $b0 & 0x0F;
        my $len = $b1 & 0x7F;
        my $hdr = 2;
        if    ($len == 126) { last if $i + 4 > length $buf; $len = unpack('n', substr($buf, $i + 2, 2)); $hdr = 4; }
        elsif ($len == 127) { last; }   # server never sends a 64-bit control/close frame here
        $hdr += 4 if $b1 & 0x80;          # a (spec-illegal) masked server frame, defensively skipped
        last if $i + $hdr + $len > length $buf;
        push @ops, $op;
        $closes++ if $op == 8;
        $i += $hdr + $len;
    }
    return ($closes, \@ops);
}

sub start_server {
    my ($app) = @_;
    my $server = PAGI::Server->new(
        app              => $app,
        host             => '127.0.0.1',
        port             => 0,
        quiet            => 1,
        shutdown_timeout => 1,
        ws_close_timeout => 30,   # long: only the test drives the close finish
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

sub pump_until {
    my ($cond, $timeout) = @_;
    $timeout //= 5;
    my $deadline = time + $timeout;
    while (time < $deadline) {
        return 1 if $cond->();
        $loop->loop_once(0.02);
    }
    return $cond->() ? 1 : 0;
}

# Returns ($response_headers, $leftover). $leftover is any bytes read past the
# "\r\n\r\n" of the 101 response -- an app that sends its Close immediately after
# accept can have that frame arrive in the same read as the upgrade response, so
# the caller MUST seed its Close-frame accumulator with $leftover or lose it.
sub ws_upgrade {
    my ($sock) = @_;
    syswrite($sock,
          "GET / HTTP/1.1\r\n"
        . "Host: localhost\r\n"
        . "Upgrade: websocket\r\n"
        . "Connection: Upgrade\r\n"
        . "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n"
        . "Sec-WebSocket-Version: 13\r\n"
        . "\r\n");
    my $got = '';
    pump_until(sub {
        my $buf; my $n = sysread($sock, $buf, 4096);
        $got .= $buf if defined $n && $n > 0;
        return $got =~ /\r\n\r\n/;
    }, 5);
    my ($headers, $leftover) = split /\r\n\r\n/, $got, 2;
    return ($headers, $leftover // '');
}

# The racing-close app. $order is 'peer_first' or 'app_first'. The app's own
# Close names 1000/'appbye'; the peer's (sent by the harness) names the DISTINCT
# 1001/'peerbye', so a preserved-peer-code assertion cannot pass on the app's
# own intent. Records the racing close's Future outcome and a subsequent send's.
sub build_race_app {
    my ($obs, $park, $order) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        my $conn = $scope->{'pagi.connection'};

        await $receive->();                              # websocket.connect
        await $send->({ type => 'websocket.accept' });

        $conn->on_complete(sub {
            $obs->{complete}++;
            $obs->{code}   = $conn->close_code;
            $obs->{reason} = $conn->close_reason;
            $obs->{complete_dreason} = $conn->disconnect_reason;
        });
        $conn->on_disconnect(sub {
            $obs->{disconnect}++;
            $obs->{code}   = $conn->close_code;
            $obs->{reason} = $conn->close_reason;
            $obs->{disconnect_reason} = $_[0];
        });
        $conn->on_end(sub { $obs->{end}++ });

        if ($order eq 'peer_first') {
            # The peer closed first: drain its disconnect, THEN send our own
            # Close (close_received is now true -- the send handler's arm under
            # test). The parser already sent the reciprocal.
            my $d = await $receive->();
            $obs->{recv_type} = $d->{type};
        }

        my $close_f = $send->({ type => 'websocket.close', code => 1000, reason => 'appbye' });
        $close_f->on_fail(sub { $obs->{close_failed} = 1 });
        eval { await $close_f };
        $obs->{sent_close} = 1;

        # The scope is now closing: a subsequent application send MUST fail
        # (an app cannot race its own close).
        my $after_f = $send->({ type => 'websocket.send', bytes => 'nope' });
        eval { await $after_f };
        $obs->{after_failed} = $after_f->is_failed ? 1 : 0;
        $obs->{after_done}   = 1;

        await $park;
        return;
    };
}

# =============================================================================
# peer-first: the CONFIRMED duplicate. Parser reciprocal Close + the app's own
# websocket.close. RED on base (two Close frames on the wire).
# =============================================================================
subtest 'peer-first: app websocket.close does not duplicate the reciprocal Close' => sub {
    my %obs;
    my $park   = $loop->new_future;
    my $app    = build_race_app(\%obs, $park, 'peer_first');
    my $server = start_server($app);
    my $sock   = connect_client($server->port);
    my ($hdrs, $leftover) = ws_upgrade($sock);
    like($hdrs, qr/HTTP\/1\.1 101/, "upgrade");
    ok(pump_until(sub { (values %{$server->{connections}})[0] }, 5), "connection registered");
    my ($conn) = values %{$server->{connections}};

    my $rx = $leftover;   # seed with any Close frame bundled with the 101 response
    my $read = sub { my $b; my $n = sysread($sock, $b, 65536); $rx .= $b if defined $n && $n > 0; };

    {
        # Hold the server-owned transport close open across the racing send, so
        # the app's Close genuinely reaches the wire rather than dying on an
        # already-closed transport (the post-closure no-op would falsely pass).
        no warnings 'redefine';
        my $held = 0;
        local *IO::Async::Stream::close_when_empty = sub { $held++ };

        # The peer closes first (1001/'peerbye'): the parser sends the reciprocal
        # and delivers the disconnect, which resumes the app to send its own.
        syswrite($sock, make_websocket_frame(8, pack('n', 1001) . 'peerbye'));
        ok(pump_until(sub { $read->(); $obs{after_done} }, 5),
            'app drained the peer Close, sent its own close, and tried a later send');
        # Pump so the server's queued writes flush to the socket, then read.
        $loop->loop_once(0.02), $read->() for 1 .. 10;

        is($obs{recv_type}, 'websocket.disconnect', 'app received the peer Close');
        ok($held, 'the server-owned transport close was requested but held open');
        ok(!$obs{complete} && !$obs{disconnect},
            'PENDING: no terminal while the transport is held open');

        my ($ncloses, $ops) = count_close_frames($rx);
        is($ncloses, 1, 'exactly ONE Close frame reached the wire (no duplicate)')
            or diag('frame opcodes on wire: ' . join(',', @$ops));

        ok(!$obs{close_failed}, 'the racing websocket.close did NOT fail its Future');
        ok($obs{after_failed},  'a subsequent application send failed (cannot race its own close)');
    }

    # Stub restored: really finish the server-owned close now.
    $conn->{stream}->close_when_empty if $conn->{stream};
    ok(pump_until(sub { $obs{complete} || $obs{disconnect} }, 5), 'a terminal fired');
    is(($obs{complete} // 0) + ($obs{disconnect} // 0), 1, 'exactly ONE terminal notification');
    ok($obs{complete}, 'clean terminal (the closing handshake completed)');
    is($obs{end}, 1, 'on_end fired exactly once');
    is($obs{code},   1001,      'close_code is the PEER code, preserved (not the app 1000)');
    is($obs{reason}, 'peerbye', 'close_reason is the peer text, preserved');

    $park->done unless $park->is_ready;
    close($sock);
    eval { $server->shutdown->get }; eval { $loop->remove($server) };
};

# =============================================================================
# app-first: LOCKS the existing parser guard (`if !close_sent`). The app closes
# first; the peer then closes and the parser must NOT emit a second frame.
# Passes on base and after the fix.
# =============================================================================
subtest 'app-first: peer Close does not duplicate the app close (parser guard held)' => sub {
    my %obs;
    my $park   = $loop->new_future;
    my $app    = build_race_app(\%obs, $park, 'app_first');
    my $server = start_server($app);
    my $sock   = connect_client($server->port);
    my ($hdrs, $leftover) = ws_upgrade($sock);
    like($hdrs, qr/HTTP\/1\.1 101/, "upgrade");
    ok(pump_until(sub { (values %{$server->{connections}})[0] }, 5), "connection registered");
    my ($conn) = values %{$server->{connections}};

    my $rx = $leftover;   # seed with any Close frame bundled with the 101 response
    my $read = sub { my $b; my $n = sysread($sock, $b, 65536); $rx .= $b if defined $n && $n > 0; };

    {
        no warnings 'redefine';
        my $held = 0;
        local *IO::Async::Stream::close_when_empty = sub { $held++ };

        # The app closes first (send handler writes the Close, close_sent) and
        # tries a subsequent send. Then the peer closes -- the parser's
        # reciprocal arm is guarded, so no second frame.
        ok(pump_until(sub { $read->(); $obs{after_done} }, 5),
            'app sent its own close first and tried a later send');
        syswrite($sock, make_websocket_frame(8, pack('n', 1001) . 'peerbye'));
        ok(pump_until(sub { $read->(); $conn->{ws_peer_closed} }, 5),
            'the peer Close was processed by the parser');
        # Pump so the server's queued writes flush to the socket, then read.
        $loop->loop_once(0.02), $read->() for 1 .. 10;

        ok(!$obs{complete} && !$obs{disconnect},
            'PENDING: no terminal while the transport is held open');

        my ($ncloses, $ops) = count_close_frames($rx);
        is($ncloses, 1, 'exactly ONE Close frame reached the wire (no duplicate)')
            or diag('frame opcodes on wire: ' . join(',', @$ops));

        ok(!$obs{close_failed}, 'the app websocket.close did NOT fail its Future');
        ok($obs{after_failed},  'a subsequent application send failed');
    }

    $conn->{stream}->close_when_empty if $conn->{stream};
    ok(pump_until(sub { $obs{complete} || $obs{disconnect} }, 5), 'a terminal fired');
    is(($obs{complete} // 0) + ($obs{disconnect} // 0), 1, 'exactly ONE terminal notification');
    ok($obs{complete}, 'clean terminal (the closing handshake completed)');
    is($obs{end}, 1, 'on_end fired exactly once');
    is($obs{code},   1001,      'close_code is the PEER code, preserved (not the app 1000)');
    is($obs{reason}, 'peerbye', 'close_reason is the peer text, preserved');

    $park->done unless $park->is_ready;
    close($sock);
    eval { $server->shutdown->get }; eval { $loop->remove($server) };
};

done_testing;
