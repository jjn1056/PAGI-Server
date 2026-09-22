#!/usr/bin/env perl

# =============================================================================
# Test: an application's websocket.close never puts a SECOND Close frame on an
# HTTP/2 WebSocket stream when it races the reciprocal Close the server owes the
# peer (RFC 6455 5.5.1: exactly one Close frame per direction). The h2 twin of
# t/ws-close-no-duplicate-frame.t.
#
# Two orderings race a Close against an already-in-progress closure:
#
#   app-first   the APP closes first (_h2_ws_close enqueues its Close, sets
#               ws_eof_pending). The peer then closes; the peer-inbound arm
#               enqueues the server's reciprocal Close. On base that enqueue is
#               UNCONDITIONAL, so a SECOND Close is queued -- and when the
#               stream's output was still PENDING (the app's Close not yet
#               flushed with END_STREAM, e.g. under flow-control backpressure),
#               BOTH Close frames flush and reach the wire. The guard
#               `unless ws_eof_pending` on the peer-inbound enqueue suppresses
#               the duplicate; the lifecycle below it runs unchanged.
#
#   peer-first  the PEER closes first (the peer-inbound arm enqueues the
#               reciprocal, ws_eof_pending). The app then closes; _h2_ws_close
#               is ALREADY guarded by `unless ws_eof_pending`, so it enqueues no
#               second frame. This cell LOCKS that existing guard -- it passes on
#               base and after.
#
# Genuine PENDING output is the crux of the app-first cell (the GATE): the app's
# Close is held UNFLUSHED (SETTINGS_INITIAL_WINDOW_SIZE 0 blocks the server's
# DATA) so END_STREAM has NOT been emitted when the peer's Close is processed --
# both frames then sit in one send queue and flush together (the app's Close
# with flags=0, the reciprocal with END_STREAM). Were END_STREAM already out,
# the second frame would die unsent and there would be no defect. Each cell
# drives the application's REAL $send, counts Close frames (opcode 0x8) on the
# stream, asserts exactly ONE; the racing websocket.close did NOT fail its
# Future; a subsequent app send still fails; the scope is PENDING before the
# stream completes, then exactly ONE terminal fires; the peer's
# close_code/close_reason are preserved on that terminal.
# =============================================================================

use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use Future;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../../lib";
use Socket qw(AF_UNIX SOCK_STREAM);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.011+ required)');
}

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;
use Protocol::WebSocket::Frame;

my $loop     = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# Per-run capture of server -> client DATA payloads (the raw WebSocket frames)
# and of each received frame's END_STREAM flag, keyed by stream id.
my %DATA;
my @FRAMES;

sub create_test_server {
    my (%args) = @_;
    my $server = PAGI::Server->new(
        app => $args{app} // sub { }, host => '127.0.0.1', port => 0,
        quiet => 1, http2 => 1, %args,
    );
    $loop->add($server);
    return $server;
}

sub create_h2_connection {
    my (%o) = @_;
    socketpair(my $sa, my $sb, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $sa->blocking(0);
    $sb->blocking(0);
    my $app    = $o{app} // sub { };
    my $server = $o{server} // create_test_server(app => $app);
    my $stream = IO::Async::Stream->new(read_handle => $sa, write_handle => $sa, on_read => sub { 0 });
    my $conn   = PAGI::Server::Connection->new(
        stream => $stream, app => $app, protocol => $protocol, server => $server,
        h2_protocol => $server->{http2_protocol}, alpn_protocol => 'h2',
        ws_close_timeout => $o{ws_close_timeout} // 30,
    );
    $server->add_child($stream);
    $conn->start;
    return ($conn, $stream, $sb, $server);
}

sub create_client {
    require Net::HTTP2::nghttp2::Session;
    return Net::HTTP2::nghttp2::Session->new_client(
        callbacks => {
            on_begin_headers   => sub { 0 },
            on_header          => sub { 0 },
            on_frame_recv      => sub { my $f = $_[0]; push @FRAMES, $f; 0 },
            on_data_chunk_recv => sub {
                my @a = @_; shift @a if ref $a[0];   # tolerate an optional leading session arg
                my ($sid, $data) = @a;
                $DATA{$sid} .= $data if defined $data;
                0;
            },
            on_stream_close    => sub { 0 },
        },
    );
}

# %opt passes SETTINGS (e.g. initial_window_size => 0) to the client preface.
sub complete_h2_handshake {
    my ($client, $cs, %opt) = @_;
    $loop->loop_once(0.1);
    my $ss = ''; $cs->sysread($ss, 4096);
    $client->send_connection_preface(enable_connect_protocol => 1, %opt);
    $cs->syswrite($client->mem_send);
    $loop->loop_once(0.1);
    $client->mem_recv($ss);
    $loop->loop_once(0.1);
    my $ack = ''; $cs->sysread($ack, 4096); $client->mem_recv($ack) if length $ack;
    my $ca = $client->mem_send; $cs->syswrite($ca) if length $ca;
    $loop->loop_once(0.1);
    my $ex = ''; $cs->sysread($ex, 4096); $client->mem_recv($ex) if length $ex;
}

sub send_stream_data {
    my ($client, $cs, $sid, $data, $eof) = @_;
    $eof //= 0;
    $client->submit_data($sid, $data, $eof);
    my $out = $client->mem_send; $cs->syswrite($out) if length $out;
}

sub exchange {
    my ($client, $cs, $rounds) = @_;
    $rounds //= 8;
    for (1 .. $rounds) {
        $loop->loop_once(0.05);
        my $b = ''; $cs->sysread($b, 16384); $client->mem_recv($b) if length $b;
        my $o = $client->mem_send; $cs->syswrite($o) if length $o;
    }
}

sub pump_until {
    my ($client, $cs, $cond, $max) = @_;
    $max //= 80;
    for (1 .. $max) {
        return 1 if $cond->();
        $loop->loop_once(0.05);
        my $b = ''; $cs->sysread($b, 16384); $client->mem_recv($b) if length $b;
        my $o = $client->mem_send; $cs->syswrite($o) if length $o;
    }
    return $cond->() ? 1 : 0;
}

sub open_ws_stream {
    my ($client, $cs, $path) = @_;
    $path //= '/ws';
    my $sid = $client->submit_request(
        method => 'CONNECT', path => $path, scheme => 'https', authority => 'localhost',
        headers => [[':protocol', 'websocket'], ['sec-websocket-version', '13']],
        body => sub { return undef },   # streaming: keep the stream open
    );
    $cs->syswrite($client->mem_send);
    exchange($client, $cs, 4);
    return $sid;
}

sub client_close_frame {
    my ($code, $reason) = @_;
    $reason //= '';
    return Protocol::WebSocket::Frame->new(
        type => 'close', buffer => pack('n', $code) . $reason, masked => 1,
    )->to_bytes;
}

# Count server -> client Close frames (opcode 0x8) in a raw WebSocket byte
# buffer (the concatenated DATA payloads). Server frames are unmasked.
sub count_close_frames {
    my ($buf) = @_;
    $buf //= '';
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
        elsif ($len == 127) { last; }
        $hdr += 4 if $b1 & 0x80;
        last if $i + $hdr + $len > length $buf;
        push @ops, $op;
        $closes++ if $op == 8;
        $i += $hdr + $len;
    }
    return ($closes, \@ops);
}

# The racing-close app. The app's own Close names 1000/'appbye'; the peer's
# (sent by the harness) names the DISTINCT 1001/'peerbye'. For 'peer_first' the
# app drains the peer's disconnect BEFORE sending its own close.
sub build_race_app {
    my ($obs, $park, $order) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        my $c = $scope->{'pagi.connection'};

        await $receive->();                          # websocket.connect
        await $send->({ type => 'websocket.accept' });

        $c->on_complete(sub {
            $obs->{complete}++;
            $obs->{code}   = $c->close_code;
            $obs->{reason} = $c->close_reason;
            $obs->{complete_dreason} = $c->disconnect_reason;
        });
        $c->on_disconnect(sub {
            $obs->{disconnect}++;
            $obs->{code}   = $c->close_code;
            $obs->{reason} = $c->close_reason;
            $obs->{disconnect_reason} = $_[0];
        });
        $c->on_end(sub { $obs->{end}++ });

        if ($order eq 'peer_first') {
            my $d = await $receive->();              # the peer's websocket.disconnect
            $obs->{recv_type} = $d->{type};
        }

        my $close_f = $send->({ type => 'websocket.close', code => 1000, reason => 'appbye' });
        $close_f->on_fail(sub { $obs->{close_failed} = 1 });
        eval { await $close_f };
        $obs->{closed_sent} = 1;

        # The scope is now closing: a subsequent application send MUST fail.
        my $after_f = $send->({ type => 'websocket.send', bytes => 'nope' });
        eval { await $after_f };
        $obs->{after_failed} = $after_f->is_failed ? 1 : 0;
        $obs->{after_done}   = 1;

        await $park;
        return;
    };
}

# =============================================================================
# app-first: the app closes first; the peer then closes while the app's Close is
# still PENDING (window 0), so the peer-inbound reciprocal is enqueued behind it.
# On base BOTH Close frames flush -> RED (two on the wire).
# =============================================================================
subtest 'app-first: peer Close does not duplicate the app close (pending output)' => sub {
    %DATA = (); @FRAMES = ();
    my %obs;
    my $park = $loop->new_future;
    my $app  = build_race_app(\%obs, $park, 'app_first');
    my ($conn, $stream_io, $cs, $server) = create_h2_connection(app => $app);
    my $client = create_client();

    # Window 0: the server cannot flush the app's Close DATA, so END_STREAM is
    # held and the output stays PENDING across the racing peer Close.
    complete_h2_handshake($client, $cs, initial_window_size => 0);
    my $sid = open_ws_stream($client, $cs);
    ok(pump_until($client, $cs, sub { $obs{after_done} }), 'app sent its close (held pending) and tried a later send');

    my $ss = $conn->{h2_streams}{$sid};
    is(scalar @{ $ss->{send_queue} || [] }, 1, 'the app Close is queued but UNFLUSHED (output pending)');
    ok($ss->{ws_eof_pending}, 'ws_eof_pending is set (END_STREAM owed, not yet emitted)');

    # The peer closes WITHOUT END_STREAM, so the stream stays open and its send
    # queue is drained (not abandoned) when the window later opens.
    send_stream_data($client, $cs, $sid, client_close_frame(1001, 'peerbye'), 0);
    ok(pump_until($client, $cs, sub { $ss->{ws_peer_closed} }), 'the peer Close was processed');
    ok(!$obs{complete} && !$obs{disconnect}, 'PENDING: no terminal yet (stream not complete)');

    # Open the per-stream window: the server flushes its queued output. On base
    # that is TWO Close frames (the app Close with flags=0, then the reciprocal
    # with END_STREAM); with the guard it is ONE.
    $client->submit_window_update($sid, 1 << 20);
    $cs->syswrite($client->mem_send);
    exchange($client, $cs, 8);

    my ($ncloses, $ops) = count_close_frames($DATA{$sid});
    is($ncloses, 1, 'exactly ONE Close frame reached the wire (no duplicate)')
        or diag('WebSocket opcodes on the stream: ' . join(',', @$ops));

    ok(!$obs{close_failed}, 'the racing websocket.close did NOT fail its Future');
    ok($obs{after_failed},  'a subsequent application send failed (cannot race its own close)');

    # Complete the stream: the peer sends END_STREAM.
    send_stream_data($client, $cs, $sid, '', 1);
    ok(pump_until($client, $cs, sub { $obs{complete} || $obs{disconnect} }), 'a terminal fired');
    is(($obs{complete} // 0) + ($obs{disconnect} // 0), 1, 'exactly ONE terminal notification');
    ok($obs{complete}, 'clean terminal (the closing handshake completed)');
    is($obs{end}, 1, 'on_end fired exactly once');
    is($obs{code},   1001,      'close_code is the PEER code, preserved (not the app 1000)');
    is($obs{reason}, 'peerbye', 'close_reason is the peer text, preserved');

    $park->done unless $park->is_ready;
    eval { $stream_io->close_now };
    $loop->remove($server);
};

# =============================================================================
# peer-first: the peer closes first; the peer-inbound arm enqueues the
# reciprocal (ws_eof_pending). The app then closes and _h2_ws_close's OWN guard
# suppresses a second frame. LOCKS that existing guard (passes on base + after).
# =============================================================================
subtest 'peer-first: app close does not duplicate the reciprocal (h2_ws_close guard held)' => sub {
    %DATA = (); @FRAMES = ();
    my %obs;
    my $park = $loop->new_future;
    my $app  = build_race_app(\%obs, $park, 'peer_first');
    my ($conn, $stream_io, $cs, $server) = create_h2_connection(app => $app);
    my $client = create_client();

    complete_h2_handshake($client, $cs, initial_window_size => 0);
    my $sid = open_ws_stream($client, $cs);

    # The peer closes first (no END_STREAM): the peer-inbound arm enqueues the
    # server's reciprocal Close and delivers the disconnect that resumes the app.
    send_stream_data($client, $cs, $sid, client_close_frame(1001, 'peerbye'), 0);
    ok(pump_until($client, $cs, sub { $obs{after_done} }),
        'peer closed first; app drained the disconnect, sent its own close, tried a later send');

    my $ss = $conn->{h2_streams}{$sid};
    is($obs{recv_type}, 'websocket.disconnect', 'app received the peer Close');
    is(scalar @{ $ss->{send_queue} || [] }, 1,
        'only the reciprocal is queued -- the app close added no second frame');
    ok(!$obs{complete} && !$obs{disconnect}, 'PENDING: no terminal yet (stream not complete)');

    $client->submit_window_update($sid, 1 << 20);
    $cs->syswrite($client->mem_send);
    exchange($client, $cs, 8);

    my ($ncloses, $ops) = count_close_frames($DATA{$sid});
    is($ncloses, 1, 'exactly ONE Close frame reached the wire (no duplicate)')
        or diag('WebSocket opcodes on the stream: ' . join(',', @$ops));

    ok(!$obs{close_failed}, 'the app websocket.close did NOT fail its Future');
    ok($obs{after_failed},  'a subsequent application send failed');

    send_stream_data($client, $cs, $sid, '', 1);
    ok(pump_until($client, $cs, sub { $obs{complete} || $obs{disconnect} }), 'a terminal fired');
    is(($obs{complete} // 0) + ($obs{disconnect} // 0), 1, 'exactly ONE terminal notification');
    ok($obs{complete}, 'clean terminal (the closing handshake completed)');
    is($obs{end}, 1, 'on_end fired exactly once');
    is($obs{code},   1001,      'close_code is the PEER code, preserved');
    is($obs{reason}, 'peerbye', 'close_reason is the peer text, preserved');

    $park->done unless $park->is_ready;
    eval { $stream_io->close_now };
    $loop->remove($server);
};

done_testing;
