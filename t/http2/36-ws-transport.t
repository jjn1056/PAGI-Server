use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../../lib";
use Socket qw(AF_UNIX SOCK_STREAM);
use Scalar::Util qw(weaken);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.008+ required)');
}

# ============================================================
# Test: pagi.transport on a real WebSocket-over-HTTP/2 stream (packet 10B)
# ============================================================
# WebSocket-over-h2 must provide the same pagi.transport handle as HTTP/2
# streaming, SSE-over-h2, and HTTP/1.1 WebSocket: the app cannot tell which
# transport carries its events. Before this task the h2 WebSocket send path
# wrote frames straight to nghttp2 via its push-style submit_data, which kept
# no per-stream send queue for pagi.transport to measure -- buffered_amount
# always reported 0 and on_high_water/on_drain never fired. Converging h2 WS
# frame emission onto the send-queue/data-provider model its h2 siblings
# already use closes that gap.

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;
use Protocol::WebSocket::Frame;

my $loop = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# NGHTTP2_FLAG_END_STREAM (RFC 9113 section 6.1).
use constant END_STREAM_FLAG => 0x1;

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
    my (%overrides) = @_;
    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $sock_a->blocking(0);
    $sock_b->blocking(0);
    my $app = $overrides{app} // sub { };
    my $server = $overrides{server} // create_test_server(app => $app);
    my $stream = IO::Async::Stream->new(
        read_handle => $sock_a, write_handle => $sock_a, on_read => sub { 0 },
    );
    my $conn = PAGI::Server::Connection->new(
        stream => $stream, app => $app, protocol => $protocol, server => $server,
        h2_protocol => $server->{http2_protocol}, alpn_protocol => 'h2',
        write_high_watermark => $server->{write_high_watermark},
        write_low_watermark  => $server->{write_low_watermark},
    );
    $server->add_child($stream);
    $conn->start;
    return ($conn, $stream, $sock_b, $server);
}

sub create_client {
    my (%overrides) = @_;
    require Net::HTTP2::nghttp2::Session;
    return Net::HTTP2::nghttp2::Session->new_client(
        callbacks => {
            on_begin_headers   => $overrides{on_begin_headers}   // sub { 0 },
            on_header          => $overrides{on_header}          // sub { 0 },
            on_frame_recv      => $overrides{on_frame_recv}      // sub { 0 },
            on_data_chunk_recv => $overrides{on_data_chunk_recv} // sub { 0 },
            on_stream_close    => $overrides{on_stream_close}    // sub { 0 },
        },
    );
}

sub complete_h2_handshake {
    my ($client, $client_sock, %settings) = @_;
    $loop->loop_once(0.1);
    my $server_settings = '';
    $client_sock->sysread($server_settings, 4096);

    $client->send_connection_preface(%settings);
    $client_sock->syswrite($client->mem_send);
    $loop->loop_once(0.1);

    $client->mem_recv($server_settings);
    $loop->loop_once(0.1);
    my $ack = '';
    $client_sock->sysread($ack, 4096);
    $client->mem_recv($ack) if length($ack);

    my $client_ack = $client->mem_send;
    $client_sock->syswrite($client_ack) if length($client_ack);
    $loop->loop_once(0.1);

    my $extra = '';
    $client_sock->sysread($extra, 4096);
    $client->mem_recv($extra) if length($extra);
}

sub send_stream_data {
    my ($client, $client_sock, $stream_id, $data, $end_stream) = @_;
    $end_stream //= 0;
    $client->submit_data($stream_id, $data, $end_stream);
    my $out = $client->mem_send;
    $client_sock->syswrite($out) if length($out);
}

sub exchange_frames {
    my ($client, $client_sock, $rounds) = @_;
    $rounds //= 10;
    for (1..$rounds) {
        $loop->loop_once(0.1);
        my $buf = '';
        $client_sock->sysread($buf, 65536);
        $client->mem_recv($buf) if length($buf);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length($out);
    }
}

sub open_ws_stream_tracked {
    my ($client, $client_sock, $path, $id_ref) = @_;
    $path //= '/ws';
    $$id_ref = $client->submit_request(
        method    => 'CONNECT',
        path      => $path,
        scheme    => 'https',
        authority => 'localhost',
        headers   => [
            [':protocol', 'websocket'],
            ['sec-websocket-version', '13'],
        ],
        body      => sub { return undef },   # streaming: keep open
    );
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock);
    return $$id_ref;
}

sub send_ws_text {
    my ($client, $client_sock, $stream_id, $text) = @_;
    my $frame = Protocol::WebSocket::Frame->new(
        type   => 'text',
        buffer => $text,
        masked => 1,
    );
    send_stream_data($client, $client_sock, $stream_id, $frame->to_bytes);
}

sub extract_ws_frames {
    my ($raw) = @_;
    my @frames;
    my $parser = Protocol::WebSocket::Frame->new;
    $parser->append($raw);
    while (defined(my $bytes = $parser->next_bytes)) {
        push @frames, { opcode => $parser->opcode, bytes => $bytes };
    }
    return @frames;
}

# ============================================================
# buffered_amount grows under withheld WINDOW_UPDATE and drains after release
# ============================================================
subtest 'buffered_amount grows under withheld WINDOW_UPDATE and drains after release' => sub {
    # 4 chunks of 20000 bytes (80000 total): each chunk stays under
    # Protocol::WebSocket::Frame's own 64KB default max_payload_size, but
    # the sum exceeds the client's default 64KB (65535-byte) h2 flow-control
    # window, so nghttp2 can only pull ~64KB per round and the remainder
    # backs up in the server's own per-stream send queue.
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        await $send->({ type => 'websocket.accept' });
        my $event = await $receive->();   # websocket.connect
        while (1) {
            $event = await $receive->();
            if ($event->{type} eq 'websocket.receive'
                    && ($event->{text} // '') eq 'go') {
                for my $_i (1 .. 4) {
                    await $send->({ type => 'websocket.send', text => ('x' x 20_000) });
                }
                next;
            }
            last if $event->{type} eq 'websocket.disconnect';
        }
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $client = create_client;

    complete_h2_handshake($client, $client_sock);
    my $ws_stream_id;
    open_ws_stream_tracked($client, $client_sock, '/ws', \$ws_stream_id);

    my $ss = $conn->{h2_streams}{$ws_stream_id};
    ok($ss, 'server holds stream state for the accepted ws stream');
    ok($ss->{transport_state}, 'pagi.transport handle attached to the ws stream state');

    send_ws_text($client, $client_sock, $ws_stream_id, 'go');

    # Withhold: only a couple of exchange rounds, not enough for the client's
    # own default 64KB window to fully replenish against a 200KB send -- the
    # rest must sit in the server's own per-stream send queue.
    my $buffered_after_send = 0;
    for (1 .. 5) {
        exchange_frames($client, $client_sock, 1);
        $buffered_after_send = $ss->{transport_state}->buffered_amount;
        last if $buffered_after_send > 0;
    }
    ok($buffered_after_send > 0,
        'buffered_amount grew while WINDOW_UPDATE was withheld')
        or diag("buffered_amount: $buffered_after_send");

    # Release: keep pumping (the client's own window updates flow as it reads
    # each round) until the queue is fully drained. Ceiling 100 rounds (10s)
    # is a wide margin over what a 200KB payload needs at 64KB/round.
    my $drained = 0;
    for (1 .. 100) {
        exchange_frames($client, $client_sock, 1);
        if ($ss->{transport_state}->buffered_amount == 0) { $drained = 1; last; }
    }
    ok($drained, 'buffered_amount drained to 0 once WINDOW_UPDATE flowed again')
        or diag('final buffered_amount: ' . $ss->{transport_state}->buffered_amount);

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# High/low water marks report the configured values
# ============================================================
subtest 'high/low water marks report the configured values' => sub {
    my ($seen_high, $seen_low);
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        my $t = $scope->{'pagi.transport'};
        $seen_high = $t->high_water_mark;
        $seen_low  = $t->low_water_mark;
        await $send->({ type => 'websocket.accept' });
        my $event = await $receive->();
        while (1) {
            $event = await $receive->();
            last if $event->{type} eq 'websocket.disconnect';
        }
    };

    my $server = create_test_server(
        app => $app, write_high_watermark => 32768, write_low_watermark => 8192,
    );
    my ($conn, $stream_io, $client_sock) = create_h2_connection(app => $app, server => $server);
    my $client = create_client;

    complete_h2_handshake($client, $client_sock);
    my $ws_stream_id;
    open_ws_stream_tracked($client, $client_sock, '/ws', \$ws_stream_id);

    is($seen_high, 32768, 'high_water_mark reports the configured write_high_watermark');
    is($seen_low, 8192, 'low_water_mark reports the configured write_low_watermark');

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# on_high_water / on_drain hysteresis
# ============================================================
subtest 'on_high_water and on_drain fire on a WebSocket-over-h2 stream' => sub {
    my ($hit_high, $hit_drain) = (0, 0);

    # A single 65536-byte message: the largest single payload
    # Protocol::WebSocket::Frame's own default max_payload_size allows, and
    # (with WS framing overhead) just over the 64KB default high-water mark.
    # Unlike a multi-message burst -- where each individual send() queues,
    # then fully drains via its own synchronous _h2_write_pending flush
    # before the next send() even runs, so the queue never actually
    # accumulates across sends under the 64KB default flow-control window --
    # ONE big push is measured by _check_watermarks before any draining
    # happens, so it reliably crosses the mark in a single synchronous step,
    # exactly like the h2 http-streaming and SSE-over-h2 transport tests
    # (t/http2/19-transport-callbacks.t, t/http2/20-sse-transport.t) do.
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';

        my $t = $scope->{'pagi.transport'};
        $t->on_high_water(sub { $hit_high++ });
        $t->on_drain(sub     { $hit_drain++ });

        await $send->({ type => 'websocket.accept' });
        my $event = await $receive->();   # websocket.connect
        await $send->({ type => 'websocket.send', text => ('x' x 65_536) });
        while (1) {
            $event = await $receive->();
            last if $event->{type} eq 'websocket.disconnect';
        }
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $client = create_client;

    complete_h2_handshake($client, $client_sock);
    my $ws_stream_id;
    open_ws_stream_tracked($client, $client_sock, '/ws', \$ws_stream_id);

    for (1 .. 100) {
        exchange_frames($client, $client_sock, 1);
        last if $hit_high && $hit_drain;
    }

    ok($hit_high,  'on_high_water fired when the per-stream queue exceeded the high mark');
    ok($hit_drain, 'on_drain fired once nghttp2 drained the queue below the low mark');

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# A big send parks at the high watermark and resumes (bounded)
# ============================================================
subtest 'a send that finds the queue already at the high watermark parks, then resumes' => sub {
    my @marks;

    # The client declares a small 4096-byte initial per-stream window, so the
    # FIRST 10000-byte send's own synchronous _h2_write_pending flush can
    # only pull 4096 bytes of it -- unlike a fully-drained push (see the
    # on_high_water/on_drain subtest's comment above), this genuinely leaves
    # a backlog (~5900 bytes) sitting in the queue, over the low 2048-byte
    # high watermark configured below, so the SECOND send's pre-push check
    # finds a real backlog and parks.
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';

        await $send->({ type => 'websocket.accept' });
        my $event = await $receive->();   # websocket.connect
        while (1) {
            $event = await $receive->();
            if ($event->{type} eq 'websocket.receive'
                    && ($event->{text} // '') eq 'go') {
                # First send: queue starts empty, so this returns without
                # parking even though the payload alone is over the high mark.
                await $send->({ type => 'websocket.send', text => ('x' x 10_000) });
                push @marks, 'first-done';
                # Second send: the queue is already at/above the high
                # watermark, so THIS call must park until the peer's pumping
                # drains it back below the low mark.
                await $send->({ type => 'websocket.send', text => ('y' x 10_000) });
                push @marks, 'second-done';
                next;
            }
            last if $event->{type} eq 'websocket.disconnect';
        }
    };

    my $server = create_test_server(
        app => $app, write_high_watermark => 2048, write_low_watermark => 512,
    );
    my ($conn, $stream_io, $client_sock) = create_h2_connection(app => $app, server => $server);
    my $client = create_client;

    complete_h2_handshake($client, $client_sock,
        initial_window_size => 4096, max_concurrent_streams => 100);
    my $ws_stream_id;
    open_ws_stream_tracked($client, $client_sock, '/ws', \$ws_stream_id);

    send_ws_text($client, $client_sock, $ws_stream_id, 'go');

    my $saw_first = 0;
    for (1 .. 10) {
        exchange_frames($client, $client_sock, 1);
        if (grep { $_ eq 'first-done' } @marks) { $saw_first = 1; last; }
    }
    ok($saw_first, 'the first send completed without parking');

    # The second send must NOT have completed yet -- give it a couple more
    # rounds (bounded, not a sleep) while confirming it's still parked.
    exchange_frames($client, $client_sock, 2);
    is(scalar(grep { $_ eq 'second-done' } @marks), 0,
        'the second send is parked while the queue is still at/above the high mark (bounded, not a sleep)');

    # Now let it resume: bounded pump until it completes.
    my $saw_second = 0;
    for (1 .. 100) {
        exchange_frames($client, $client_sock, 1);
        if (grep { $_ eq 'second-done' } @marks) { $saw_second = 1; last; }
    }
    ok($saw_second, 'the second send resumed and completed once the queue drained (bounded)');

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# The close frame + END_STREAM still land on the SAME final DATA frame
# (wire assertion: the queue/data-provider convergence must not split them)
# ============================================================
subtest 'app-initiated close: the close frame carries END_STREAM, same as before the convergence' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        await $send->({ type => 'websocket.accept' });
        my $event = await $receive->();  # connect
        await $send->({ type => 'websocket.close', code => 1000, reason => 'bye' });
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);

    my $ws_data = '';
    my $ws_stream_id;
    my @data_frames;   # {length, flags} for every DATA frame on the ws stream
    my $client = create_client(
        on_data_chunk_recv => sub {
            my ($sid, $data) = @_;
            $ws_data .= $data if defined $ws_stream_id && $sid == $ws_stream_id;
            return 0;
        },
        on_frame_recv => sub {
            my ($frame) = @_;
            if (defined $ws_stream_id && $frame->{stream_id} == $ws_stream_id
                    && $frame->{type} == 0) {   # NGHTTP2_DATA
                push @data_frames, { length => $frame->{length}, flags => $frame->{flags} };
            }
            return 0;
        },
    );

    complete_h2_handshake($client, $client_sock);
    open_ws_stream_tracked($client, $client_sock, '/ws', \$ws_stream_id);

    exchange_frames($client, $client_sock, 20);

    my @frames = extract_ws_frames($ws_data);
    is(scalar @frames, 1, 'exactly one WS frame arrived: the close frame')
        or diag('frames seen: ' . scalar(@frames));
    if (@frames) {
        is($frames[0]{opcode}, 8, 'the frame is a Close frame (opcode 8)');
        is(unpack('n', substr($frames[0]{bytes}, 0, 2)), 1000, 'close code is 1000');
    }

    # Wire assertion: the close bytes and END_STREAM must land on the SAME
    # h2 DATA frame -- not the close bytes in one frame followed by a
    # separate empty END_STREAM frame. Exactly one DATA frame carrying this
    # stream's payload, and it is END_STREAM-flagged.
    my @nonempty = grep { $_->{length} > 0 } @data_frames;
    is(scalar @nonempty, 1, 'exactly one non-empty DATA frame arrived on the ws stream')
        or diag('DATA frames seen: ' . join(', ', map { "len=$_->{length} flags=$_->{flags}" } @data_frames));
    if (@nonempty) {
        ok(($nonempty[0]{flags} & END_STREAM_FLAG), 'that DATA frame carries END_STREAM')
            or diag("flags: $nonempty[0]{flags}");
    }

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# A parked websocket.send() that wakes AFTER the close frame is already
# queued must not push data behind it (RFC 6455 5.5.1: no frame may follow
# Close, and Close itself must carry END_STREAM alone).
# ============================================================
# White-box on the wake trigger by necessity: the real trigger is a window
# grant landing the per-stream queue below the low watermark, but that also
# tends to deliver the close frame's own EOF pull in the SAME data_callback
# invocation that releases the drain waiter -- racing the wake against
# stream teardown in a way that isn't reliably provokable through real
# pumping. _h2_resolve_stream_drain_waiters is the exact function the real
# data_callback calls to release a parked send; invoking it directly still
# exercises the real send()/_h2_ws_close code paths and the real guard,
# just without gambling on which deferred callback the event loop runs
# first.
subtest 'a send() parked when close is queued does not push behind the close frame' => sub {
    my @marks;
    my $f2;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';

        await $send->({ type => 'websocket.accept' });
        my $event = await $receive->();   # websocket.connect
        while (1) {
            $event = await $receive->();
            if ($event->{type} eq 'websocket.receive'
                    && ($event->{text} // '') eq 'go') {
                # First send: queue starts empty, doesn't park.
                await $send->({ type => 'websocket.send', text => ('x' x 10_000) });
                push @marks, 'first-done';

                # Second send: queue already at/above the high mark -> parks.
                # Fire-and-forget (not awaited) so the app can go on to close
                # while it is still parked.
                $f2 = $send->({ type => 'websocket.send', text => ('y' x 10_000) });
                $f2->on_ready(sub { push @marks, 'second-settled'; });

                # Close while send#2 is still parked.
                await $send->({ type => 'websocket.close', code => 1000, reason => 'bye' });
                push @marks, 'closed';
                next;
            }
            last if $event->{type} eq 'websocket.disconnect';
        }
    };

    my $server = create_test_server(
        app => $app, write_high_watermark => 2048, write_low_watermark => 512,
    );
    my ($conn, $stream_io, $client_sock) = create_h2_connection(app => $app, server => $server);
    my $ws_data = '';
    my $client = create_client(
        on_data_chunk_recv => sub { my ($sid, $data) = @_; $ws_data .= $data; return 0; },
    );

    complete_h2_handshake($client, $client_sock,
        initial_window_size => 4096, max_concurrent_streams => 100);
    my $ws_stream_id;
    open_ws_stream_tracked($client, $client_sock, '/ws', \$ws_stream_id);

    send_ws_text($client, $client_sock, $ws_stream_id, 'go');

    my $saw_closed = 0;
    for (1 .. 10) {
        exchange_frames($client, $client_sock, 1);
        if (grep { $_ eq 'closed' } @marks) { $saw_closed = 1; last; }
    }
    ok($saw_closed, 'app reached websocket.close while the second send was still parked');

    my $ss = $conn->{h2_streams}{$ws_stream_id};
    ok($ss, 'server still holds stream state right after close');
    ok($ss && $ss->{ws_eof_pending}, 'ws_eof_pending is set (close frame queued)');
    ok($ss && $ss->{stream_drain_waiters} && @{$ss->{stream_drain_waiters}},
        'the second send is genuinely parked (a drain waiter is registered)');

    my $queue_bytes_before_wake = $ss ? ($ss->{send_queue_bytes} // 0) : undef;

    # Simulate the window opening enough to drain below the low watermark --
    # the exact release the real data_callback performs.
    $conn->_h2_resolve_stream_drain_waiters($ss) if $ss;

    my $saw_settled = 0;
    for (1 .. 20) {
        $loop->loop_once(0.05);
        if (grep { $_ eq 'second-settled' } @marks) { $saw_settled = 1; last; }
    }
    ok($saw_settled, 'the parked second send eventually settles (bounded)');

    # The fix: waking must NOT push 'y' x 10_000 behind the already-queued
    # close frame. $ss may since have been reclaimed by teardown (also a
    # correct outcome -- either way, no bytes were added by the wake).
    my $ss_after = $conn->{h2_streams}{$ws_stream_id};
    if ($ss_after && defined $queue_bytes_before_wake) {
        ok(($ss_after->{send_queue_bytes} // 0) <= $queue_bytes_before_wake,
            'the parked send did not push new bytes onto the queue after close was queued')
            or diag("before=$queue_bytes_before_wake after=$ss_after->{send_queue_bytes}");
    }

    # Wire assertion: drain everything and confirm no Text/Binary frame
    # (opcode 1/2, i.e. the 'y' x 10_000 payload) ever reached the wire --
    # only the Close frame (and whatever of send#1's 'x' payload made it out
    # under the small window) may appear.
    for (1 .. 20) {
        exchange_frames($client, $client_sock, 1);
    }
    my @frames = extract_ws_frames($ws_data);
    # send#1's 'x' x 10_000 payload legitimately reached the wire before the
    # close (it completed and marked 'first-done' before send#2 ever
    # parked) -- only a 'y'-payload frame (send#2's, which must never have
    # been pushed) would indicate the bug.
    my @leaked = grep { ($_->{opcode} == 1 || $_->{opcode} == 2) && $_->{bytes} =~ /^y/ } @frames;
    is(scalar @leaked, 0,
        "no Text/Binary frame carrying send#2's payload reached the wire")
        or diag('leaked frames: ' . join(', ', map { "opcode=$_->{opcode} len=" . length($_->{bytes}) } @leaked));

    my @closes = grep { $_->{opcode} == 8 } @frames;
    is(scalar @closes, 1, 'exactly one Close frame reached the wire');

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# A parked websocket.send() releases (bounded) on connection teardown
# (M1: pin one of the drain-waiter release sweeps -- _h2_ws_enqueue_disconnect,
# reached here via the peer RST_STREAM path -- so a parked producer never
# hangs forever on a stream that is going away).
# ============================================================
subtest 'a parked websocket.send() releases (bounded) when the client RSTs the stream' => sub {
    my @marks;
    my $f2;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';

        await $send->({ type => 'websocket.accept' });
        my $event = await $receive->();   # websocket.connect
        while (1) {
            $event = await $receive->();
            if ($event->{type} eq 'websocket.receive'
                    && ($event->{text} // '') eq 'go') {
                # First send: queue starts empty, doesn't park.
                await $send->({ type => 'websocket.send', text => ('x' x 10_000) });
                push @marks, 'first-done';

                # Second send: queue already at/above the high mark -> parks.
                # Fire-and-forget so the test can RST the stream while it is
                # still waiting.
                $f2 = $send->({ type => 'websocket.send', text => ('y' x 10_000) });
                $f2->on_ready(sub { push @marks, 'released'; });
                next;
            }
            last if $event->{type} eq 'websocket.disconnect';
        }
    };

    my $server = create_test_server(
        app => $app, write_high_watermark => 2048, write_low_watermark => 512,
    );
    my ($conn, $stream_io, $client_sock) = create_h2_connection(app => $app, server => $server);
    my $client = create_client;

    complete_h2_handshake($client, $client_sock,
        initial_window_size => 4096, max_concurrent_streams => 100);
    my $ws_stream_id;
    open_ws_stream_tracked($client, $client_sock, '/ws', \$ws_stream_id);

    send_ws_text($client, $client_sock, $ws_stream_id, 'go');

    my $saw_first = 0;
    for (1 .. 10) {
        exchange_frames($client, $client_sock, 1);
        if (grep { $_ eq 'first-done' } @marks) { $saw_first = 1; last; }
    }
    ok($saw_first, 'the first send completed');

    my $ss = $conn->{h2_streams}{$ws_stream_id};
    ok($ss && $ss->{stream_drain_waiters} && @{$ss->{stream_drain_waiters}},
        'the second send is genuinely parked (a drain waiter is registered)');

    # Client resets the stream (RFC 9113 CANCEL, error code 8) instead of
    # closing cleanly -- the abrupt teardown path.
    $client->submit_rst_stream($ws_stream_id, 8);
    $client_sock->syswrite($client->mem_send);

    my $saw_released = 0;
    for (1 .. 30) {
        exchange_frames($client, $client_sock, 1);
        if (grep { $_ eq 'released' } @marks) { $saw_released = 1; last; }
    }
    ok($saw_released,
        'the parked send released (bounded), instead of hanging forever, once the stream was torn down');

    $stream_io->close_now;
    $loop->remove($server);
};

done_testing;
