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
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.008+ required)');
}

# ============================================================
# Test: h2 WebSocket inbound queue-overflow enforcement (audit-found gap)
# ============================================================
# h1's _process_websocket_frames checks max_receive_queue before queueing
# each text/binary websocket.receive event -- once the queue is already at
# the configured cap, the next inbound message closes the connection with
# 1008 ("Message queue overflow") and delivers a queue_overflow
# websocket.disconnect instead of queueing without bound. h2's
# _h2_process_ws_frames had no such check: a client flooding messages
# faster than the app drains receive() could grow a stream's receive_queue
# without limit -- an unbounded per-connection memory DoS on a
# transport that otherwise mirrors HTTP/1.1's WebSocket support exactly.
#
# This uses an app that never calls receive() at all (accepts and returns
# immediately) as its "never drains" case -- the stream stays open at the
# h2 level (nothing about returning from a WebSocket app coroutine tears
# the stream down; only an explicit close/END_STREAM does), so every
# flooded message is available to inspect via white-box access to the
# stream's own receive_queue.

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;
use Protocol::WebSocket::Frame;

my $loop = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# Small cap so a handful of flooded messages clearly demonstrates the
# enforcement (or its absence) without needing a large flood.
use constant MAX_RECEIVE_QUEUE => 3;
use constant FLOOD_COUNT       => 10;   # well past the cap

# ============================================================
# Helpers (lifted from t/http2/31-ws-keepalive-disconnect.t /
# t/http2/36-ws-transport.t / t/http2/37-ws-frame-validation.t)
# ============================================================

sub create_test_server {
    my (%args) = @_;
    my $server = PAGI::Server->new(
        app               => $args{app} // sub { },
        host              => '127.0.0.1',
        port              => 0,
        quiet             => 1,
        http2             => 1,
        max_receive_queue => MAX_RECEIVE_QUEUE,
        %args,
    );
    $loop->add($server);
    return $server;
}

sub create_h2_connection {
    my (%overrides) = @_;

    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0)
        or die "socketpair: $!";
    $sock_a->blocking(0);
    $sock_b->blocking(0);

    my $app = $overrides{app} // sub { };
    my $server = $overrides{server} // create_test_server(app => $app);

    my $stream = IO::Async::Stream->new(
        read_handle  => $sock_a,
        write_handle => $sock_a,
        on_read => sub { 0 },
    );

    my $conn = PAGI::Server::Connection->new(
        stream        => $stream,
        app           => $app,
        protocol      => $protocol,
        server        => $server,
        h2_protocol   => $server->{http2_protocol},
        alpn_protocol => 'h2',
        max_body_size => $server->{max_body_size},
        max_receive_queue => $server->{max_receive_queue},
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
    my ($client, $client_sock) = @_;

    $loop->loop_once(0.1);
    my $server_settings = '';
    $client_sock->sysread($server_settings, 4096);

    $client->send_connection_preface;
    my $data = $client->mem_send;
    $client_sock->syswrite($data);
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

sub send_ws_text {
    my ($client, $client_sock, $stream_id, $text) = @_;
    my $frame = Protocol::WebSocket::Frame->new(
        type   => 'text',
        buffer => $text,
        masked => 1,
    );
    send_stream_data($client, $client_sock, $stream_id, $frame->to_bytes);
}

sub exchange_frames {
    my ($client, $client_sock, $rounds) = @_;
    $rounds //= 10;
    for (1..$rounds) {
        $loop->loop_once(0.1);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
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

sub close_codes {
    my ($raw) = @_;
    return map  { unpack('n', substr($_->{bytes}, 0, 2)) }
           grep { $_->{opcode} == 8 && length($_->{bytes}) >= 2 }
           extract_ws_frames($raw);
}

# App that accepts and immediately returns -- it never calls receive() at
# all, so nothing ever drains the stream's receive_queue. Nothing about a
# WebSocket app coroutine returning tears the h2 stream down by itself
# (only an explicit close/END_STREAM does), so the stream stays open and
# every flooded message lands in receive_queue for white-box inspection.
sub make_never_draining_app {
    return async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        await $send->({ type => 'websocket.accept' });
        return;
    };
}

subtest 'flooding past max_receive_queue is bounded, closes 1008/queue_overflow' => sub {
    my $app = make_never_draining_app();
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);

    my $ws_stream_id;
    my $ws_data = '';
    my $client = create_client(
        on_data_chunk_recv => sub {
            my ($sid, $data) = @_;
            $ws_data .= $data if defined $ws_stream_id && $sid == $ws_stream_id;
            return 0;
        },
    );

    complete_h2_handshake($client, $client_sock);
    open_ws_stream_tracked($client, $client_sock, '/ws', \$ws_stream_id);

    my $ss = $conn->{h2_streams}{$ws_stream_id};
    ok($ss, 'server holds stream state for the accepted ws stream');

    # Flood well past the configured cap. Nobody is draining (the app never
    # calls receive()), so every message that is allowed to queue stays
    # queued for inspection below.
    for my $i (1 .. FLOOD_COUNT) {
        send_ws_text($client, $client_sock, $ws_stream_id, "msg$i");
    }

    # Bounded poll: give the server enough rounds to process the flood and
    # (post-fix) flush its Close frame to the wire. 30 rounds (3s) is a wide
    # margin over the single in-process flood this subtest drives.
    my $queue_len = 0;
    for (1 .. 30) {
        exchange_frames($client, $client_sock, 1);
        $queue_len = scalar @{ $ss->{receive_queue} // [] };
        last unless exists $conn->{h2_streams}{$ws_stream_id};
    }
    # A couple more rounds so a just-queued Close frame reaches the wire.
    exchange_frames($client, $client_sock, 5);

    # The definitive "no leak" check: the queue must never hold more than
    # max_receive_queue websocket.receive entries plus the one disconnect
    # event queue-overflow enforcement appends when it closes the
    # connection -- never FLOOD_COUNT (10) entries, which is what an
    # unenforced queue would accumulate.
    ok($queue_len <= MAX_RECEIVE_QUEUE + 1,
        "receive_queue stayed bounded (<= @{[ MAX_RECEIVE_QUEUE + 1 ]}), got $queue_len")
        or diag("FLOOD_COUNT was @{[ FLOOD_COUNT ]}; an unbounded queue would show that many");

    my @receive_events = grep { $_->{type} eq 'websocket.receive' } @{ $ss->{receive_queue} // [] };
    ok(scalar(@receive_events) <= MAX_RECEIVE_QUEUE,
        'no more than max_receive_queue websocket.receive events were ever queued');

    my @disconnects = grep { $_->{type} eq 'websocket.disconnect' } @{ $ss->{receive_queue} // [] };
    is(scalar(@disconnects), 1, 'exactly one websocket.disconnect queued for the overflow');
    if (@disconnects) {
        is($disconnects[0]{code}, 1008, 'code is 1008 (policy violation / queue overflow)');
        is($disconnects[0]{reason}, 'queue_overflow', "reason is 'queue_overflow'");
    }

    my @codes = close_codes($ws_data);
    is(scalar(@codes), 1, 'server sent exactly one Close frame on the wire');
    is($codes[0], 1008, 'wire Close frame carries code 1008') if @codes;

    $stream_io->close_now;
    $loop->remove($server);
};

done_testing;
