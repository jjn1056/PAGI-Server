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
# Test: exactly-one h2 WebSocket disconnect (design section 10.3)
# ============================================================
# Before this task, a single h2 WebSocket close could enqueue up to
# three websocket.disconnect events for one scope: the peer close-frame
# path (correct code/reason), the bare END_STREAM eof path (wrongly
# 1005, ''), and the on_close ws branch (wrongly 1006, ''). This file
# drives each closure shape and verifies the app's receive() stream
# carries exactly one disconnect, paired with the code/reason Www.pod
# "Disconnect - receive event" mandates:
#   - peer Close frame  -> the peer's own code (1005 when the frame
#     carried none) and the peer's reason text;
#   - close WITHOUT a close handshake (bare END_STREAM, RST_STREAM,
#     timeouts) -> 1006 and the 'client_closed' token.

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;
use Protocol::WebSocket::Frame;

use constant H2_CANCEL_CODE => 8;   # RST_STREAM error code CANCEL (RFC 9113)

my $loop = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# ============================================================
# Helpers (lifted from t/http2/07-websocket.t)
# ============================================================

sub create_test_server {
    my (%args) = @_;
    my $server = PAGI::Server->new(
        app   => $args{app} // sub { },
        host  => '127.0.0.1',
        port  => 0,
        quiet => 1,
        http2 => 1,
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

sub open_ws_stream {
    my ($client, $client_sock, $path) = @_;
    $path //= '/ws';

    my $ws_stream_id = $client->submit_request(
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

    return $ws_stream_id;
}

# Collected receive() events for the app under test; reset per subtest.
our @WS_EVENTS;

# Build an app that accepts, drains receive() into @WS_EVENTS until a
# websocket.disconnect arrives, then makes ONE bounded extra receive()
# call to catch a second queued event -- the queue property that proves
# "exactly one". A synthesized fallback 1006 surfacing here only happens
# if the connection was already torn down, which none of these subtests
# do before this check, so any second event caught here is a genuine
# duplicate-enqueue regression.
sub make_ws_app {
    return async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';

        await $send->({ type => 'websocket.accept' });

        my $event = await $receive->();
        while ($event->{type} ne 'websocket.disconnect') {
            push @WS_EVENTS, $event;
            $event = await $receive->();
        }
        push @WS_EVENTS, $event;

        my $extra = await Future->wait_any($receive->(), $loop->delay_future(after => 0.3));
        push @WS_EVENTS, $extra if ref $extra eq 'HASH';
    };
}

sub disconnects_seen {
    return grep { $_->{type} eq 'websocket.disconnect' } @WS_EVENTS;
}

# ============================================================
# Peer Close(4321, "bye") -> exactly one disconnect, 4321/"bye"
# ============================================================
subtest 'peer Close(4321, "bye") delivers exactly one disconnect with the peer code/reason' => sub {
    @WS_EVENTS = ();
    my $app = make_ws_app();
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    my $ws_stream_id = open_ws_stream($client, $client_sock);

    my $close_frame = Protocol::WebSocket::Frame->new(
        type   => 'close',
        buffer => pack('n', 4321) . 'bye',
        masked => 1,
    );
    send_stream_data($client, $client_sock, $ws_stream_id, $close_frame->to_bytes, 1);
    exchange_frames($client, $client_sock, 20);

    my @disconnects = disconnects_seen();
    is(scalar @disconnects, 1, 'exactly one websocket.disconnect event');
    if (@disconnects) {
        is($disconnects[0]{code}, 4321, 'code is the peer\'s code');
        is($disconnects[0]{reason}, 'bye', 'reason is the peer\'s reason text');
    }

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# Peer Close with empty payload -> exactly one disconnect, code 1005
# ============================================================
subtest 'peer Close with empty payload delivers exactly one disconnect, code 1005' => sub {
    @WS_EVENTS = ();
    my $app = make_ws_app();
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    my $ws_stream_id = open_ws_stream($client, $client_sock);

    my $close_frame = Protocol::WebSocket::Frame->new(
        type   => 'close',
        buffer => '',
        masked => 1,
    );
    send_stream_data($client, $client_sock, $ws_stream_id, $close_frame->to_bytes, 1);
    exchange_frames($client, $client_sock, 20);

    my @disconnects = disconnects_seen();
    is(scalar @disconnects, 1, 'exactly one websocket.disconnect event');
    if (@disconnects) {
        is($disconnects[0]{code}, 1005, 'code is 1005 (no status received)');
        is($disconnects[0]{reason}, '', 'reason is empty');
    }

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# Bare END_STREAM (no close frame) -> exactly one disconnect, 1006/client_closed
# ============================================================
subtest 'bare END_STREAM delivers exactly one disconnect, 1006/client_closed' => sub {
    @WS_EVENTS = ();
    my $app = make_ws_app();
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    my $ws_stream_id = open_ws_stream($client, $client_sock);

    # No close frame at all -- just end the client's side of the stream.
    send_stream_data($client, $client_sock, $ws_stream_id, '', 1);
    exchange_frames($client, $client_sock, 20);

    my @disconnects = disconnects_seen();
    is(scalar @disconnects, 1, 'exactly one websocket.disconnect event')
        or diag("saw: " . join(', ', map { "$_->{code}/'$_->{reason}'" } @disconnects));
    if (@disconnects) {
        is($disconnects[0]{code}, 1006, 'code is 1006 (abnormal closure)');
        is($disconnects[0]{reason}, 'client_closed', "reason is 'client_closed'");
    }

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# RST_STREAM -> exactly one disconnect, 1006/client_closed
# ============================================================
subtest 'peer RST_STREAM delivers exactly one disconnect, 1006/client_closed' => sub {
    @WS_EVENTS = ();
    my $app = make_ws_app();
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    my $ws_stream_id = open_ws_stream($client, $client_sock);

    $client->submit_rst_stream($ws_stream_id, H2_CANCEL_CODE);
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 20);

    my @disconnects = disconnects_seen();
    is(scalar @disconnects, 1, 'exactly one websocket.disconnect event');
    if (@disconnects) {
        is($disconnects[0]{code}, 1006, 'code is 1006 (abnormal closure)');
        is($disconnects[0]{reason}, 'client_closed', "reason is 'client_closed'");
    }

    $stream_io->close_now;
    $loop->remove($server);
};

done_testing;
