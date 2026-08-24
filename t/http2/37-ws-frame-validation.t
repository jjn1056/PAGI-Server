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
# Test: h2 WebSocket RFC 6455 framing enforcement (audit-found gap)
# ============================================================
# h1's _process_websocket_frames enforces three RFC 6455 framing rules
# before dispatching on opcode: nonzero RSV bits close with 1002 (section
# 5.2), reserved/unknown opcodes 3-7 and 11-15 close with 1002 (section
# 5.2), and an oversized control-frame payload (>125 bytes) closes with
# 1002 (section 5.5). h2's _h2_process_ws_frames had none of these checks
# -- Protocol::WebSocket::Frame itself only exposes rsv/opcode, it doesn't
# validate them -- so a client could send any of these three malformed
# frames over an h2 WebSocket stream and the server would silently accept
# (or mishandle) them instead of closing per spec. Per Www.pod's RFC 8441
# claim ("identical to HTTP/1.1"), h2 must enforce the same rules h1 does.
#
# Each subtest below hand-crafts one malformed frame, sends it as h2 DATA
# on an accepted WS stream, and checks both sides of the contract: the
# wire (server sends a Close frame with code 1002) and the app (receive()
# delivers exactly one websocket.disconnect, code 1002, reason
# 'protocol_error' -- the same server-initiated-protocol-close pairing the
# existing 1007 invalid-UTF-8 path already uses).

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;
use Protocol::WebSocket::Frame;

my $loop = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# ============================================================
# Helpers (lifted from t/http2/31-ws-keepalive-disconnect.t /
# t/http2/36-ws-transport.t)
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
        max_body_size => $server->{max_body_size},
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

# Every status code carried by a Close frame (opcode 8) seen on the wire so
# far. A Close frame with a 0-byte payload carries no code and contributes
# nothing.
sub close_codes {
    my ($raw) = @_;
    return map  { unpack('n', substr($_->{bytes}, 0, 2)) }
           grep { $_->{opcode} == 8 && length($_->{bytes}) >= 2 }
           extract_ws_frames($raw);
}

# Build an app that accepts, drains receive() into $events until a
# websocket.disconnect arrives, then makes ONE bounded extra receive()
# call to catch a second queued event -- the queue property that proves
# "exactly one" (mirrors t/http2/31-ws-keepalive-disconnect.t's
# make_ws_app). A synthesized fallback surfacing here only happens if the
# connection was already torn down, which these subtests don't do before
# this check, so any second event caught here is a genuine
# duplicate-enqueue regression.
sub make_ws_app {
    my ($events) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';

        await $send->({ type => 'websocket.accept' });

        my $event = await $receive->();
        while ($event->{type} ne 'websocket.disconnect') {
            push @$events, $event;
            $event = await $receive->();
        }
        push @$events, $event;

        my $extra = await Future->wait_any($receive->(), $loop->delay_future(after => 0.3));
        push @$events, $extra if ref $extra eq 'HASH';
    };
}

sub disconnects_seen {
    my ($events) = @_;
    return grep { $_->{type} eq 'websocket.disconnect' } @$events;
}

# Poll until the app has recorded a websocket.disconnect (or the ceiling is
# hit). 30 rounds (3s) is a wide margin over the single in-process frame
# exchange each of these malformed-frame subtests needs. The wire-level
# Close frame the server queues at the same time is flushed to the h2
# session and pulled by the client's own recv/send pump one round behind
# the app-level event (queuing and app delivery happen synchronously
# in-process; handing the queued DATA frame to the socket happens on the
# next round-trip) -- a handful of extra rounds after the app event settles
# that without the caller needing to know the exact offset.
sub wait_for_disconnect {
    my ($client, $client_sock, $events) = @_;
    for (1 .. 30) {
        exchange_frames($client, $client_sock, 1);
        if (disconnects_seen($events)) {
            exchange_frames($client, $client_sock, 5);
            return 1;
        }
    }
    return 0;
}

# ============================================================
# RSV bits must be 0 (RFC 6455 section 5.2)
# ============================================================
subtest 'nonzero RSV bit closes h2 WebSocket stream with 1002' => sub {
    my @events;
    my $app = make_ws_app(\@events);
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

    # RSV1 set on an otherwise-ordinary text frame -- no extension was
    # negotiated, so this is a protocol violation regardless of payload.
    my $bad = Protocol::WebSocket::Frame->new(
        type   => 'text',
        buffer => 'hello',
        masked => 1,
        rsv    => [1, 0, 0],
    );
    send_stream_data($client, $client_sock, $ws_stream_id, $bad->to_bytes);

    ok(wait_for_disconnect($client, $client_sock, \@events),
        'app observed a websocket.disconnect within the bounded window');

    my @disconnects = disconnects_seen(\@events);
    is(scalar(@disconnects), 1, 'exactly one websocket.disconnect delivered');
    if (@disconnects) {
        is($disconnects[0]{code}, 1002, 'code is 1002 (protocol error)');
        is($disconnects[0]{reason}, 'protocol_error', "reason is 'protocol_error'");
    }

    my @codes = close_codes($ws_data);
    is(scalar(@codes), 1, 'server sent exactly one Close frame on the wire');
    is($codes[0], 1002, 'wire Close frame carries code 1002') if @codes;

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# Reserved/unknown opcodes 3-7 and 11-15 (RFC 6455 section 5.2)
# ============================================================
subtest 'reserved opcode closes h2 WebSocket stream with 1002' => sub {
    my @events;
    my $app = make_ws_app(\@events);
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

    # Opcode 3 is a reserved non-control opcode (RFC 6455 5.2).
    my $bad = Protocol::WebSocket::Frame->new(
        buffer => 'hello',
        masked => 1,
        opcode => 3,
    );
    send_stream_data($client, $client_sock, $ws_stream_id, $bad->to_bytes);

    ok(wait_for_disconnect($client, $client_sock, \@events),
        'app observed a websocket.disconnect within the bounded window');

    my @disconnects = disconnects_seen(\@events);
    is(scalar(@disconnects), 1, 'exactly one websocket.disconnect delivered');
    if (@disconnects) {
        is($disconnects[0]{code}, 1002, 'code is 1002 (protocol error)');
        is($disconnects[0]{reason}, 'protocol_error', "reason is 'protocol_error'");
    }

    my @codes = close_codes($ws_data);
    is(scalar(@codes), 1, 'server sent exactly one Close frame on the wire');
    is($codes[0], 1002, 'wire Close frame carries code 1002') if @codes;

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# Control frame payload > 125 bytes (RFC 6455 section 5.5)
# ============================================================
subtest 'oversized control frame closes h2 WebSocket stream with 1002' => sub {
    my @events;
    my $app = make_ws_app(\@events);
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

    # Ping (opcode 9) with a 126-byte payload -- one over RFC 6455 5.5's
    # 125-byte control-frame limit.
    my $bad = Protocol::WebSocket::Frame->new(
        type   => 'ping',
        buffer => ('x' x 126),
        masked => 1,
    );
    send_stream_data($client, $client_sock, $ws_stream_id, $bad->to_bytes);

    ok(wait_for_disconnect($client, $client_sock, \@events),
        'app observed a websocket.disconnect within the bounded window');

    my @disconnects = disconnects_seen(\@events);
    is(scalar(@disconnects), 1, 'exactly one websocket.disconnect delivered');
    if (@disconnects) {
        is($disconnects[0]{code}, 1002, 'code is 1002 (protocol error)');
        is($disconnects[0]{reason}, 'protocol_error', "reason is 'protocol_error'");
    }

    my @codes = close_codes($ws_data);
    is(scalar(@codes), 1, 'server sent exactly one Close frame on the wire');
    is($codes[0], 1002, 'wire Close frame carries code 1002') if @codes;

    $stream_io->close_now;
    $loop->remove($server);
};

done_testing;
