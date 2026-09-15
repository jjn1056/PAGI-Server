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

# ============================================================
# Test: an h2 WebSocket that completes its closing handshake is a clean end
# ============================================================
# Www.pod "Meaning per scope": a completed WebSocket closing handshake is a
# clean end (on_complete; is_connected() false; disconnect_reason() undef),
# while an abrupt close with no handshake -- a bare END_STREAM, an RST, a
# transport drop -- is abnormal (on_disconnect with the standard token).
#
# On HTTP/2 the peer's Close frame and its END_STREAM can arrive in one DATA
# chunk. _h2_process_ws_frames validates the Close and sets ws_peer_closed
# (the receive half of a completed handshake); the same call queues the
# server's reciprocal Close + END_STREAM, whose serialization fully closes the
# stream and makes _h2_on_close run and mark the scope. _h2_on_body's eof
# branch must therefore leave a completed handshake for _h2_on_close to mark
# complete, and only route a bare END_STREAM (no handshake) to the abnormal
# client_closed end.
#
# The application here accepts, registers both terminal callbacks on
# pagi.connection, then either parks on a Future that never resolves (never
# draining receive() again) or drains its receive() loop. The only thing that
# can deliver its terminal notification is the server's own frame/stream
# handling.
#
# Cases 1 (parked, peer Close) and 2 (draining, peer Close) are the regression:
# RED on base (on_disconnect 'client_closed'), GREEN after (on_complete, no
# reason). Case 3 (bare END_STREAM) is the guard's negative case and case 4
# (sibling isolation) both stay GREEN either way, so the fix cannot over-reach.
# ============================================================

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;
use Protocol::WebSocket::Frame;

my $loop = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# ============================================================
# Harness (lifted from t/http2/31-ws-keepalive-disconnect.t)
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
        on_read      => sub { 0 },
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
    require Net::HTTP2::nghttp2::Session;
    return Net::HTTP2::nghttp2::Session->new_client(
        callbacks => {
            on_begin_headers   => sub { 0 },
            on_header          => sub { 0 },
            on_frame_recv      => sub { 0 },
            on_data_chunk_recv => sub { 0 },
            on_stream_close    => sub { 0 },
        },
    );
}

sub complete_h2_handshake {
    my ($client, $client_sock) = @_;

    $loop->loop_once(0.1);
    my $server_settings = '';
    $client_sock->sysread($server_settings, 4096);

    $client->send_connection_preface;
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
    for (1 .. $rounds) {
        $loop->loop_once(0.1);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length($out);
    }
}

# Pump the loop and the wire until $cond is true or $max_rounds is spent.
# Returns its final truth.
sub pump_until {
    my ($client, $client_sock, $cond, $max_rounds) = @_;
    $max_rounds //= 40;
    for (1 .. $max_rounds) {
        return 1 if $cond->();
        $loop->loop_once(0.05);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length($out);
    }
    return $cond->() ? 1 : 0;
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
        body      => sub { return undef },   # streaming: keep the stream open
    );
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock);

    return $ws_stream_id;
}

sub client_close_frame {
    my ($code, $reason) = @_;
    $reason //= '';
    return Protocol::WebSocket::Frame->new(
        type   => 'close',
        buffer => pack('n', $code) . $reason,
        masked => 1,
    )->to_bytes;
}

# An app that accepts, registers the terminal callbacks on pagi.connection
# recording the object's state at the instant each fires, then parks on a
# Future that never resolves -- it never returns and never drains receive()
# again while the test observes it. Observations are keyed by scope path so a
# two-stream connection keeps sibling scopes apart. The connection object is
# stashed so the test can read is_connected() on a sibling whose callbacks
# never fire.
sub parked_ws_app {
    my ($obs, $park) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        my $path = $scope->{path} // '/ws';
        my $c    = $scope->{'pagi.connection'};
        $obs->{$path}{conn} = $c;

        await $receive->();                        # websocket.connect
        await $send->({ type => 'websocket.accept' });

        $c->on_complete(sub {
            $obs->{$path}{complete}++;
            $obs->{$path}{complete_connected} = $c->is_connected ? 1 : 0;
            $obs->{$path}{complete_reason}    = $c->disconnect_reason;
        });
        $c->on_disconnect(sub {
            my ($reason) = @_;
            $obs->{$path}{disconnect}++;
            $obs->{$path}{disconnect_connected} = $c->is_connected ? 1 : 0;
            $obs->{$path}{disconnect_reason}    = $reason;
        });

        $obs->{$path}{parked} = 1;
        await $park;                               # never resolves while observed
        $obs->{$path}{returned} = 1;
        return;
    };
}

# An app that accepts, registers the terminal callbacks, then drains its
# receive() loop until the scope's websocket.disconnect arrives, does one turn
# of async work, and returns -- the non-parked shape that pins the broader
# (not parked-specific) bug.
#
# The yield after the disconnect is load-bearing to the reproduction, not a
# nicety. The peer's Close frame and its END_STREAM arrive in one DATA chunk:
# _h2_process_ws_frames validates the Close and wakes this app's parked
# receive() synchronously (Future::AsyncAwait resumes inline off ->done),
# still inside _h2_on_body, BEFORE _h2_on_body's own eof branch runs. An app
# that returns in that same synchronous tick reaches the dispatch wrapper's
# clean-end tail first, which marks the scope complete before the eof branch
# can mismark it -- so a straight receive-and-return app is incidentally saved
# by its own return and never shows the bug. A realistic app does some async
# work on disconnect (a log flush, a cleanup await) before returning; one turn
# of the event loop models the smallest such gap, and in it the eof branch
# runs on an unmarked scope and mismarks it 'client_closed' on base.
sub draining_ws_app {
    my ($obs) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        my $path = $scope->{path} // '/ws';
        my $c    = $scope->{'pagi.connection'};
        $obs->{$path}{conn} = $c;

        await $receive->();                        # websocket.connect
        await $send->({ type => 'websocket.accept' });

        $c->on_complete(sub {
            $obs->{$path}{complete}++;
            $obs->{$path}{complete_connected} = $c->is_connected ? 1 : 0;
            $obs->{$path}{complete_reason}    = $c->disconnect_reason;
        });
        $c->on_disconnect(sub {
            my ($reason) = @_;
            $obs->{$path}{disconnect}++;
            $obs->{$path}{disconnect_reason} = $reason;
        });

        $obs->{$path}{accepted} = 1;
        while (1) {
            my $event = await $receive->();
            if ($event->{type} eq 'websocket.disconnect') {
                $obs->{$path}{saw_disconnect_event} = 1;
                await $loop->delay_future(after => 0);  # one turn of async work
                last;
            }
        }
        $obs->{$path}{returned} = 1;
        return;
    };
}

# ============================================================
# 1. Parked app, peer Close(1000,'bye')+END_STREAM -> on_complete, without the
#    app returning. RED on base (on_disconnect 'client_closed'); GREEN after.
# ============================================================
subtest 'parked app: peer Close is a clean end -> on_complete, no reason' => sub {
    my %obs;
    my $park = $loop->new_future;
    my $app  = parked_ws_app(\%obs, $park);
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    my $sid = open_ws_stream($client, $client_sock);
    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{parked} }),
        'app accepted and parked');

    send_stream_data($client, $client_sock, $sid, client_close_frame(1000, 'bye'), 1);

    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{complete} }),
        'on_complete fired for the parked app without it returning');
    is($obs{'/ws'}{complete}, 1, 'on_complete fired exactly once');
    is($obs{'/ws'}{complete_connected}, 0, 'is_connected() was false when on_complete fired');
    is($obs{'/ws'}{complete_reason}, undef,
        'disconnect_reason() was undef -- a completed handshake is a clean end');
    ok(!$obs{'/ws'}{disconnect}, 'on_disconnect did NOT fire (clean end, not abnormal)');
    ok(!$obs{'/ws'}{returned},
        'the app never returned -- the notice arrived without it draining receive()');

    eval { $stream_io->close_now };
    $loop->remove($server);
};

# ============================================================
# 2. Draining (non-parked) app, peer clean Close -> on_complete, not
#    on_disconnect. Pins the broader (not parked-specific) bug. RED on base;
#    GREEN after.
# ============================================================
subtest 'draining app: peer Close ends the scope via on_complete, not on_disconnect' => sub {
    my %obs;
    my $app = draining_ws_app(\%obs);
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    my $sid = open_ws_stream($client, $client_sock);
    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{accepted} }),
        'app accepted and entered its receive loop');

    send_stream_data($client, $client_sock, $sid, client_close_frame(1000, 'bye'), 1);

    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{returned} }),
        'the app drained the disconnect event and returned');
    ok($obs{'/ws'}{saw_disconnect_event},
        'the app received the peer Close as a websocket.disconnect');
    is($obs{'/ws'}{complete}, 1, 'on_complete fired');
    is($obs{'/ws'}{complete_reason}, undef,
        'disconnect_reason() was undef -- a completed handshake is a clean end');
    ok(!$obs{'/ws'}{disconnect}, 'on_disconnect did NOT fire (clean end, not abnormal)');

    eval { $stream_io->close_now };
    $loop->remove($server);
};

# ============================================================
# 3. Bare END_STREAM (no Close frame) -> still abnormal client_closed. This is
#    a genuine abrupt close; the guard must NOT turn it clean. GREEN both ways.
# ============================================================
subtest 'parked app: bare END_STREAM is abnormal -> on_disconnect client_closed' => sub {
    my %obs;
    my $park = $loop->new_future;
    my $app  = parked_ws_app(\%obs, $park);
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    my $sid = open_ws_stream($client, $client_sock);
    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{parked} }),
        'app accepted and parked');

    # No Close frame at all -- just end the client's side of the stream.
    send_stream_data($client, $client_sock, $sid, '', 1);

    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{disconnect} }),
        'on_disconnect fired for the parked app without it returning');
    is($obs{'/ws'}{disconnect}, 1, 'on_disconnect fired exactly once');
    is($obs{'/ws'}{disconnect_connected}, 0, 'is_connected() was false when on_disconnect fired');
    is($obs{'/ws'}{disconnect_reason}, 'client_closed',
        "on_disconnect carried 'client_closed' -- a bare END_STREAM is a client-gone close");
    ok(!$obs{'/ws'}{complete}, 'on_complete did NOT fire (abnormal end, not clean)');
    ok(!$obs{'/ws'}{returned}, 'the app never returned');

    eval { $stream_io->close_now };
    $loop->remove($server);
};

# ============================================================
# 4. Sibling-stream isolation: two accepted WS streams; the peer cleanly
#    closes one. The other's pagi.connection stays connected and its app keeps
#    running. GREEN both ways -- the end is per-stream on either code path.
# ============================================================
subtest 'ending one h2 WS stream leaves its sibling connected and running' => sub {
    my %obs;
    my $park = $loop->new_future;
    my $app  = parked_ws_app(\%obs, $park);
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    my $sid_a = open_ws_stream($client, $client_sock, '/wsA');
    my $sid_b = open_ws_stream($client, $client_sock, '/wsB');
    ok(pump_until($client, $client_sock,
            sub { $obs{'/wsA'}{parked} && $obs{'/wsB'}{parked} }),
        'both apps accepted and parked');

    # Cleanly close stream A only.
    send_stream_data($client, $client_sock, $sid_a, client_close_frame(1000, 'bye'), 1);

    ok(pump_until($client, $client_sock,
            sub { $obs{'/wsA'}{complete} || $obs{'/wsA'}{disconnect} }),
        'stream A received its terminal notification');

    # Give the loop a few more turns; the sibling must not be swept along.
    exchange_frames($client, $client_sock, 5);

    ok(!$obs{'/wsB'}{complete} && !$obs{'/wsB'}{disconnect},
        'stream B got no terminal notification');
    ok($obs{'/wsB'}{conn}->is_connected,
        "stream B's pagi.connection is still connected");
    is($obs{'/wsB'}{conn}->disconnect_reason, undef,
        "stream B has no disconnect reason");
    ok(!$obs{'/wsB'}{returned}, "stream B's app is still running");

    eval { $stream_io->close_now };
    $loop->remove($server);
};

done_testing;
