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
use Scalar::Util qw(refaddr);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.011+ required)');
}

# =============================================================================
# Test: h2 app-initiated WebSocket close waits for the peer, per stream, under a
# finite close deadline, instead of deciding a clean end eagerly at the app's
# websocket.close send. The h2 twin of t/ws-close-deadline-h1.t.
#
# Www.pod L857-869 / design spec "WebSocket Close Truthfulness": when the
# application sends websocket.close on an accepted socket, that STREAM enters a
# PENDING closing phase. A completed handshake -- the peer's Close AND the
# stream then closing -- is the ONLY clean end. The wait is bounded by a finite
# per-stream close deadline (ws_close_timeout); its expiry with no peer Close is
# abnormal close_timeout / close_code 1006, an END_STREAM or RST with no peer
# Close is the transport-loss token / 1006 (NOT close_timeout), and a peer Close
# observed before an abnormal outcome keeps its own code/reason.
#
# The PANEL ruling: RST_STREAM(NO_ERROR) AFTER a valid peer Close is a NORMAL
# close (the handshake completed), NOT transport loss.
#
# These cases are all APP-INITIATED (the handler sends websocket.close). The
# peer-initiated close (peer sends Close first) is a completed handshake at the
# reciprocal-Close point and stays clean -- covered by t/http2/47 and
# t/http2/48, not here.
# =============================================================================

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;
use Protocol::WebSocket::Frame;

my $loop = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# ============================================================
# Harness (lifted from t/http2/48-ws-close-code.t)
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
        stream           => $stream,
        app              => $app,
        protocol         => $protocol,
        server           => $server,
        h2_protocol      => $server->{http2_protocol},
        alpn_protocol    => 'h2',
        ws_close_timeout => $overrides{ws_close_timeout} // 10,
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

sub send_rst_stream {
    my ($client, $client_sock, $stream_id, $error_code) = @_;
    $client->submit_rst_stream($stream_id, $error_code);
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
sub pump_until {
    my ($client, $client_sock, $cond, $max_rounds) = @_;
    $max_rounds //= 60;
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

# Pump for a fixed number of rounds (used to prove a scope stays PENDING while
# the peer is silent and the deadline has not yet elapsed).
sub pump_rounds {
    my ($client, $client_sock, $rounds) = @_;
    for (1 .. $rounds) {
        $loop->loop_once(0.05);
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
        body      => sub { return undef },   # streaming: keep the stream open
    );
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 4);

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

# ------------------------------------------------------------
# Applications
# ------------------------------------------------------------

# An accepted WebSocket that registers terminal-callback observers, sends its
# OWN websocket.close (app-initiated), then either parks or returns, per $opt.
sub app_initiated_close {
    my ($obs, $park, %opt) = @_;
    my $code   = $opt{code}   // 1000;
    my $reason = $opt{reason} // 'bye';
    return async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        my $path = $scope->{path} // '/ws';
        my $c    = $scope->{'pagi.connection'};
        $obs->{$path}{conn} = $c;

        await $receive->();                       # websocket.connect
        await $send->({ type => 'websocket.accept' });

        $c->on_complete(sub {
            $obs->{$path}{complete}++;
            $obs->{$path}{code}            = $c->close_code;
            $obs->{$path}{reason}          = $c->close_reason;
            $obs->{$path}{complete_reason} = $c->disconnect_reason;
        });
        $c->on_disconnect(sub {
            my ($r) = @_;
            $obs->{$path}{disconnect}++;
            $obs->{$path}{disconnect_reason} = $r;
            $obs->{$path}{code}   = $c->close_code;
            $obs->{$path}{reason} = $c->close_reason;
        });
        $c->on_end(sub { $obs->{$path}{end}++ });
        $c->end_future->on_ready(sub { $obs->{$path}{end_future}++ });

        await $send->({ type => 'websocket.close', code => $code, reason => $reason });
        $obs->{$path}{closed_sent} = 1;

        if ($opt{park}) {
            await $park;
            $obs->{$path}{returned} = 1;
        }
        return;
    };
}

# An accepted WebSocket the application walks away from WITHOUT any closing
# handshake (send no websocket.close). This is the incomplete-scope case that
# must stay 1011 / server_error, untouched by the closing-phase work.
sub app_walks_away {
    my ($obs) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        my $path = $scope->{path} // '/ws';
        my $c    = $scope->{'pagi.connection'};
        $obs->{$path}{conn} = $c;

        await $receive->();
        await $send->({ type => 'websocket.accept' });

        $c->on_complete(sub {
            $obs->{$path}{complete}++;
        });
        $c->on_disconnect(sub {
            my ($r) = @_;
            $obs->{$path}{disconnect}++;
            $obs->{$path}{disconnect_reason} = $r;
        });
        # returns immediately, no websocket.close
        return;
    };
}

# ============================================================
# 1. app close + peer Close + stream closes cleanly -> CLEAN.
#    SPLIT: while the peer is silent the scope is PENDING (neither terminal
#    fired); the peer's Close + END_STREAM then completes it clean, close_code
#    the peer's own.
# ============================================================
subtest 'app close, peer answers, stream closes -> CLEAN (pending until then)' => sub {
    my %obs;
    my $park = $loop->new_future;
    my $app  = app_initiated_close(\%obs, $park, code => 1000, reason => 'bye', park => 1);
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    my $sid = open_ws_stream($client, $client_sock);
    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{closed_sent} }),
        'app accepted and sent its websocket.close');

    # Peer stays silent: the scope must stay PENDING (deadline is 10s, far off).
    pump_rounds($client, $client_sock, 6);
    ok(!$obs{'/ws'}{complete},   'not complete while the peer is silent (PENDING)');
    ok(!$obs{'/ws'}{disconnect}, 'not disconnected while the peer is silent (PENDING)');

    # Peer completes the handshake: its Close + END_STREAM.
    send_stream_data($client, $client_sock, $sid, client_close_frame(1000, 'bye'), 1);

    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{complete} }),
        'on_complete fired once the peer completed the handshake');
    is($obs{'/ws'}{code},   1000,  'close_code is the peer code (1000)');
    is($obs{'/ws'}{reason}, 'bye', 'close_reason is the peer text');
    is($obs{'/ws'}{complete_reason}, undef, 'disconnect_reason undef -- a clean end');
    ok(!$obs{'/ws'}{disconnect}, 'on_disconnect did NOT fire (clean end)');

    $park->done unless $park->is_ready;
    eval { $stream_io->close_now };
    $loop->remove($server);
};

# ============================================================
# 2. app close, peer silent, the deadline expires -> close_timeout / 1006.
#    (app returns after sending its close.)
# ============================================================
subtest 'app close, peer silent, deadline expires -> close_timeout/1006' => sub {
    my %obs;
    my $park = $loop->new_future;
    my $app  = app_initiated_close(\%obs, $park, park => 0);
    my ($conn, $stream_io, $client_sock, $server) =
        create_h2_connection(app => $app, ws_close_timeout => 0.3);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    open_ws_stream($client, $client_sock);

    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{disconnect} }, 80),
        'on_disconnect fired on deadline expiry');
    is($obs{'/ws'}{disconnect_reason}, 'close_timeout', 'disconnect_reason is close_timeout');
    is($obs{'/ws'}{code}, 1006, 'close_code is 1006');
    is($obs{'/ws'}{reason}, undef, 'close_reason is undef');
    ok(!$obs{'/ws'}{complete}, 'on_complete did NOT fire');

    eval { $stream_io->close_now };
    $loop->remove($server);
};

# ============================================================
# 3. app close then PARKS, peer silent, the deadline expires -> close_timeout.
#    The deadline is armed at the SEND, so a parked handler is still bounded
#    (the original unbounded-wait bug).
# ============================================================
subtest 'app close then PARK, peer silent, deadline expires -> close_timeout/1006' => sub {
    my %obs;
    my $park = $loop->new_future;
    my $app  = app_initiated_close(\%obs, $park, park => 1);
    my ($conn, $stream_io, $client_sock, $server) =
        create_h2_connection(app => $app, ws_close_timeout => 0.3);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    open_ws_stream($client, $client_sock);
    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{closed_sent} }),
        'app sent its close and parked');

    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{disconnect} }, 80),
        'on_disconnect fired on deadline expiry despite the parked handler');
    is($obs{'/ws'}{disconnect_reason}, 'close_timeout', 'disconnect_reason is close_timeout');
    is($obs{'/ws'}{code}, 1006, 'close_code is 1006');
    ok(!$obs{'/ws'}{complete}, 'on_complete did NOT fire');

    $park->done unless $park->is_ready;
    eval { $stream_io->close_now };
    $loop->remove($server);
};

# ============================================================
# 4. app close (parked), peer answers with Close + END_STREAM -> CLEAN.
# ============================================================
subtest 'app close (parked), peer answers -> CLEAN' => sub {
    my %obs;
    my $park = $loop->new_future;
    my $app  = app_initiated_close(\%obs, $park, code => 1001, reason => 'later', park => 1);
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    my $sid = open_ws_stream($client, $client_sock);
    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{closed_sent} }),
        'app sent its close and parked');

    send_stream_data($client, $client_sock, $sid, client_close_frame(1001, 'later'), 1);

    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{complete} }),
        'on_complete fired');
    is($obs{'/ws'}{code}, 1001, 'close_code is the peer code');
    is($obs{'/ws'}{complete_reason}, undef, 'disconnect_reason undef -- clean');
    ok(!$obs{'/ws'}{disconnect}, 'on_disconnect did NOT fire');

    $park->done unless $park->is_ready;
    eval { $stream_io->close_now };
    $loop->remove($server);
};

# ============================================================
# 5. app close, then a stream RST with no peer Close -> transport-loss / 1006
#    (NOT close_timeout, NOT server_error).
# ============================================================
subtest 'app close, stream RST with no peer Close -> transport-loss/1006' => sub {
    my %obs;
    my $park = $loop->new_future;
    my $app  = app_initiated_close(\%obs, $park, park => 1);
    # Long deadline so the RST, not the deadline, is what ends the scope.
    my ($conn, $stream_io, $client_sock, $server) =
        create_h2_connection(app => $app, ws_close_timeout => 30);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    my $sid = open_ws_stream($client, $client_sock);
    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{closed_sent} }),
        'app sent its close and parked');

    send_rst_stream($client, $client_sock, $sid, 8);   # CANCEL

    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{disconnect} }),
        'on_disconnect fired on the stream reset');
    isnt($obs{'/ws'}{disconnect_reason}, 'close_timeout', 'specifically NOT close_timeout');
    isnt($obs{'/ws'}{disconnect_reason}, 'server_error', 'specifically NOT server_error');
    is($obs{'/ws'}{code}, 1006, 'close_code is 1006 (no peer Close)');
    ok(!$obs{'/ws'}{complete}, 'on_complete did NOT fire');

    $park->done unless $park->is_ready;
    eval { $stream_io->close_now };
    $loop->remove($server);
};

# ============================================================
# 6. app returns WITHOUT a Close -> 1011 / server_error (UNCHANGED).
# ============================================================
subtest 'app returns without a Close -> server_error (unchanged)' => sub {
    my %obs;
    my @logs;
    my $app    = app_walks_away(\%obs);
    my $server = create_test_server(app => $app,
        logger => sub { push @logs, $_[0]->{message} // '' });
    my ($conn, $stream_io, $client_sock) =
        create_h2_connection(app => $app, server => $server);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    open_ws_stream($client, $client_sock);

    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{disconnect} }),
        'on_disconnect fired');
    is($obs{'/ws'}{disconnect_reason}, 'server_error',
        'an accepted socket left without a closing handshake -> server_error');
    ok(!$obs{'/ws'}{complete}, 'on_complete did NOT fire');
    ok(scalar(grep { /without ending the scope cleanly/ } @logs),
        'the expected server_error was logged (captured, not leaked)');

    eval { $stream_io->close_now };
    $loop->remove($server);
};

# ============================================================
# 7. STAGED (app-initiated), WS-CLOSE-TRUTH-4: the peer answers with a valid
#    Close but WITHHOLDS its END_STREAM forever. Per WS-CLOSE-TRUTH-3 the
#    app-initiated close_timeout deadline is DISPOSED the instant the peer
#    answers (a peer that replied is not silent -> never close_timeout);
#    WS-CLOSE-TRUTH-4 then ARMS a per-stream close-FINISH bound (ws_close_timeout)
#    in its place, so a half-closed stream that can never finish is BOUNDED
#    rather than left hanging. On expiry the stream is RST and the scope ends
#    abnormally with the DISTINCT reason close_incomplete, which PRESERVES the
#    peer's close_code/close_reason (the peer sent a valid Close; category 4,
#    WS-CLOSE-TRUTH-5) -- NOT close_timeout (the peer replied), NOT server_error.
#    on_disconnect + on_end fire once, never on_complete. Observable stages:
#      (a) after the Close WITHOUT END_STREAM the close_timeout deadline is
#          disposed and a FRESH finish bound is armed in its place; NO terminal
#          has fired -- the scope is PENDING;
#      (b) the client NEVER sends its END_STREAM; the finish bound expires ->
#          close_incomplete with the peer's code, the stream RST.
#    Do NOT send Close+END_STREAM together -- that completes the stream and
#    never arms the finish edge. The clean contrast (END_STREAM before the
#    bound) is subtest 7b. (Was, pre-WS-CLOSE-TRUTH-4: "peer Close disarms the
#    deadline, scope PENDING forever" -- an UNRESOLVED half-closed case now
#    bounded by the finish bound.)
# ============================================================
subtest 'app close, peer Close WITHOUT END_STREAM, withheld -> close_incomplete (peer code preserved)' => sub {
    my %obs;
    my $park = $loop->new_future;
    # The app's own Close code (1000/'bye') is DISTINCT from the peer's
    # (1001/'peerbye'), so preserving the peer's code cannot pass on app intent.
    my $app  = app_initiated_close(\%obs, $park, code => 1000, reason => 'bye', park => 1);
    # A short finish bound: with the END_STREAM withheld, it is what ends the
    # scope, within the pump window below.
    my ($conn, $stream_io, $client_sock, $server) =
        create_h2_connection(app => $app, ws_close_timeout => 0.5);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    my $sid = open_ws_stream($client, $client_sock);
    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{closed_sent} }),
        'app sent its close and parked');

    my $ss = $conn->{h2_streams}{$sid};
    ok($ss, 'the closing-phase stream is live');
    my $close_deadline = $ss->{ws_close_deadline};
    ok($close_deadline && $close_deadline->is_running,
        'the app-initiated close deadline is armed while the peer is silent');

    # STAGE (a): peer sends its valid Close frame but NOT END_STREAM.
    send_stream_data($client, $client_sock, $sid, client_close_frame(1001, 'peerbye'), 0);
    ok(pump_until($client, $client_sock, sub { $ss->{ws_peer_closed} }),
        'the server recorded the peer Close (ws_peer_closed)');

    # The close_timeout deadline is disposed and a FRESH finish bound armed in
    # its place -- a different timer occupying the same per-stream slot.
    my $finish_bound = $ss->{ws_close_deadline};
    ok($finish_bound && $finish_bound->is_running,
        'a per-stream close-finish bound is armed after the peer Close');
    isnt(refaddr($finish_bound), refaddr($close_deadline),
        'it is a FRESH bound, not the leftover close_timeout deadline');
    ok(!$obs{'/ws'}{complete},   'PENDING: not complete (END_STREAM withheld)');
    ok(!$obs{'/ws'}{disconnect}, 'PENDING: not yet disconnected');

    # STAGE (b): the client NEVER sends its END_STREAM; the finish bound expires.
    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{disconnect} }, 80),
        'on_disconnect fired when the close-finish bound expired');
    is($obs{'/ws'}{disconnect_reason}, 'close_incomplete',
        'disconnect_reason is close_incomplete (WS-CLOSE-TRUTH-5)');
    isnt($obs{'/ws'}{disconnect_reason}, 'close_timeout', 'specifically NOT close_timeout');
    is($obs{'/ws'}{code}, 1001, 'close_code is the PEER code 1001, preserved (WS-CLOSE-TRUTH-5)');
    is($obs{'/ws'}{reason}, 'peerbye', 'close_reason is the peer text, preserved');
    is($obs{'/ws'}{disconnect}, 1, 'on_disconnect fired exactly once');
    is($obs{'/ws'}{end}, 1, 'on_end fired exactly once');
    ok(!$obs{'/ws'}{complete}, 'on_complete did NOT fire');

    # The finish bound was disposed on the terminal path (dequeued + stopped).
    ok(!$ss->{ws_close_deadline}, 'the finish bound was disposed (dequeued)');
    ok(!$finish_bound->is_running, 'the finish-bound timer was stopped (no late fire)');

    $park->done unless $park->is_ready;
    eval { $stream_io->close_now };
    $loop->remove($server);
};

# ============================================================
# 7b. CLEAN CONTRAST to 7: the peer answers with its Close WITHOUT END_STREAM
#     (arming the finish bound), then sends its END_STREAM BEFORE the bound
#     expires -> full stream closure -> CLEAN, the peer's own code/reason
#     preserved, the finish bound disposed without firing. A generous
#     ws_close_timeout so the END_STREAM, not the bound, is what ends the scope.
# ============================================================
subtest 'app close, peer Close WITHOUT END_STREAM, then END_STREAM before the bound -> CLEAN' => sub {
    my %obs;
    my $park = $loop->new_future;
    my $app  = app_initiated_close(\%obs, $park, code => 1000, reason => 'bye', park => 1);
    my ($conn, $stream_io, $client_sock, $server) =
        create_h2_connection(app => $app, ws_close_timeout => 30);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    my $sid = open_ws_stream($client, $client_sock);
    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{closed_sent} }),
        'app sent its close and parked');

    my $ss = $conn->{h2_streams}{$sid};

    # Peer Close WITHOUT END_STREAM -> the finish bound is armed.
    send_stream_data($client, $client_sock, $sid, client_close_frame(1000, 'peerbye'), 0);
    ok(pump_until($client, $client_sock, sub { $ss->{ws_peer_closed} }),
        'the server recorded the peer Close');
    my $finish_bound = $ss->{ws_close_deadline};
    ok($finish_bound && $finish_bound->is_running, 'the finish bound is armed');
    pump_rounds($client, $client_sock, 6);
    ok(!$obs{'/ws'}{complete},   'still PENDING before the client END_STREAM');
    ok(!$obs{'/ws'}{disconnect}, 'no abnormal terminal while the bound is far off');

    # The client sends its END_STREAM before the (30s) bound -> full closure -> CLEAN.
    send_stream_data($client, $client_sock, $sid, '', 1);
    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{complete} }),
        'on_complete fired at full stream closure');
    is($obs{'/ws'}{complete}, 1, 'on_complete fired exactly once');
    is($obs{'/ws'}{code},   1000,      'close_code is the peer code, preserved');
    is($obs{'/ws'}{reason}, 'peerbye', 'close_reason is the peer text, preserved');
    is($obs{'/ws'}{complete_reason}, undef, 'disconnect_reason undef -- a clean end');
    ok(!$obs{'/ws'}{disconnect}, 'on_disconnect did NOT fire (clean end)');
    ok(!$ss->{ws_close_deadline}, 'the finish bound was disposed on the clean closure');
    ok(!$finish_bound->is_running, 'the finish-bound timer was stopped (no late fire)');

    $park->done unless $park->is_ready;
    eval { $stream_io->close_now };
    $loop->remove($server);
};

# ============================================================
# 8. normal close never trips the closing-phase entry guard (no server_error
#    log from the bounded-wait invariant).
# ============================================================
subtest 'normal close never trips the closing-phase guard' => sub {
    my %obs;
    my @logs;
    my $park = $loop->new_future;
    my $app  = app_initiated_close(\%obs, $park, park => 1);
    my $server = create_test_server(app => $app,
        logger => sub { push @logs, $_[0]->{message} // '' });
    my ($conn, $stream_io, $client_sock) =
        create_h2_connection(app => $app, server => $server);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    my $sid = open_ws_stream($client, $client_sock);
    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{closed_sent} }),
        'app sent its close and parked');
    send_stream_data($client, $client_sock, $sid, client_close_frame(1000, 'bye'), 1);
    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{complete} }),
        'clean end');

    ok(!scalar(grep { /bounded-wait invariant/ } @logs),
        'the bounded-wait guard was never tripped on the normal path');
    ok(!scalar(grep { /failed to enter WebSocket closing phase/ } @logs),
        'no closing-phase entry failure logged');

    $park->done unless $park->is_ready;
    eval { $stream_io->close_now };
    $loop->remove($server);
};

# ============================================================
# 9. END_STREAM with no Close -> transport-loss / 1006 (NOT close_timeout).
# ============================================================
subtest 'app close, peer bare END_STREAM (no Close) -> transport-loss/1006' => sub {
    my %obs;
    my $park = $loop->new_future;
    my $app  = app_initiated_close(\%obs, $park, park => 1);
    my ($conn, $stream_io, $client_sock, $server) =
        create_h2_connection(app => $app, ws_close_timeout => 30);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    my $sid = open_ws_stream($client, $client_sock);
    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{closed_sent} }),
        'app sent its close and parked');

    send_stream_data($client, $client_sock, $sid, '', 1);   # bare END_STREAM, no Close

    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{disconnect} }),
        'on_disconnect fired on the premature stream termination');
    is($obs{'/ws'}{disconnect_reason}, 'client_closed',
        'transport-loss token client_closed (NOT close_timeout)');
    isnt($obs{'/ws'}{disconnect_reason}, 'close_timeout', 'specifically NOT close_timeout');
    is($obs{'/ws'}{code}, 1006, 'close_code is 1006');
    ok(!$obs{'/ws'}{complete}, 'on_complete did NOT fire');

    $park->done unless $park->is_ready;
    eval { $stream_io->close_now };
    $loop->remove($server);
};

# ============================================================
# 10. [PANEL] RST_STREAM(NO_ERROR) AFTER a valid peer Close -> NORMAL / clean
#     (the handshake completed), NOT transport-loss.
# ============================================================
subtest 'RST_STREAM(NO_ERROR) after a peer Close -> CLEAN (panel)' => sub {
    my %obs;
    my $park = $loop->new_future;
    my $app  = app_initiated_close(\%obs, $park, park => 1);
    my ($conn, $stream_io, $client_sock, $server) =
        create_h2_connection(app => $app, ws_close_timeout => 30);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    my $sid = open_ws_stream($client, $client_sock);
    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{closed_sent} }),
        'app sent its close and parked');

    # Peer answers with a valid Close frame (no END_STREAM) ...
    send_stream_data($client, $client_sock, $sid, client_close_frame(1000, 'bye'), 0);
    pump_rounds($client, $client_sock, 4);
    # ... then closes its half with RST_STREAM(NO_ERROR) instead of END_STREAM.
    send_rst_stream($client, $client_sock, $sid, 0);   # NO_ERROR

    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{complete} }),
        'on_complete fired -- the completed handshake is a clean end');
    is($obs{'/ws'}{code}, 1000, 'close_code is the peer code (1000)');
    is($obs{'/ws'}{complete_reason}, undef, 'disconnect_reason undef -- clean');
    ok(!$obs{'/ws'}{disconnect}, 'on_disconnect did NOT fire (NOT transport-loss)');

    $park->done unless $park->is_ready;
    eval { $stream_io->close_now };
    $loop->remove($server);
};

# ============================================================
# 11. bare RST_STREAM(NO_ERROR) with NO prior peer Close -> transport-loss /
#     1006 (the panel's negative case: a bare RST stays transport-loss, and it
#     is NOT the server_error the app-walked-away path reports).
# ============================================================
subtest 'app close, bare RST(NO_ERROR) no peer Close -> transport-loss/1006' => sub {
    my %obs;
    my $park = $loop->new_future;
    my $app  = app_initiated_close(\%obs, $park, park => 1);
    my ($conn, $stream_io, $client_sock, $server) =
        create_h2_connection(app => $app, ws_close_timeout => 30);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    my $sid = open_ws_stream($client, $client_sock);
    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{closed_sent} }),
        'app sent its close and parked');

    send_rst_stream($client, $client_sock, $sid, 0);   # NO_ERROR, no prior Close

    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{disconnect} }),
        'on_disconnect fired');
    isnt($obs{'/ws'}{disconnect_reason}, 'close_timeout', 'specifically NOT close_timeout');
    isnt($obs{'/ws'}{disconnect_reason}, 'server_error', 'specifically NOT server_error');
    is($obs{'/ws'}{code}, 1006, 'close_code is 1006 (no peer Close)');
    ok(!$obs{'/ws'}{complete}, 'on_complete did NOT fire');

    $park->done unless $park->is_ready;
    eval { $stream_io->close_now };
    $loop->remove($server);
};

# ============================================================
# 12. STREAM ISOLATION: two concurrent WebSocket streams. Closing one and
#     letting its deadline expire leaves the OTHER stream's scope and deadline
#     untouched -- it is still connected, and completes cleanly on its own peer
#     Close afterwards.
# ============================================================
subtest 'stream isolation: expiring one stream leaves the sibling untouched' => sub {
    my %obs;
    my $park = $loop->new_future;
    # /close closes and parks (its deadline will expire); /keep just parks
    # (never closes, stays connected).
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        my $path = $scope->{path} // '/ws';
        my $c    = $scope->{'pagi.connection'};
        $obs{$path}{conn} = $c;

        await $receive->();
        await $send->({ type => 'websocket.accept' });

        $c->on_complete(sub {
            $obs{$path}{complete}++;
            $obs{$path}{code} = $c->close_code;
        });
        $c->on_disconnect(sub {
            my ($r) = @_;
            $obs{$path}{disconnect}++;
            $obs{$path}{disconnect_reason} = $r;
            $obs{$path}{code} = $c->close_code;
        });
        $obs{$path}{connected} = $c->is_connected ? 1 : 0;

        if ($path eq '/close') {
            await $send->({ type => 'websocket.close', code => 1000, reason => 'bye' });
            $obs{$path}{closed_sent} = 1;
        }
        await $park;
        return;
    };

    my ($conn, $stream_io, $client_sock, $server) =
        create_h2_connection(app => $app, ws_close_timeout => 0.3);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    my $sid_close = open_ws_stream($client, $client_sock, '/close');
    my $sid_keep  = open_ws_stream($client, $client_sock, '/keep');

    ok(pump_until($client, $client_sock, sub { $obs{'/close'}{closed_sent} }),
        '/close sent its websocket.close');

    # /close's deadline expires -> close_timeout on THAT stream only.
    ok(pump_until($client, $client_sock, sub { $obs{'/close'}{disconnect} }, 80),
        '/close ended on its deadline');
    is($obs{'/close'}{disconnect_reason}, 'close_timeout', '/close ended close_timeout');
    is($obs{'/close'}{code}, 1006, '/close close_code 1006');

    # The sibling /keep is untouched: no terminal fired, still connected, and it
    # completes cleanly on its OWN peer Close afterwards.
    ok(!$obs{'/keep'}{disconnect}, '/keep did NOT disconnect when its sibling expired');
    ok(!$obs{'/keep'}{complete},   '/keep did NOT complete when its sibling expired');
    ok($obs{'/keep'}{conn}->is_connected, '/keep is still connected');

    send_stream_data($client, $client_sock, $sid_keep, client_close_frame(1000, 'bye'), 1);
    ok(pump_until($client, $client_sock, sub { $obs{'/keep'}{complete} }),
        '/keep completes cleanly on its own peer Close, independent of /close');
    is($obs{'/keep'}{code}, 1000, '/keep close_code is its own peer code');

    $park->done unless $park->is_ready;
    eval { $stream_io->close_now };
    $loop->remove($server);
};

# ============================================================
# 12b. FINISH-BOUND ISOLATION (WS-CLOSE-TRUTH-4): the finish bound is per stream.
#     One stream whose peer answered its Close but withheld its END_STREAM ends
#     close_incomplete when its finish bound expires; a sibling that is fully
#     connected is untouched -- no terminal, still connected, completes cleanly
#     on its OWN peer Close afterwards. (Mutating the arm/resolve to the
#     connection rather than the stream breaks the sibling here.)
# ============================================================
subtest 'finish-bound isolation: one stream close_incomplete leaves the sibling untouched' => sub {
    my %obs;
    my $park = $loop->new_future;
    # /close closes; its peer answers WITHOUT END_STREAM (finish bound expires).
    # /keep just parks (never closes, stays connected).
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        my $path = $scope->{path} // '/ws';
        my $c    = $scope->{'pagi.connection'};
        $obs{$path}{conn} = $c;

        await $receive->();
        await $send->({ type => 'websocket.accept' });

        $c->on_complete(sub {
            $obs{$path}{complete}++;
            $obs{$path}{code} = $c->close_code;
        });
        $c->on_disconnect(sub {
            my ($r) = @_;
            $obs{$path}{disconnect}++;
            $obs{$path}{disconnect_reason} = $r;
            $obs{$path}{code} = $c->close_code;
        });

        if ($path eq '/close') {
            await $send->({ type => 'websocket.close', code => 1000, reason => 'bye' });
            $obs{$path}{closed_sent} = 1;
        }
        await $park;
        return;
    };

    my ($conn, $stream_io, $client_sock, $server) =
        create_h2_connection(app => $app, ws_close_timeout => 0.5);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    my $sid_close = open_ws_stream($client, $client_sock, '/close');
    my $sid_keep  = open_ws_stream($client, $client_sock, '/keep');

    ok(pump_until($client, $client_sock, sub { $obs{'/close'}{closed_sent} }),
        '/close sent its websocket.close');

    # /close's peer answers with its Close (code 1001, DISTINCT from /close's own
    # 1000) but WITHHOLDS END_STREAM: its finish bound arms and, unanswered,
    # expires -> close_incomplete on THAT stream only, preserving the peer's code.
    send_stream_data($client, $client_sock, $sid_close, client_close_frame(1001, 'peerbye'), 0);
    ok(pump_until($client, $client_sock, sub { $obs{'/close'}{disconnect} }, 80),
        '/close ended on its finish bound');
    is($obs{'/close'}{disconnect_reason}, 'close_incomplete', '/close ended close_incomplete');
    is($obs{'/close'}{code}, 1001, '/close close_code is the peer code, preserved (WS-CLOSE-TRUTH-5)');

    # The sibling /keep is untouched: no terminal fired, still connected, and it
    # completes cleanly on its OWN peer Close afterwards.
    ok(!$obs{'/keep'}{disconnect}, '/keep did NOT disconnect when its sibling expired');
    ok(!$obs{'/keep'}{complete},   '/keep did NOT complete when its sibling expired');
    ok($obs{'/keep'}{conn}->is_connected, '/keep is still connected');

    send_stream_data($client, $client_sock, $sid_keep, client_close_frame(1000, 'bye'), 1);
    ok(pump_until($client, $client_sock, sub { $obs{'/keep'}{complete} }),
        '/keep completes cleanly on its own peer Close, independent of /close');
    is($obs{'/keep'}{code}, 1000, '/keep close_code is its own peer code');

    $park->done unless $park->is_ready;
    eval { $stream_io->close_now };
    $loop->remove($server);
};

# ============================================================
# 13. SHUTDOWN PREEMPTS the in-flight per-stream close-wait. A stream parked in
#     the app-initiated closing phase when the server begins its graceful drain
#     resolves with server_shutdown, disposes the per-stream close deadline, and
#     does NOT wait the full ws_close_timeout. ws_close_timeout is 30s (far
#     beyond any drain), so a resolution naming close_timeout, or one that
#     blocked on the deadline, would be the bug. Terminal shape: on_disconnect +
#     on_end (once) + end_future, NOT on_complete. The h2 twin of h1 subtest 10.
# ============================================================
subtest 'server shutdown preempts the in-flight per-stream close-wait -> server_shutdown' => sub {
    my %obs;
    my $park = $loop->new_future;
    my $app  = app_initiated_close(\%obs, $park, park => 1);
    my ($conn, $stream_io, $client_sock, $server) =
        create_h2_connection(app => $app, ws_close_timeout => 30);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    my $sid = open_ws_stream($client, $client_sock);
    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{closed_sent} }),
        'app sent its close and parked (peer silent)');

    # The manual harness builds the connection directly, so register it the way
    # the real accept path does (by refaddr) -- the drain iterates that registry.
    $server->{connections}{refaddr($conn)} = $conn;
    $server->{shutting_down} = 1;

    # Reach the pending stream and its armed per-stream deadline pre-drain.
    my $ss = $conn->{h2_streams}{$sid};
    ok($ss, 'the closing-phase stream is live pre-shutdown');
    my $deadline = $ss->{ws_close_deadline};
    ok($deadline && $deadline->is_running, 'the per-stream close deadline is armed and live');
    ok(!$obs{'/ws'}{disconnect}, 'still PENDING before the drain (no terminal yet)');

    # Begin the graceful drain. A ws-only connection has no requests in flight,
    # so the drain closes it at once with server_shutdown (sync terminal).
    $park->done unless $park->is_ready;
    eval { $server->_drain_connections->get };

    ok($obs{'/ws'}{disconnect}, 'the pending stream was resolved by the drain');
    is($obs{'/ws'}{disconnect}, 1, 'on_disconnect fired exactly once');
    is($obs{'/ws'}{disconnect_reason}, 'server_shutdown',
        'reason is server_shutdown -- the drain preempted the close-wait, NOT close_timeout');
    isnt($obs{'/ws'}{disconnect_reason}, 'close_timeout', 'specifically not close_timeout');
    is($obs{'/ws'}{code}, 1006, 'close_code 1006 (no peer Close)');
    is($obs{'/ws'}{end}, 1, 'on_end fired exactly once');
    ok($obs{'/ws'}{end_future}, 'end_future resolved');
    ok(!$obs{'/ws'}{complete}, 'on_complete did NOT fire');

    # The per-stream deadline was disposed on the terminal path: dequeued from
    # the stream (only _dispose_ws_close_deadline deletes it) and stopped.
    ok(!$ss->{ws_close_deadline}, 'the per-stream deadline was disposed (dequeued)');
    ok(!$deadline->is_running, 'the per-stream deadline timer was stopped (no late fire)');

    eval { $stream_io->close_now };
    $loop->remove($server);
};

# ============================================================
# 14. Observability: the h2 close_timeout resolution emits one structured warn
#     line (per stream) so an operator can watch the new abnormal-close
#     population. warn keeps a server at the default/quiet (error) threshold
#     silent; a server dropped to info routes it to its logger. The h2 twin of
#     h1 subtest 11.
# ============================================================
subtest 'h2 close_timeout resolution emits a structured observability log line' => sub {
    my %obs;
    my @logs;
    my $park = $loop->new_future;
    my $app  = app_initiated_close(\%obs, $park, park => 1);
    my $server = create_test_server(app => $app,
        log_level => 'info',
        logger    => sub { push @logs, $_[0]->{message} // '' });
    my ($conn, $stream_io, $client_sock) =
        create_h2_connection(app => $app, server => $server, ws_close_timeout => 0.3);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    open_ws_stream($client, $client_sock);
    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{closed_sent} }),
        'app sent its close and parked');

    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{disconnect} }, 80),
        'the deadline resolved close_timeout');
    is($obs{'/ws'}{disconnect_reason}, 'close_timeout', 'reason is close_timeout');

    my @hits = grep { /close_timeout/ && /close deadline/i } @logs;
    is(scalar(@hits), 1, 'exactly one structured close_timeout log line was emitted');
    like($hits[0], qr/1006/, 'the line records the abnormal close_code 1006');
    like($hits[0], qr/0\.3/, 'the line records the ws_close_timeout bound that elapsed');

    $park->done unless $park->is_ready;
    eval { $stream_io->close_now };
    $loop->remove($server);
};

done_testing;
