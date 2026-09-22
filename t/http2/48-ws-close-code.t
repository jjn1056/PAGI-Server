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
# Test: close_code() / close_reason() expose the PEER's WebSocket Close on
# HTTP/2, populated before the terminal callback (Www.pod "Connection State"
# and "State Transition Order").
#
# Each app reads close_code/close_reason INSIDE its on_complete / on_disconnect
# callback, so the assertions pin F4: every terminal fact is populated before
# the callback runs. A populate-after-mark ordering would make the callback see
# undef (see the F4 mutation in the task report).
#
# Cases:
#   1. Peer Close(1000,'bye')+END_STREAM -> close_code 1000, reason 'bye',
#      read in on_complete (a completed handshake is a clean end).
#   2. Peer Close with no code (empty payload)+END_STREAM -> 1005 / undef.
#   3. Bare END_STREAM, no Close -> on_disconnect client_closed, 1006 / undef.
#   4. Server-initiated close (app sends websocket.close 1011), peer sends a
#      bare END_STREAM without its own Close -> close_code 1006, NOT 1011.
# ============================================================

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;
use Protocol::WebSocket::Frame;

my $loop = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# ============================================================
# Harness (lifted from t/http2/47-ws-parked-terminal-notification.t)
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

# A Close frame carrying no status code at all (empty payload).
sub client_close_frame_empty {
    return Protocol::WebSocket::Frame->new(
        type   => 'close',
        buffer => '',
        masked => 1,
    )->to_bytes;
}

# A parked accepted-WebSocket app recording close_code/close_reason at the
# instant each terminal callback fires. $pre_park, if given, runs after the
# callbacks are registered and before the app parks.
sub parked_ws_app {
    my ($obs, $park, %opt) = @_;
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
            $obs->{$path}{code}   = $c->close_code;
            $obs->{$path}{reason} = $c->close_reason;
        });
        $c->on_disconnect(sub {
            my ($reason) = @_;
            $obs->{$path}{disconnect}++;
            $obs->{$path}{disconnect_reason} = $reason;
            $obs->{$path}{code}   = $c->close_code;
            $obs->{$path}{reason} = $c->close_reason;
        });

        if ($opt{pre_park}) {
            await $opt{pre_park}->($send);
        }

        $obs->{$path}{parked} = 1;
        await $park;                               # never resolves while observed
        $obs->{$path}{returned} = 1;
        return;
    };
}

subtest 'a websocket refusal preserves 1006 before its clean completion' => sub {
    my %seen = (complete => 0, end => 0, disconnect => 0);
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        my $conn = $scope->{'pagi.connection'};
        await $receive->();
        my $snapshot = sub {
            return [ $conn->close_code, $conn->close_reason,
                $conn->response_complete ? 1 : 0, $conn->is_connected ? 1 : 0,
                $conn->disconnect_reason, $conn->disconnect_detail ];
        };
        $conn->on_complete(sub { ++$seen{complete}; $seen{at_complete} = $snapshot->() });
        $conn->on_end(sub { ++$seen{end}; $seen{at_end} = $snapshot->() });
        $conn->on_disconnect(sub { ++$seen{disconnect} });
        my $end = $conn->end_future;
        await $send->({ type => 'http.response.start', status => 403, headers => [] });
        $seen{before_body} = $conn->close_code;
        await $send->({ type => 'http.response.body', body => 'Access denied' });
        $seen{end_value} = await $end;
        $seen{after_end} = $snapshot->();
        $seen{receive_type} = (await $receive->())->{type};
        $seen{returned} = 1;
    };
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my ($status, $body, $sid) = ('', '');
    my %server_end_stream;
    require Net::HTTP2::nghttp2::Session;
    my $client = Net::HTTP2::nghttp2::Session->new_client(callbacks => {
        on_begin_headers => sub { 0 },
        on_header => sub { my (undef, $name, $value) = @_; $status = $value if $name eq ':status'; 0 },
        on_frame_recv => sub {
            my ($frame) = @_;
            $server_end_stream{$frame->{stream_id}} = 1
                if $frame->{type} == 0 && ($frame->{flags} & 0x1);
            0
        },
        on_data_chunk_recv => sub { my (undef, $data) = @_; $body .= $data; 0 },
        on_stream_close => sub { 0 },
    });
    complete_h2_handshake($client, $client_sock);
    $sid = open_ws_stream($client, $client_sock);
    ok(pump_until($client, $client_sock, sub {
        $seen{returned} && $seen{complete} && $seen{end} && $server_end_stream{$sid}
    }), 'refusing application, callbacks, and server END_STREAM completed');
    is($status, '403', 'refusal returned HTTP 403');
    is($body, 'Access denied', 'refusal returned exactly the HTTP body');
    ok($server_end_stream{$sid}, 'the response DATA frame carried server END_STREAM');
    is($seen{before_body}, undef, 'no close metadata before refusal completion');
    is($seen{at_complete}, [1006, undef, 1, 0, undef, undef], 'complete sees terminal facts');
    is($seen{at_end}, [1006, undef, 1, 0, undef, undef], 'end sees terminal facts');
    is($seen{after_end}, [1006, undef, 1, 0, undef, undef], 'end future sees terminal facts');
    is([@seen{qw(complete end disconnect)}], [1, 1, 0], 'clean callback families');
    is($seen{end_value}, undef, 'successful end future');
    is($seen{receive_type}, 'http.disconnect', 'no synthetic WebSocket disconnect');
    eval { $stream_io->close_now };
    $loop->remove($server);
};

# ============================================================
# 1. Peer Close(1000,'bye')+END_STREAM -> on_complete, close_code/reason set.
# ============================================================
subtest 'peer Close(1000,"bye") -> close_code 1000, close_reason "bye"' => sub {
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
        'on_complete fired');
    is($obs{'/ws'}{code},   1000,  'close_code is the peer code read inside on_complete');
    is($obs{'/ws'}{reason}, 'bye', 'close_reason is the peer text read inside on_complete');
    ok(!$obs{'/ws'}{disconnect}, 'on_disconnect did NOT fire (clean end)');

    eval { $stream_io->close_now };
    $loop->remove($server);
};

# ============================================================
# 2. Peer Close with no code (empty payload)+END_STREAM -> 1005 / undef.
# ============================================================
subtest 'peer Close with no code -> close_code 1005, close_reason undef' => sub {
    my %obs;
    my $park = $loop->new_future;
    my $app  = parked_ws_app(\%obs, $park);
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    my $sid = open_ws_stream($client, $client_sock);
    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{parked} }),
        'app accepted and parked');

    send_stream_data($client, $client_sock, $sid, client_close_frame_empty(), 1);

    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{complete} }),
        'on_complete fired');
    is($obs{'/ws'}{code},   1005,  'close_code is 1005 for a codeless peer Close');
    is($obs{'/ws'}{reason}, undef, 'close_reason is undef for a codeless peer Close');

    eval { $stream_io->close_now };
    $loop->remove($server);
};

# ============================================================
# 3. Bare END_STREAM, no Close -> on_disconnect client_closed, 1006 / undef.
# ============================================================
subtest 'bare END_STREAM -> on_disconnect client_closed, close_code 1006' => sub {
    my %obs;
    my $park = $loop->new_future;
    my $app  = parked_ws_app(\%obs, $park);
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    my $sid = open_ws_stream($client, $client_sock);
    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{parked} }),
        'app accepted and parked');

    send_stream_data($client, $client_sock, $sid, '', 1);   # no Close frame

    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{disconnect} }),
        'on_disconnect fired');
    is($obs{'/ws'}{disconnect_reason}, 'client_closed',
        "on_disconnect carried 'client_closed'");
    is($obs{'/ws'}{code},   1006,  'close_code is 1006 for a bare END_STREAM (no Close)');
    is($obs{'/ws'}{reason}, undef, 'close_reason is undef for a bare END_STREAM');
    ok(!$obs{'/ws'}{complete}, 'on_complete did NOT fire (abnormal end)');

    eval { $stream_io->close_now };
    $loop->remove($server);
};

# ============================================================
# 4. Server-initiated close, peer sends a bare END_STREAM without its own Close
#    -> close_code 1006, NOT the server's 1011.
# ============================================================
subtest 'server-initiated close, peer silent -> close_code 1006, not the server code' => sub {
    my %obs;
    my $park = $loop->new_future;
    my $app  = parked_ws_app(\%obs, $park,
        pre_park => sub {
            my ($send) = @_;
            return $send->({ type => 'websocket.close', code => 1011, reason => 'server closing' });
        },
    );
    my ($conn, $stream_io, $client_sock, $server) = create_h2_connection(app => $app);
    my $client = create_client();

    complete_h2_handshake($client, $client_sock);
    my $sid = open_ws_stream($client, $client_sock);
    ok(pump_until($client, $client_sock, sub { $obs{'/ws'}{parked} }),
        'app accepted, sent its close, and parked');

    # The peer never sends its own Close; it just ends its half of the stream.
    send_stream_data($client, $client_sock, $sid, '', 1);

    ok(pump_until($client, $client_sock,
            sub { $obs{'/ws'}{complete} || $obs{'/ws'}{disconnect} }),
        'a terminal callback fired');
    is($obs{'/ws'}{code},   1006,  'close_code is 1006 -- the peer never replied, never the server 1011');
    is($obs{'/ws'}{reason}, undef, 'close_reason is undef for a peer that never replied');

    eval { $stream_io->close_now };
    $loop->remove($server);
};

done_testing;
