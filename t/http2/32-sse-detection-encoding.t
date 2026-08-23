use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../../lib";
use Socket qw(AF_UNIX SOCK_STREAM);
use Time::HiRes ();

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.008+ required)');
}

# ============================================================
# Test: SSE UTF-8 wire encoding over HTTP/2 (design section 11.2)
# ============================================================
# PAGI Www.pod "Send SSE": String fields (data, event, id, comment) MUST be
# UTF-8 encoded by the server before transmission. Verifies the h2 SSE send
# path (data/event/id/comment, and the keepalive writer) encodes exactly
# once at the wire boundary, and that an unencodable string fails the send
# Future without disturbing the stream.

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;

my $loop = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# ============================================================
# Helpers (same pattern as t/http2/14-sse-events.t / 15-sse-keepalive.t)
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

sub create_h2c_connection {
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
        h2c_enabled   => $server->{h2c_enabled},
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

sub h2c_handshake {
    my ($client, $client_sock) = @_;
    $client->send_connection_preface;
    my $data = $client->mem_send;
    $client_sock->syswrite($data);
    for (1..5) {
        $loop->loop_once(0.1);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length($out);
    }
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

# ============================================================
# data/event/id/comment: UTF-8 octets on the wire (DATA frames)
# ============================================================
subtest 'SSE UTF-8 wire encoding: data/event/id/comment arrive as UTF-8 octets' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();

        await $send->({ type => 'sse.start', status => 200 });

        await $send->({
            type  => 'sse.send',
            event => "\x{e9}vent",
            data  => "caf\x{e9}",
            id    => "\x{e9}d",
        });

        await $send->({ type => 'sse.comment', comment => "r\x{e9}sum\x{e9}" });
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(app => $app);

    my $response_body = '';
    my $client = create_client(
        on_data_chunk_recv => sub {
            my ($sid, $data) = @_;
            $response_body .= $data;
            return 0;
        },
    );

    h2c_handshake($client, $client_sock);

    $client->submit_request(
        method    => 'GET',
        path      => '/events',
        scheme    => 'http',
        authority => 'localhost',
        headers   => [['accept', 'text/event-stream']],
    );
    $client_sock->syswrite($client->mem_send);

    exchange_frames($client, $client_sock, 20);

    # U+00E9 (e-acute) is UTF-8 octets \xc3\xa9. Assert the exact octets on
    # the wire, not the character.
    like($response_body, qr/event: \xc3\xa9vent\n/, 'event field UTF-8 octets on the wire');
    like($response_body, qr/data: caf\xc3\xa9\n/,   'data field UTF-8 octets on the wire');
    like($response_body, qr/id: \xc3\xa9d\n/,       'id field UTF-8 octets on the wire');
    like($response_body, qr/:r\xc3\xa9sum\xc3\xa9\n\n/, 'comment field UTF-8 octets on the wire');

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# Invalid string: send Future fails; stream stays usable
# ============================================================
subtest 'SSE UTF-8 wire encoding: invalid string fails the send Future; stream stays usable' => sub {
    my $invalid_err;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();

        await $send->({ type => 'sse.start', status => 200 });

        # A lone UTF-16 surrogate has no UTF-8 representation.
        eval { await $send->({ type => 'sse.send', data => "bad \x{D800}" }) };
        $invalid_err = $@;

        await $send->({ type => 'sse.send', event => 'after', data => 'still-fine' });
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(app => $app);

    my $response_body = '';
    my $client = create_client(
        on_data_chunk_recv => sub {
            my ($sid, $data) = @_;
            $response_body .= $data;
            return 0;
        },
    );

    h2c_handshake($client, $client_sock);

    $client->submit_request(
        method    => 'GET',
        path      => '/events',
        scheme    => 'http',
        authority => 'localhost',
        headers   => [['accept', 'text/event-stream']],
    );
    $client_sock->syswrite($client->mem_send);

    exchange_frames($client, $client_sock, 20);

    like($invalid_err, qr/not encodable as UTF-8/i, 'invalid surrogate send fails the send Future');
    unlike($response_body, qr/bad/, 'invalid payload never reached the wire');
    like($response_body, qr/event: after\ndata: still-fine\n\n/,
        'stream stayed usable: subsequent valid send arrived on the wire');

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# Keepalive comment: UTF-8 octets on the wire (DATA frames)
# ============================================================
subtest 'SSE UTF-8 wire encoding: keepalive comment arrives as UTF-8 octets' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();

        await $send->({ type => 'sse.start', status => 200 });

        await $send->({
            type     => 'sse.keepalive',
            interval => 0.2,
            comment  => "p\x{e9}ng",
        });

        await $send->({ type => 'sse.send', data => 'start' });

        my $delay_f = $loop->delay_future(after => 0.7);
        await $delay_f;

        await $send->({ type => 'sse.send', data => 'end' });
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(app => $app);

    my $response_body = '';
    my $client = create_client(
        on_data_chunk_recv => sub {
            my ($sid, $data) = @_;
            $response_body .= $data;
            return 0;
        },
    );

    h2c_handshake($client, $client_sock);

    $client->submit_request(
        method    => 'GET',
        path      => '/events',
        scheme    => 'http',
        authority => 'localhost',
        headers   => [['accept', 'text/event-stream']],
    );
    $client_sock->syswrite($client->mem_send);

    my $timed_out = 1;
    my $deadline = Time::HiRes::time() + 5;
    while (Time::HiRes::time() < $deadline) {
        if ($response_body =~ /data: end\n/) {
            $timed_out = 0;
            last;
        }
        exchange_frames($client, $client_sock, 5);
    }
    ok(!$timed_out, 'end event arrived before 5s deadline')
        or diag "response_body so far: " . unpack('H*', $response_body);

    like($response_body, qr/:p\xc3\xa9ng\n\n/, 'keepalive comment UTF-8 octets on the wire');
    my @pings = ($response_body =~ /(:p\xc3\xa9ng\n\n)/g);
    ok(scalar @pings >= 2, 'at least 2 keepalive comments received (got ' . scalar(@pings) . ')');

    $stream_io->close_now;
    $loop->remove($server);
};

done_testing;
