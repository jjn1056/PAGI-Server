use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
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
# Test: HTTP/2 `http.fullflush` dispatch (and, appended by Task 5,
# `http.cancel`)
# ============================================================
# Phase 1 left http.fullflush as a stub on the h2 HTTP send path (it died
# unconditionally) and the h2 SSE send closure had no dispatch branch at all
# -- a validated+advanced fullflush fell into the closure's "Unrecognized
# event type" fallback (ledgered Phase-1 carry M4). Task 4 wires both
# closures to hand any pending frames to the session's write path (design
# section 8.4): resume_stream (if a stream is actively streaming) then
# _h2_write_pending. Extension gating itself (validate_http_send /
# validate_sse_send / advance_http / advance_sse) was already correct from
# Phase 1 -- this file only exercises the new dispatch behavior plus a
# regression check that the unadvertised gating croak still applies.

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;

my $loop = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# ============================================================
# Helpers (lifted verbatim from t/http2/28-file-fh.t, plus extensions
# passthrough into Connection->new so per-server extension gating can be
# exercised -- t/http2/26 and t/http2/28's copies of this helper never
# needed that).
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
    my $server = $overrides{server}
        // create_test_server(app => $app, extensions => $overrides{extensions} // {});

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
        extensions    => $server->{extensions},
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
# Request helper
# ============================================================

sub h2_request {
    my ($path, %opts) = @_;
    my ($conn, $stream_io, $client_sock, $server) =
        create_h2c_connection(app => $opts{app}, server => $opts{server},
            extensions => $opts{extensions});

    my %headers;
    my $body = '';
    my $client = create_client(
        on_header          => sub { my ($sid, $n, $v) = @_; $headers{$n} = $v; return 0 },
        on_data_chunk_recv => sub { my ($sid, $d)    = @_; $body .= $d;         return 0 },
    );
    h2c_handshake($client, $client_sock);
    $client->submit_request(
        method    => 'GET',
        path      => $path,
        scheme    => 'http',
        authority => 'localhost',
        headers   => $opts{headers} // [],
    );
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, $opts{rounds} // 20);

    $stream_io->close_now;
    $loop->remove($server);
    return { headers => \%headers, body => $body };
}

sub get_h2 {
    my ($path, %opts) = @_;
    return h2_request($path, %opts);
}

# ============================================================
# Apps
# ============================================================
#
# $Flush::HTTP_ERR / $Flush::SSE_ERR / $Flush::UNADV_ERR record what (if
# anything) the eval'd fullflush send raised, per the brief's assertions.

package Flush;
our $HTTP_ERR;
our $SSE_ERR;
our $UNADV_ERR;
package main;

my $http_flush_app = async sub {
    my ($scope, $receive, $send) = @_;
    die "unsupported scope" if $scope->{type} ne 'http';
    while (1) {
        my $e = await $receive->();
        last if $e->{type} ne 'http.request' || !$e->{more};
    }

    await $send->({ type => 'http.response.start', status => 200,
                     headers => [['content-type','text/plain']] });
    await $send->({ type => 'http.response.body', body => 'a', more => 1 });
    $Flush::HTTP_ERR = do {
        local $@;
        eval { await $send->({ type => 'http.fullflush' }) };
        $@;
    };
    await $send->({ type => 'http.response.body', body => 'b', more => 0 });
    return;
};

my $sse_flush_app = async sub {
    my ($scope, $receive, $send) = @_;
    die "unsupported scope" if $scope->{type} ne 'sse';
    await $receive->();

    await $send->({ type => 'sse.start', status => 200 });
    await $send->({ type => 'sse.send', data => 'x' });
    $Flush::SSE_ERR = do {
        local $@;
        eval { await $send->({ type => 'http.fullflush' }) };
        $@;
    };
    await $send->({ type => 'sse.close' });
    return;
};

my $unadvertised_flush_app = async sub {
    my ($scope, $receive, $send) = @_;
    die "unsupported scope" if $scope->{type} ne 'http';
    while (1) {
        my $e = await $receive->();
        last if $e->{type} ne 'http.request' || !$e->{more};
    }

    await $send->({ type => 'http.response.start', status => 200,
                     headers => [['content-type','text/plain']] });
    $Flush::UNADV_ERR = do {
        local $@;
        eval { await $send->({ type => 'http.fullflush' }) };
        $@;
    };
    await $send->({ type => 'http.response.body', body => 'done', more => 0 });
    return;
};

# ============================================================
# fullflush: dispatch on h2 HTTP and h2 SSE (advertised), gating regression
# on an unadvertised server. Each request gets its own server (matching
# t/http2/26 and t/http2/28's helper pattern) so the per-server extensions
# hash and the h2_request/close_now/loop->remove lifecycle stay independent
# per call.
# ============================================================

like( get_h2('/flush-http', app => $http_flush_app,
        extensions => { fullflush => {} })->{body},
    qr/\Aab\z/, 'fullflush mid-stream is transparent' );
ok( !$Flush::HTTP_ERR, 'advertised fullflush resolves on h2 http' );

get_h2('/flush-sse', app => $sse_flush_app,
    extensions => { fullflush => {} },
    headers    => [['accept', 'text/event-stream']] );
ok( !$Flush::SSE_ERR, 'advertised fullflush resolves on h2 sse' );

get_h2('/flush-unadvertised', app => $unadvertised_flush_app);
like( $Flush::UNADV_ERR, qr/Extension not enabled: fullflush/,
    'unadvertised still rejected' );

done_testing;
