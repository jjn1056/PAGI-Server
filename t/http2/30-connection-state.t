use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../../lib";
use Socket qw(AF_UNIX SOCK_STREAM);
use Scalar::Util qw(refaddr);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.008+ required)');
}

# HTTP/2 counterpart to h1's connection-state wiring: each h2 stream's own
# PAGI::Server::ConnectionState must flip to a terminal state exactly once
# when its stream closes -- clean completion fires on_complete, an early
# client RST fires on_disconnect('client_closed') -- and independently of
# any other concurrently-open stream on the same h2 connection (spec section
# 9.1). Before this task, nothing ever called _mark_complete/_mark_disconnected
# on an h2 stream's connection_state, so is_connected stayed true forever.

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;

my $loop     = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# --- h2c test harness (lifted from t/http2/29-fullflush-cancel.t) -----------

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

    my $app    = $overrides{app} // sub { };
    my $server = $overrides{server} // create_test_server(app => $app);

    my $stream = IO::Async::Stream->new(
        read_handle  => $sock_a,
        write_handle => $sock_a,
        on_read      => sub { 0 },
    );

    my $conn = PAGI::Server::Connection->new(
        stream      => $stream,
        app         => $app,
        protocol    => $protocol,
        server      => $server,
        h2_protocol => $server->{http2_protocol},
        h2c_enabled => $server->{h2c_enabled},
        extensions  => $server->{extensions},
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
    $rounds //= 20;
    for (1..$rounds) {
        $loop->loop_once(0.025);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length($out);
    }
}

use constant H2_CANCEL_CODE => 8;   # RST_STREAM error code CANCEL (RFC 9113)

# =============================================================================
# Apps
# =============================================================================

package H2CS;
our $CLEAN_CS;
our $CLEAN_COMPLETE     = 0;
our $CLEAN_DISCONNECT;   # undef unless on_disconnect fires
our $CLEAN_RESPONSE_STARTED = 0;

our $RST_CS;
our $RST_COMPLETE = 0;
our $RST_DISCONNECT;
our $RST_HELD = 0;   # flipped once the app has started streaming and is holding

our $DUAL_CS_A;
our $DUAL_COMPLETE_A = 0;
our $DUAL_DISCONNECT_A;
our $DUAL_CS_B;
our $DUAL_COMPLETE_B = 0;
our $DUAL_DISCONNECT_B;
our $DUAL_B_HELD = 0;

package main;

# Clean, non-streaming GET: captures connection_state, registers both
# callbacks, returns a complete response.
my $clean_app = async sub {
    my ($scope, $receive, $send) = @_;
    my $cs = $scope->{'pagi.connection'};
    $H2CS::CLEAN_CS = $cs;
    $cs->on_complete(sub { $H2CS::CLEAN_COMPLETE = 1 });
    $cs->on_disconnect(sub { $H2CS::CLEAN_DISCONNECT = $_[0] });

    while (1) {
        my $e = await $receive->();
        last if $e->{type} ne 'http.request' || !$e->{more};
    }

    await $send->({ type => 'http.response.start', status => 200,
                     headers => [['content-type', 'text/plain']] });
    $H2CS::CLEAN_RESPONSE_STARTED = $cs->response_started;
    await $send->({ type => 'http.response.body', body => 'ok', more => 0 });
    return;
};

# Streaming response that starts, sends one chunk, then holds forever
# (awaits a Future that never resolves) so the test can RST the stream
# while it is still mid-response.
my $rst_app = async sub {
    my ($scope, $receive, $send) = @_;
    my $cs = $scope->{'pagi.connection'};
    $H2CS::RST_CS = $cs;
    $cs->on_complete(sub { $H2CS::RST_COMPLETE = 1 });
    $cs->on_disconnect(sub { $H2CS::RST_DISCONNECT = $_[0] });

    while (1) {
        my $e = await $receive->();
        last if $e->{type} ne 'http.request' || !$e->{more};
    }

    await $send->({ type => 'http.response.start', status => 200,
                     headers => [['content-type', 'text/plain']] });
    await $send->({ type => 'http.response.body', body => 'partial', more => 1 });
    $H2CS::RST_HELD = 1;
    await Future->new;   # never resolves -- held until the stream is reset
    return;
};

# Two-stream app: routes on path so one stream completes cleanly while the
# other is deliberately held mid-response, both on the SAME connection.
my $dual_app = async sub {
    my ($scope, $receive, $send) = @_;
    my $path = $scope->{path};
    my $cs   = $scope->{'pagi.connection'};

    while (1) {
        my $e = await $receive->();
        last if $e->{type} ne 'http.request' || !$e->{more};
    }

    if ($path eq '/dual-a') {
        $H2CS::DUAL_CS_A = $cs;
        $cs->on_complete(sub { $H2CS::DUAL_COMPLETE_A = 1 });
        $cs->on_disconnect(sub { $H2CS::DUAL_DISCONNECT_A = $_[0] });
        await $send->({ type => 'http.response.start', status => 200,
                         headers => [['content-type', 'text/plain']] });
        await $send->({ type => 'http.response.body', body => 'a-done', more => 0 });
        return;
    }
    else {
        $H2CS::DUAL_CS_B = $cs;
        $cs->on_complete(sub { $H2CS::DUAL_COMPLETE_B = 1 });
        $cs->on_disconnect(sub { $H2CS::DUAL_DISCONNECT_B = $_[0] });
        await $send->({ type => 'http.response.start', status => 200,
                         headers => [['content-type', 'text/plain']] });
        await $send->({ type => 'http.response.body', body => 'b-partial', more => 1 });
        $H2CS::DUAL_B_HELD = 1;
        await Future->new;   # held -- never completes in this test
        return;
    }
};

# =============================================================================
# Test 1: clean GET -- is_connected flips false via on_complete, not on_disconnect
# =============================================================================

subtest 'h2: clean GET drives connection_state to complete' => sub {
    my ($conn, $stream_io, $client_sock, $server) =
        create_h2c_connection(app => $clean_app);

    my %headers;
    my $client = create_client(
        on_header => sub { my ($sid, $n, $v) = @_; $headers{$n} = $v; return 0 },
    );
    h2c_handshake($client, $client_sock);

    $client->submit_request(
        method    => 'GET',
        path      => '/clean',
        scheme    => 'http',
        authority => 'localhost',
    );
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 20);

    is($headers{':status'}, '200', 'clean GET got 200');
    ok($H2CS::CLEAN_CS, 'app captured a connection_state');
    ok($H2CS::CLEAN_RESPONSE_STARTED, 'response_started true once http.response.start sent');
    is($H2CS::CLEAN_CS->is_connected, 0, 'is_connected false after clean completion');
    is($H2CS::CLEAN_CS->disconnect_reason, undef, 'disconnect_reason undef on clean completion');
    is($H2CS::CLEAN_COMPLETE, 1, 'on_complete fired');
    is($H2CS::CLEAN_DISCONNECT, undef, 'on_disconnect did NOT fire');

    $stream_io->close_now;
    $loop->remove($server);
};

# =============================================================================
# Test 2: client RST mid-stream -- is_connected flips false via on_disconnect
# =============================================================================

subtest 'h2: client RST_STREAM drives connection_state to client_closed' => sub {
    my ($conn, $stream_io, $client_sock, $server) =
        create_h2c_connection(app => $rst_app);

    my %headers;
    my $client = create_client(
        on_header => sub { my ($sid, $n, $v) = @_; $headers{$n} = $v; return 0 },
    );
    h2c_handshake($client, $client_sock);

    my $stream_id = $client->submit_request(
        method    => 'GET',
        path      => '/rst',
        scheme    => 'http',
        authority => 'localhost',
    );
    $client_sock->syswrite($client->mem_send);

    # Settle until the app has started streaming and is holding (bounded
    # loop_once polling on the app's own flag -- no sleeps of despair).
    for (1 .. 100) {
        $loop->loop_once(0.05);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        last if $H2CS::RST_HELD;
    }
    ok($H2CS::RST_HELD, 'app reached the held-mid-stream point before the reset');
    # Drain any server->client frames not yet flushed at the moment the app
    # flag flipped (the RST_HELD check above races the socket, not the app).
    exchange_frames($client, $client_sock, 5);
    is($headers{':status'}, '200', 'response.start was delivered before the reset');

    $client->submit_rst_stream($stream_id, H2_CANCEL_CODE);
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 20);

    ok($H2CS::RST_CS, 'app captured a connection_state');
    is($H2CS::RST_CS->is_connected, 0, 'is_connected false after client RST');
    is($H2CS::RST_CS->disconnect_reason, 'client_closed', "disconnect_reason is 'client_closed'");
    is($H2CS::RST_COMPLETE, 0, 'on_complete did NOT fire');
    ok(defined $H2CS::RST_DISCONNECT, 'on_disconnect fired');
    is($H2CS::RST_DISCONNECT, 'client_closed', 'on_disconnect reason is client_closed');

    $stream_io->close_now;
    $loop->remove($server);
};

# =============================================================================
# Test 3: two concurrent streams -- independent terminal states (spec 9.1)
# =============================================================================

subtest 'h2: two concurrent streams have independent connection_state' => sub {
    my ($conn, $stream_io, $client_sock, $server) =
        create_h2c_connection(app => $dual_app);

    my %headers_a;
    my $client = create_client(
        on_header => sub {
            my ($sid, $n, $v) = @_;
            $headers_a{$n} = $v if $sid == 1;   # first submitted stream
            return 0;
        },
    );
    h2c_handshake($client, $client_sock);

    # Stream A: completes cleanly.
    my $stream_id_a = $client->submit_request(
        method    => 'GET',
        path      => '/dual-a',
        scheme    => 'http',
        authority => 'localhost',
    );
    # Stream B: held mid-response.
    my $stream_id_b = $client->submit_request(
        method    => 'GET',
        path      => '/dual-b',
        scheme    => 'http',
        authority => 'localhost',
    );
    $client_sock->syswrite($client->mem_send);

    for (1 .. 100) {
        $loop->loop_once(0.05);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length($out);
        last if $H2CS::DUAL_COMPLETE_A && $H2CS::DUAL_B_HELD;
    }
    # Drain any server->client frames not yet flushed at the moment the app
    # flags flipped (the settle loop above races the socket, not the app).
    exchange_frames($client, $client_sock, 5);

    is($headers_a{':status'}, '200', 'stream A got 200');
    ok($H2CS::DUAL_CS_A, 'stream A captured a connection_state');
    ok($H2CS::DUAL_CS_B, 'stream B captured a connection_state');
    isnt(refaddr($H2CS::DUAL_CS_A), refaddr($H2CS::DUAL_CS_B),
        'the two streams have distinct connection_state objects');

    is($H2CS::DUAL_CS_A->is_connected, 0, 'stream A: is_connected false (completed)');
    is($H2CS::DUAL_COMPLETE_A, 1, 'stream A: on_complete fired');
    is($H2CS::DUAL_DISCONNECT_A, undef, 'stream A: on_disconnect did NOT fire');

    is($H2CS::DUAL_CS_B->is_connected, 1, 'stream B: still is_connected true (held, untouched)');
    is($H2CS::DUAL_COMPLETE_B, 0, 'stream B: on_complete did NOT fire');
    is($H2CS::DUAL_DISCONNECT_B, undef, 'stream B: on_disconnect did NOT fire');

    $stream_io->close_now;
    $loop->remove($server);
};

done_testing;
