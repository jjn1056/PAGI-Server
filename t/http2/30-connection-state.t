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

our $CANCEL_CS;
our $CANCEL_COMPLETE = 0;
our $CANCEL_DISCONNECT;
our $CANCEL_WAITING  = 0;   # app is parked in receive(), no response started
our $CANCEL_RETURNED = 0;   # app reached its return AFTER the reset
our $CANCEL_EVENT;          # event type receive() woke with

our $SWEEP_CS;
our $SWEEP_COMPLETE = 0;
our $SWEEP_DISCONNECT;
our $SWEEP_HELD = 0;

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

# Connection-teardown app: starts a response and holds, so the stream is
# still open (and its connection_state still live) when the whole connection
# is torn down under it.
my $sweep_app = async sub {
    my ($scope, $receive, $send) = @_;
    my $cs = $scope->{'pagi.connection'};
    $H2CS::SWEEP_CS = $cs;
    $cs->on_complete(sub { $H2CS::SWEEP_COMPLETE = 1 });
    $cs->on_disconnect(sub { $H2CS::SWEEP_DISCONNECT = $_[0] });

    while (1) {
        my $e = await $receive->();
        last if $e->{type} ne 'http.request' || !$e->{more};
    }

    await $send->({ type => 'http.response.start', status => 200,
                     headers => [['content-type', 'text/plain']] });
    await $send->({ type => 'http.response.body', body => 'partial', more => 1 });
    $H2CS::SWEEP_HELD = 1;
    await Future->new;   # held open until the connection is torn down
    return;
};

# Cancel-before-response-start app: parks in receive() (the client keeps the
# request stream open, so no body event is ever complete), then returns as
# soon as the reset wakes it -- so the app really does reach the dispatch
# wrapper with no response started, which is the only way to exercise the
# wrapper's client-already-gone carve-out.
my $cancel_before_start_app = async sub {
    my ($scope, $receive, $send) = @_;
    my $cs = $scope->{'pagi.connection'};
    $H2CS::CANCEL_CS = $cs;
    $cs->on_complete(sub { $H2CS::CANCEL_COMPLETE = 1 });
    $cs->on_disconnect(sub { $H2CS::CANCEL_DISCONNECT = $_[0] });

    $H2CS::CANCEL_WAITING = 1;
    my $e = await $receive->();
    $H2CS::CANCEL_EVENT    = $e->{type};
    $H2CS::CANCEL_RETURNED = 1;
    return;   # never started a response
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
# Test 3: client RST BEFORE the response started -- quiet cancellation
# =============================================================================
# Design section 15.3: "a cancelled stream causes neither a synthetic 500 nor
# an application-error log", and the spec's client-cancel carve-out covers a
# cancel that lands before the response ever starts. The app here reaches the
# dispatch wrapper (it returns once the reset wakes its receive), so the
# wrapper's no-response branch is genuinely reached and must stay silent.

subtest 'h2: client RST before response start synthesizes nothing and logs nothing' => sub {
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };

    my ($conn, $stream_io, $client_sock, $server) =
        create_h2c_connection(app => $cancel_before_start_app);

    my %headers;
    my $client = create_client(
        on_header => sub { my ($sid, $n, $v) = @_; $headers{$sid}{$n} = $v; return 0 },
    );
    h2c_handshake($client, $client_sock);

    # body => sub { undef } defers the request body forever: the request
    # stream stays open (no END_STREAM), so the app's receive() blocks.
    my $stream_id = $client->submit_request(
        method    => 'POST',
        path      => '/cancel-before-start',
        scheme    => 'http',
        authority => 'localhost',
        body      => sub { return undef },
    );
    $client_sock->syswrite($client->mem_send);

    for (1 .. 100) {
        $loop->loop_once(0.05);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        last if $H2CS::CANCEL_WAITING;
    }
    ok($H2CS::CANCEL_WAITING, 'app is parked in receive() with no response started');
    ok(!exists $headers{$stream_id}, 'no response headers sent before the reset');

    $client->submit_rst_stream($stream_id, H2_CANCEL_CODE);
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 20);

    ok($H2CS::CANCEL_RETURNED, 'app returned after the reset (dispatch wrapper reached)');
    is($H2CS::CANCEL_EVENT, 'http.disconnect', 'receive() woke with http.disconnect');

    ok(!exists $headers{$stream_id},
        'no synthetic 500 was sent for the cancelled stream');
    ok(!(grep { /PAGI application error/ } @warnings),
        'no application-error log for the cancelled stream')
        or diag("warnings: @warnings");
    ok(!(grep { /returned without starting a response/ } @warnings),
        'no no-response warning for the cancelled stream')
        or diag("warnings: @warnings");

    is($H2CS::CANCEL_CS->disconnect_reason, 'client_closed',
        "disconnect_reason is 'client_closed'");
    is($H2CS::CANCEL_COMPLETE, 0, 'on_complete did NOT fire');
    is($H2CS::CANCEL_DISCONNECT, 'client_closed', 'on_disconnect reason is client_closed');

    $stream_io->close_now;
    $loop->remove($server);
};

# =============================================================================
# Test 4: connection-level teardown sweeps every still-open stream
# =============================================================================
# A connection-level disconnect (server shutdown, socket error, timeout) never
# reaches _h2_on_close for streams that are still mid-response, so
# _handle_disconnect sweeps them itself. Without that sweep, an app holding a
# stream open when the connection dies would never learn why -- or that it
# died at all. Drives _handle_disconnect directly (white-box on the harness,
# t/http2/30 style) because the reason under test is one only the connection
# layer can produce.

subtest 'h2: connection teardown sweeps an open stream with the connection reason' => sub {
    my ($conn, $stream_io, $client_sock, $server) =
        create_h2c_connection(app => $sweep_app);

    my %headers;
    my $client = create_client(
        on_header => sub { my ($sid, $n, $v) = @_; $headers{$n} = $v; return 0 },
    );
    h2c_handshake($client, $client_sock);

    $client->submit_request(
        method    => 'GET',
        path      => '/sweep',
        scheme    => 'http',
        authority => 'localhost',
    );
    $client_sock->syswrite($client->mem_send);

    for (1 .. 100) {
        $loop->loop_once(0.05);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        last if $H2CS::SWEEP_HELD;
    }
    ok($H2CS::SWEEP_HELD, 'app is holding the stream open mid-response');
    ok($H2CS::SWEEP_CS->is_connected, 'the held stream is still connected before teardown');

    $conn->_handle_disconnect('server_shutdown');
    $loop->loop_once(0.05) for 1 .. 3;

    is($H2CS::SWEEP_CS->is_connected, 0, 'the held stream is disconnected by the teardown');
    is($H2CS::SWEEP_CS->disconnect_reason, 'server_shutdown',
        "the held stream inherits the connection's reason");
    is($H2CS::SWEEP_DISCONNECT, 'server_shutdown',
        'on_disconnect fired with the connection reason');
    is($H2CS::SWEEP_COMPLETE, 0, 'on_complete did NOT fire');

    $stream_io->close_now;
    $loop->remove($server);
};

# =============================================================================
# Test 5: two concurrent streams -- independent terminal states (spec 9.1)
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
