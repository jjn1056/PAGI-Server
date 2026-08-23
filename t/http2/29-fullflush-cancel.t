use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../../lib";
use Socket qw(AF_UNIX SOCK_STREAM);
use File::Temp qw(tempfile);

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
        # Threaded through (rather than left to Connection's own defaults)
        # so a server built with a small write_high_watermark/low_watermark
        # (Task 5's cancellation test) actually takes effect on the h2
        # per-stream send queue backpressure it exercises.
        write_high_watermark => $server->{write_high_watermark},
        write_low_watermark  => $server->{write_low_watermark},
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

# ============================================================
# Task 5: cancellation safety during h2 file transfers (design section 8.5)
# ============================================================
# Pins the observable contract for a client RST_STREAM mid-file-transfer:
# no "PAGI application error" log, no synthesized 500, the app's send
# Future ends quietly, and the server stays healthy (a fresh request on a
# new connection still works). Also specifically provokes the race carried
# from Task 2's review: _h2_on_close's drain-waiter release
# (_h2_resolve_stream_drain_waiters) and its h2_streams deletion are BOTH
# deferred via loop->later in FIFO order, so a producer resumed by the
# waiter release can observe the dying stream as still present in
# h2_streams (its post-await re-check in $emit_chunk passes) and call
# resume_stream/_h2_write_pending on a stream nghttp2 already closed.
#
# This file's provocation (poll h2_streams for a non-empty
# stream_drain_waiters right before the RST, so the reset is guaranteed to
# land inside the blocked await -- see $drain_blocked below) DOES reach
# that exact window every run. It turns out harmless: Net::HTTP2::nghttp2's
# resume_data() XS wrapper only croaks for nghttp2 return codes other than
# NGHTTP2_ERR_INVALID_ARGUMENT (nghttp2.xs, resume_data), and a resume on an
# already-RST'd/removed stream is precisely that code -- so the stale
# resume_stream/_h2_write_pending call silently no-ops instead of raising.
# No fix to Connection.pm was needed; this test pins the resulting
# behavior (and would catch a regression if a future nghttp2 binding
# upgrade changed that tolerance).

use constant H2_CANCEL_CODE => 8;   # RST_STREAM error code CANCEL (RFC 9113)

package Cancel;
our $RESULT;
our $CS;              # connection_state of the stream the client resets
our $COMPLETE = 0;    # flips true if on_complete ever fires (it must not)
our $DISCONNECT;      # reason string once on_disconnect fires
package main;

# Mirrors t/http2/28-file-fh.t's fixture shape: large enough (> 4x
# FILE_CHUNK_SIZE, 65536) that read_file_chunked makes several separate
# emit_chunk calls, so the second-and-later ones can be made to block on
# backpressure with a small enough write_high_watermark.
my $cancel_unit = join('', map { chr(32 + ($_ % 95)) } 0..96);
my $cancel_pattern = $cancel_unit x (int(300_000 / length($cancel_unit)) + 1);
$cancel_pattern = substr($cancel_pattern, 0, 300_000);
my ($cancel_fh, $cancel_file) = tempfile(UNLINK => 1);
binmode $cancel_fh;
print $cancel_fh $cancel_pattern;
close $cancel_fh;

# Mirrors Connection.pm's private $STREAM_GONE sentinel string exactly, so
# the app can tell the "quiet abort" path apart from a real error without
# reaching into server internals.
my $STREAM_GONE_TEXT = "PAGI::h2 stream gone\n";

my $slow_file_app = async sub {
    my ($scope, $receive, $send) = @_;
    die "unsupported scope" if $scope->{type} ne 'http';
    my $cs = $scope->{'pagi.connection'};
    $Cancel::CS = $cs;
    $cs->on_complete(sub { $Cancel::COMPLETE = 1 });
    $cs->on_disconnect(sub { $Cancel::DISCONNECT = $_[0] });
    while (1) {
        my $e = await $receive->();
        last if $e->{type} ne 'http.request' || !$e->{more};
    }
    await $send->({ type => 'http.response.start', status => 200,
                     headers => [['content-type','application/octet-stream']] });
    $Cancel::RESULT = do {
        local $@;
        eval { await $send->({ type => 'http.response.body', file => $cancel_file }) };
        $@ ? ($@ eq $STREAM_GONE_TEXT ? 'quiet' : "err:$@") : 'resolved';
    };
    return;
};

my $cancel_ok_app = async sub {
    my ($scope, $receive, $send) = @_;
    while (1) {
        my $e = await $receive->();
        last if $e->{type} ne 'http.request' || !$e->{more};
    }
    await $send->({ type => 'http.response.start', status => 200,
                     headers => [['content-type','text/plain']] });
    await $send->({ type => 'http.response.body', body => 'ok', more => 0 });
    return;
};

my @cancel_warnings;
subtest 'client RST while the file pump is blocked on drain is quiet and leak-free' => sub {
    local $SIG{__WARN__} = sub { push @cancel_warnings, $_[0] };

    # A tiny write_high_watermark: the first FILE_CHUNK_SIZE (65536-byte)
    # read fills h2's own per-stream flow-control window almost exactly, so
    # the SECOND chunk can never leave send_queue (no WINDOW_UPDATE will
    # ever raise the window -- see below), and the THIRD chunk's emit_chunk
    # call blocks reliably in _h2_wait_for_stream_drain.
    my $server = create_test_server(
        app                  => $slow_file_app,
        write_high_watermark => 100,
        write_low_watermark  => 50,
    );
    my ($conn, $stream_io, $client_sock) =
        create_h2c_connection(app => $slow_file_app, server => $server);

    my %headers;
    my $got_data = 0;
    my $client = create_client(
        on_header          => sub { my ($sid, $n, $v) = @_; $headers{$n} = $v; return 0 },
        on_data_chunk_recv => sub { $got_data = 1; return 0 },
    );
    h2c_handshake($client, $client_sock);

    my $stream_id = $client->submit_request(
        method    => 'GET',
        path      => '/slow-file',
        scheme    => 'http',
        authority => 'localhost',
        headers   => [],
    );
    $client_sock->syswrite($client->mem_send);

    # Drive the server only -- read and mem_recv whatever it sends (so we
    # notice the first DATA frame), but deliberately never mem_send/write
    # the client's own output back. That output would include the h2-level
    # auto WINDOW_UPDATE nghttp2 generates on receipt of DATA; withholding
    # it keeps the server's flow-control window permanently exhausted after
    # the first chunk, so the pump cannot self-unblock out from under us.
    # Poll the connection's own h2_streams state (white-box, but this IS a
    # race-window provocation test) so this loop stops exactly once the
    # blocked state is reached, rather than guessing an iteration count.
    my $drain_blocked = 0;
    for (1 .. 100) {
        $loop->loop_once(0.05);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        my $ss = $conn->{h2_streams}{$stream_id};
        if ($ss && $ss->{stream_drain_waiters} && @{$ss->{stream_drain_waiters}}) {
            $drain_blocked = 1;
            last;
        }
    }
    ok($got_data, 'client observed at least one DATA frame before the reset');
    ok($drain_blocked,
        'file pump is blocked in _h2_wait_for_stream_drain before the reset (race window open)')
        or diag('pump never reached the blocked state -- the race window was not provoked this run');

    # RST now, landing precisely inside the blocked emit_chunk await -- this
    # is the exact window the carried finding (Task 2 review) describes.
    $client->submit_rst_stream($stream_id, H2_CANCEL_CODE);
    $client_sock->syswrite($client->mem_send);

    # 0.5s of loop turns (brief's figure) for the server to process the
    # reset and settle, now driving both directions normally.
    for (1..20) {
        $loop->loop_once(0.025);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length($out);
    }

    # M8: after settling, the reset stream's h2_streams entry must have
    # been reclaimed -- a leaked entry here would accumulate across
    # repeated client resets.
    is( scalar keys %{ $conn->{h2_streams} }, 0, 'no leaked stream state' );

    $stream_io->close_now;
    $loop->remove($server);

    ok( !(grep { /PAGI application error/ } @cancel_warnings),
        'no app-error log on client reset' );
    like( $Cancel::RESULT, qr/\A(resolved|quiet)\z/,
        'file send ended quietly (no raised error)' );
    is( $headers{':status'}, 200,
        'original 200 response was not replaced with a synthesized 500' );

    # Design section 15.3: a cancel landing while the app is blocked on drain
    # is a client cancellation like any other -- the stream's own
    # connection_state must report it as such, and must never claim the
    # response completed.
    ok( $Cancel::CS, 'app captured a connection_state' );
    is( $Cancel::CS->is_connected, 0, 'connection_state is terminal after the reset' );
    is( $Cancel::CS->disconnect_reason, 'client_closed',
        "disconnect_reason is 'client_closed' after the mid-transfer reset" );
    is( $Cancel::DISCONNECT, 'client_closed', 'on_disconnect fired with client_closed' );
    is( $Cancel::COMPLETE, 0, 'on_complete never fired' );
};

# ============================================================
# Task 5: explicit post-RST no-op send -- distinct from the drain-blocked
# race pinned above (RST landing mid-transfer, inside a blocked
# emit_chunk await) and from t/http2/12's post-413-overrun case (RST is
# never involved there; the wake comes from the server's own overrun
# branch). Here the app is simply parked on receive() -- no response
# started yet -- when a plain client RST_STREAM(CANCEL) arrives; the app's
# SUBSEQUENT send() must resolve the Future successfully as a no-op (the
# h2_closed/absent-entry carve-out, same discipline as t/http2/12's
# post-413 case), not raise, and must never reach nghttp2's
# submit_response for this stream.
# ============================================================
subtest 'send() after a plain client RST_STREAM is a silent no-op (no submit_response reaches nghttp2)' => sub {
    my ($receive_resolved, $receive_event, $send_future_resolved, $send_error);
    my @rst_warnings;
    local $SIG{__WARN__} = sub { push @rst_warnings, $_[0] };

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        $receive_event = await $receive->();
        $receive_resolved = 1;
        eval {
            await $send->({
                type    => 'http.response.start',
                status  => 200,
                headers => [],
            });
            1;
        } or $send_error = $@;
        $send_future_resolved = 1;
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(app => $app);

    my $client = create_client();
    h2c_handshake($client, $client_sock);

    # Streaming body (no content-length header, body producer yields
    # nothing) so the app gets dispatched and parked on receive() before
    # any DATA/END_STREAM arrives -- same technique as t/http2/12's
    # post-413 subtest.
    my $stream_id = $client->submit_request(
        method    => 'GET',
        path      => '/plain-rst',
        scheme    => 'http',
        authority => 'localhost',
        headers   => [],
        body      => sub { return undef },
    );
    $client_sock->syswrite($client->mem_send);

    # Poll (bounded) until the app is dispatched and parked on receive().
    my $dispatched = 0;
    for (1..20) {
        $loop->loop_once(0.1);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length($out);
        my $ss = $conn->{h2_streams}{$stream_id};
        if ($ss && $ss->{body_pending} && !$ss->{body_pending}->is_ready) {
            $dispatched = 1;
            last;
        }
    }
    ok($dispatched, 'app dispatched and parked on receive() before the RST')
        or diag('app never reached the parked-receive state -- cannot exercise post-RST no-op send');

    # Spy on submit_response the same way t/http2/12's post-413 subtest
    # does: a local typeglob wrapper that calls through and records, scoped
    # to this block so it stops intercepting once we leave it.
    my $submit_response_calls = 0;
    {
        no warnings 'redefine';
        local *PAGI::Server::Protocol::HTTP2::Session::submit_response = sub {
            my ($session, $sid, %args) = @_;
            $submit_response_calls++ if $sid == $stream_id;
            return $session->{nghttp2}->submit_response($sid, %args);
        };

        # Plain RST_STREAM(CANCEL) -- not a 413 overrun, not a drain-block.
        $client->submit_rst_stream($stream_id, H2_CANCEL_CODE);
        $client_sock->syswrite($client->mem_send);

        # Bounded wait for both the parked receive() to resolve AND the
        # app's subsequent send() to settle.
        my $settled = 0;
        for (1..20) {
            $loop->loop_once(0.1);
            my $buf = '';
            $client_sock->sysread($buf, 16384);
            $client->mem_recv($buf) if length($buf);
            my $out = $client->mem_send;
            $client_sock->syswrite($out) if length($out);
            if ($receive_resolved && $send_future_resolved) {
                $settled = 1;
                last;
            }
        }
        ok($settled, 'parked receive() resolved and the post-RST send() settled within the bounded wait')
            or diag('receive_resolved=' . ($receive_resolved // 0)
                     . ' send_future_resolved=' . ($send_future_resolved // 0));
    }

    is($receive_event->{type}, 'http.disconnect', 'receive resolved to http.disconnect');
    ok($send_future_resolved, "the app's post-RST send() resolved instead of hanging");
    ok(!$send_error, "the app's post-RST send() resolved successfully (no-op), not a rejected Future")
        or diag("send_error=$send_error");
    is($submit_response_calls, 0,
        'submit_response never reached nghttp2 for this stream -- the post-RST send() was a pure no-op');
    ok(!(grep { /PAGI application error/ } @rst_warnings), 'no app-error log for the post-RST no-op send');

    $stream_io->close_now;
    $loop->remove($server);
};

# Server stays healthy: a fresh request on a brand-new h2 connection still
# gets served normally after the mid-transfer reset above.
is( get_h2('/ok', app => $cancel_ok_app)->{body}, 'ok',
    'server healthy after mid-transfer reset (fresh h2 connection)' );

done_testing;
