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
# Test: SSE Keepalive over HTTP/2
# ============================================================
# Verifies that sse.keepalive events start a periodic timer
# that sends SSE comments as HTTP/2 DATA frames (not chunked).

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;

my $loop = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# ============================================================
# Helpers
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
        stream            => $stream,
        app               => $app,
        protocol          => $protocol,
        server            => $server,
        h2_protocol       => $server->{http2_protocol},
        h2c_enabled       => $server->{h2c_enabled},
        sse_idle_timeout  => $server->{sse_idle_timeout} // 0,
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
# Keepalive comments arrive as SSE DATA frames
# ============================================================
subtest 'keepalive comments arrive over HTTP/2' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();

        await $send->({ type => 'sse.start', status => 200 });

        # Start keepalive with short interval
        await $send->({
            type     => 'sse.keepalive',
            interval => 0.2,
            comment  => 'ping',
        });

        # Send an initial event so we know data is flowing
        await $send->({ type => 'sse.send', data => 'start' });

        # Wait long enough for at least 2 keepalive ticks
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

    # Exchange frames until 'end' event arrives or wall-clock timeout
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
        or diag "response_body so far: $response_body";

    # Verify keepalive comments arrived
    like($response_body, qr/:ping\n/, 'Keepalive comment present in DATA frames');

    # Count keepalive comments (0.7s delay / 0.2s interval = ~3 expected)
    my @pings = ($response_body =~ /(:ping\n)/g);
    ok(scalar @pings >= 2, 'At least 2 keepalive comments received (got ' . scalar(@pings) . ')');

    # Verify data events also present
    like($response_body, qr/data: start\n/, 'Start event present');
    like($response_body, qr/data: end\n/, 'End event present');

    # Verify NO chunked encoding
    unlike($response_body, qr/^[0-9a-f]+\r\n/m, 'No chunked hex length prefixes');

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# Helpers for the per-stream keepalive/idle-timeout tests below
# ============================================================

sub open_sse_stream_tracked {
    my ($client, $client_sock, $path, $id_ref) = @_;

    $$id_ref = $client->submit_request(
        method    => 'GET',
        path      => $path,
        scheme    => 'http',
        authority => 'localhost',
        headers   => [['accept', 'text/event-stream']],
    );
    $client_sock->syswrite($client->mem_send);
    # 4 rounds is enough for the request/response headers round-trip
    # (measured ~0.1-0.2s wall-clock on a local socketpair) without the
    # default 10-round helper's trailing idle rounds each burning close to
    # their full 0.1s loop_once() timeout for nothing -- wasted setup time
    # that would otherwise eat directly into the tight keepalive/idle-timer
    # budgets the subtests below measure against.
    exchange_frames($client, $client_sock, 4);

    return $$id_ref;
}

# Count occurrences of an SSE comment line ":$tag\n" in accumulated raw body
# text (comments are formatted "$text\n\n", so each occurrence of ":$tag\n"
# is exactly one keepalive tick).
sub comment_count {
    my ($raw, $tag) = @_;
    return scalar(() = $raw =~ /:\Q$tag\E\n/g);
}

# ============================================================
# Per-stream keepalive isolation and stop (design section 11.3)
# ============================================================
# Before this task, sse_keepalive_writer/timer/comment lived on $self
# (the connection), so a second multiplexed h2 SSE stream's sse.start
# silently overwrote the first stream's keepalive writer -- the pattern
# design section 19.5 explicitly rejects. This drives two concurrent h2 SSE
# streams with different keepalive comments and intervals end-to-end:
# each must receive ONLY its own comment, at its own cadence, and closing
# one stream must release only that stream's timer (white-box), leaving a
# sibling stream's timer running.
sub make_two_stream_keepalive_app {
    return async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'sse';

        await $receive->();   # sse.request

        await $send->({ type => 'sse.start', status => 200 });

        if ($scope->{path} eq '/a') {
            await $send->({
                type     => 'sse.keepalive',
                interval => 0.2,
                comment  => 'pingA',
            });
            # ~3 ticks (0.2, 0.4, 0.6s) before this stream closes itself.
            await $loop->delay_future(after => 0.65);
            await $send->({ type => 'sse.close' });
        }
        elsif ($scope->{path} eq '/b') {
            await $send->({
                type     => 'sse.keepalive',
                interval => 0.5,
                comment  => 'pingB',
            });
            # Stays open well past A's 0.65s close (~3.8x longer) so the test
            # can observe B still ticking after A is gone.
            await $loop->delay_future(after => 2.5);
            await $send->({ type => 'sse.close' });
        }
    };
}

subtest 'two concurrent h2 SSE streams: independent keepalive cadence and per-stream stop' => sub {
    my $app = make_two_stream_keepalive_app();
    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(app => $app);

    my ($a_id, $b_id);
    my $a_data = '';
    my $b_data = '';
    my %closed;
    my $client = create_client(
        on_data_chunk_recv => sub {
            my ($sid, $data) = @_;
            if    (defined $a_id && $sid == $a_id) { $a_data .= $data; }
            elsif (defined $b_id && $sid == $b_id) { $b_data .= $data; }
            return 0;
        },
        on_stream_close => sub {
            my ($sid, $err) = @_;
            $closed{$sid} = 1;
            return 0;
        },
    );

    h2c_handshake($client, $client_sock);
    open_sse_stream_tracked($client, $client_sock, '/a', \$a_id);
    # Captured immediately after open (before A's own 0.65s close budget can
    # possibly have elapsed) and held onto for the rest of the subtest: once
    # captured, this hashref stays alive and inspectable even after
    # _h2_on_close later deletes the connection's OWN $stream_id entry --
    # the question under test is what happened to THIS stream's timer, not
    # whether the connection's lookup table still lists it (same technique
    # t/http2/31-ws-keepalive-disconnect.t uses for the WS twin).
    my $ss_a = $conn->{h2_streams}{$a_id};
    ok($ss_a, 'server holds per-stream state for A immediately after open');

    open_sse_stream_tracked($client, $client_sock, '/b', \$b_id);
    my $ss_b = $conn->{h2_streams}{$b_id};
    ok($ss_b, 'server holds per-stream state for B immediately after open');

    # Both keepalives are armed immediately after each stream's sse.start.
    # A ticks every 0.2s; expect >= 2 pingA comments by ~0.4s. Ceiling 3s is
    # a >= 7x margin over that expectation.
    my $deadline = Time::HiRes::time() + 3;
    while (Time::HiRes::time() < $deadline && comment_count($a_data, 'pingA') < 2) {
        exchange_frames($client, $client_sock, 1);
    }
    ok(comment_count($a_data, 'pingA') >= 2,
        'stream A received >= 2 of its own keepalive comments')
        or diag("a_data: $a_data");

    # Isolation: the bug this task fixes is the h2 SSE keepalive writer being
    # connection-level, so the second stream's sse.start silently replaced
    # the first stream's writer and one stream ended up carrying (some of)
    # the other's comments. Neither stream may EVER see the other's text.
    is(comment_count($a_data, 'pingB'), 0, 'stream A never received a pingB comment');
    is(comment_count($b_data, 'pingA'), 0, 'stream B never received a pingA comment');

    my $timer_a = $ss_a->{sse_ka_timer};
    ok($timer_a && $timer_a->is_running, "A's per-stream keepalive timer is armed");

    # A closes itself at ~0.65s (app-driven, see make_two_stream_keepalive_app).
    # Ceiling 3s (fresh deadline from here) is a wide margin over whatever
    # remains of that 0.65s budget.
    $deadline = Time::HiRes::time() + 3;
    while (Time::HiRes::time() < $deadline && !$closed{$a_id}) {
        exchange_frames($client, $client_sock, 1);
    }
    ok($closed{$a_id}, 'stream A closed within the bounded window');

    # White-box: A's own per-stream keepalive timer is released; B's timer
    # (a DIFFERENT $ss, on the same connection) is untouched and still
    # running. This is design section 11.3's "closing one stream removes
    # only its timers".
    ok(!$ss_a->{sse_ka_timer}, "A's sse_ka_timer field is released after A's close");
    ok($timer_a && !$timer_a->is_running, "A's armed timer object itself is stopped");
    ok($ss_b->{sse_ka_timer} && $ss_b->{sse_ka_timer}->is_running,
        "B's sse_ka_timer is still running after A closed");
    ok(!$closed{$b_id},
        'stream B has not closed yet (well before its own 2.5s close budget)');

    # B keeps ticking at its own 0.5s cadence after A is gone. Window is
    # 0.7s (>= 1 more tick expected); B's own close (2.5s total) is not due
    # for a long while yet, so this cannot race B's teardown.
    my $before_b = comment_count($b_data, 'pingB');
    my $settle_deadline = Time::HiRes::time() + 0.7;
    while (Time::HiRes::time() < $settle_deadline) {
        exchange_frames($client, $client_sock, 1);
    }
    ok(comment_count($b_data, 'pingB') > $before_b,
        "B's keepalive kept ticking after A's stream closed")
        or diag("b_data: $b_data");

    # Drain: let B's own app coroutine reach its 2.5s close and return
    # naturally before tearing down, so no suspended async sub is abandoned
    # mid-await. Ceiling 6s is a wide margin over B's own budget.
    $deadline = Time::HiRes::time() + 6;
    while (Time::HiRes::time() < $deadline && !$closed{$b_id}) {
        exchange_frames($client, $client_sock, 1);
    }
    ok($closed{$b_id}, "B's own close completed before teardown");

    $stream_io->close_now;
    $loop->remove($server);
};

# ============================================================
# Per-stream SSE idle timeout independence (design section 11.3)
# ============================================================
# Before this task, sse_idle_timer lived on $self (the connection): only
# the FIRST stream's sse.start actually armed it (a `return if
# $self->{sse_idle_timer}` guard on the second), and on expiry it called
# the connection-wide _handle_disconnect_and_close, tearing down every
# multiplexed stream -- not just the idle one. This proves one idle stream
# closes on its own while an active sibling (which keeps resetting its own
# idle timer via sse.send) is unaffected.
my $active_done = 0;   # flips true once the '/active' app coroutine returns

subtest 'per-stream SSE idle timeout: an idle stream closes without killing an active sibling' => sub {
    $active_done = 0;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'sse';

        await $receive->();
        await $send->({ type => 'sse.start', status => 200 });

        if ($scope->{path} eq '/idle') {
            # Sends nothing else -- only the per-stream idle timer (armed at
            # sse.start) can end this stream. Wait on receive() for the
            # eventual sse.disconnect it delivers, rather than a fixed
            # background delay that would outlive the test's own teardown
            # and get abandoned.
            await $receive->();
        }
        elsif ($scope->{path} eq '/active') {
            # First send immediately (no delay), so this stream already has
            # data well before the idle stream's independently-timed 0.3s
            # timeout can fire -- the two streams are dispatched moments
            # apart, so a delayed first send here would race that closure
            # instead of reliably preceding it. Then resets its own idle
            # timer on every further send (8 * 0.1s = 0.8s), comfortably
            # outliving the 0.3s idle timeout, but still short enough that
            # the coroutine returns (rather than being abandoned mid-await)
            # before this subtest's own teardown -- see $active_done below.
            await $send->({ type => 'sse.send', data => 'tick0' });
            for my $i (1 .. 8) {
                await $loop->delay_future(after => 0.1);
                await $send->({ type => 'sse.send', data => "tick$i" });
            }
            $active_done = 1;
        }
    };

    my $server = create_test_server(app => $app, sse_idle_timeout => 0.3);
    my ($conn, $stream_io, $client_sock) =
        create_h2c_connection(app => $app, server => $server);

    my ($idle_id, $active_id);
    my $active_data = '';
    my %closed;
    my $client = create_client(
        on_data_chunk_recv => sub {
            my ($sid, $data) = @_;
            $active_data .= $data if defined $active_id && $sid == $active_id;
            return 0;
        },
        on_stream_close => sub {
            my ($sid, $err) = @_;
            $closed{$sid} = 1;
            return 0;
        },
    );

    h2c_handshake($client, $client_sock);
    open_sse_stream_tracked($client, $client_sock, '/idle', \$idle_id);
    open_sse_stream_tracked($client, $client_sock, '/active', \$active_id);

    # Idle timeout is 0.3s and the idle stream sends nothing after
    # sse.start, so it should close at ~0.3s. Ceiling 5s is a >= 16x margin.
    my $deadline = Time::HiRes::time() + 5;
    while (Time::HiRes::time() < $deadline && !$closed{$idle_id}) {
        exchange_frames($client, $client_sock, 1);
    }
    ok($closed{$idle_id}, 'idle stream closed within the bounded window');

    # Independence: the active stream (resetting its OWN idle timer every
    # 0.1s) must not have been swept away by the idle stream's timeout --
    # that whole-connection blast radius is exactly the pre-fix behavior.
    ok(!$closed{$active_id}, 'active stream is still open when the idle stream closes');
    ok(length($active_data) > 0, 'active stream had already received data');

    # The active stream keeps receiving further data afterward too, proving
    # the connection (and the active stream's own scope) survived the idle
    # stream's teardown, not just the instant of closure. 0.3s is short
    # enough to land mid-loop (active's total budget is 0.8s from its own
    # dispatch, which starts only slightly after idle's).
    my $before = length($active_data);
    my $settle_deadline = Time::HiRes::time() + 0.3;
    while (Time::HiRes::time() < $settle_deadline) {
        exchange_frames($client, $client_sock, 1);
    }
    ok(length($active_data) > $before,
        'active stream continued receiving data after the sibling idle-timeout');

    # Drain: let the active app coroutine's bounded loop finish and return
    # naturally before tearing down, so no suspended async sub is abandoned
    # mid-await. Ceiling 5s is a wide margin over its ~0.8s own budget.
    my $drain_deadline = Time::HiRes::time() + 5;
    while (Time::HiRes::time() < $drain_deadline && !$active_done) {
        exchange_frames($client, $client_sock, 1);
    }
    ok($active_done, "active stream's app coroutine returned before teardown");

    $stream_io->close_now;
    $loop->remove($server);
};

done_testing;
