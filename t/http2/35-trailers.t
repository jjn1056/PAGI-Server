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
use File::Temp qw(tempfile);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.009+ required)');
}

# ============================================================
# Test: HTTP/2 trailers output (design section 8.3)
# ============================================================
# "a terminal body event does not end the HTTP/2 stream. A following
# http.response.trailers submits trailing HEADERS with END_STREAM and
# marks the response terminal. Without the declaration, the trailers
# event fails. On HEAD, it is validated and discarded while still
# completing the response."
#
# On the wire: the final DATA frame carries no END_STREAM, a subsequent
# HEADERS frame (NGHTTP2_HCAT_HEADERS) carries END_STREAM and the trailer
# fields in order with duplicates preserved, and the stream closes with
# error code 0.

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;
use Net::HTTP2::nghttp2 qw(:header_categories NGHTTP2_FLAG_END_STREAM);

my $loop = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

use constant H2_INTERNAL_ERROR_CODE => 2;   # NGHTTP2_INTERNAL_ERROR (RFC 9113 section 7)
use constant H2_CANCEL_CODE         => 8;   # NGHTTP2_CANCEL

# ============================================================
# Helpers (lifted verbatim from t/http2/29-fullflush-cancel.t /
# t/http2/28-file-fh.t)
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

# Client with per-headers-block tracking: on_begin_headers resets the
# in-flight accumulator, on_header appends to it (ordered, duplicates
# preserved -- a plain hash would collide on repeated names like
# set-cookie), and on_frame_recv snapshots the completed block (with its
# frame flags/category) once the HEADERS frame is fully received. DATA
# frames are recorded the same way so the final one's END_STREAM bit can
# be inspected.
sub create_tracking_client {
    my (%overrides) = @_;
    require Net::HTTP2::nghttp2::Session;

    my @header_blocks;   # { category => ..., flags => ..., pairs => [...] }
    my @data_frames;     # { flags => ..., length => ... }
    my @current;

    my $client = Net::HTTP2::nghttp2::Session->new_client(
        callbacks => {
            on_begin_headers => sub { @current = (); return 0 },
            on_header        => sub {
                my ($sid, $name, $value) = @_;
                push @current, [$name, $value];
                return 0;
            },
            on_frame_recv => sub {
                my ($frame) = @_;
                if ($frame->{type} == 1) {   # HEADERS
                    push @header_blocks, {
                        category => $frame->{headers_category},
                        flags    => $frame->{flags},
                        pairs    => [@current],
                    };
                }
                elsif ($frame->{type} == 0) {   # DATA
                    push @data_frames, {
                        flags  => $frame->{flags},
                        length => $frame->{length},
                    };
                }
                $overrides{on_frame_recv}->($frame) if $overrides{on_frame_recv};
                return 0;
            },
            on_data_chunk_recv => $overrides{on_data_chunk_recv} // sub { 0 },
            on_stream_close    => $overrides{on_stream_close}    // sub { 0 },
        },
    );

    return ($client, \@header_blocks, \@data_frames);
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
        $loop->loop_once(0.05);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length($out);
    }
}

# ============================================================
# Step 1 (RED, then GREEN): acceptance wire shape
# ============================================================
# start(trailers=>1) -> streamed body (more=>1, then terminal) ->
# http.response.trailers with a header list carrying a duplicate name
# (set-cookie x2) to pin ordering + duplicate preservation.

my ($accept_cs, $accept_complete, $accept_disconnect);
my $accept_app = async sub {
    my ($scope, $receive, $send) = @_;
    die "unsupported scope" if $scope->{type} ne 'http';
    my $cs = $scope->{'pagi.connection'};
    $accept_cs = $cs;
    $cs->on_complete(sub { $accept_complete = 1 });
    $cs->on_disconnect(sub { $accept_disconnect = $_[0] });
    while (1) {
        my $e = await $receive->();
        last if $e->{type} ne 'http.request' || !$e->{more};
    }
    await $send->({ type => 'http.response.start', status => 200, trailers => 1,
                     headers => [['content-type', 'text/plain']] });
    await $send->({ type => 'http.response.body', body => 'chunk-one', more => 1 });
    await $send->({ type => 'http.response.body', body => 'chunk-two', more => 0 });
    await $send->({ type => 'http.response.trailers',
                     headers => [['x-checksum', 'abc'], ['set-cookie', 'a=1'], ['set-cookie', 'b=2']] });
    return;
};

subtest 'acceptance: streamed body + trailers wire shape' => sub {
    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(app => $accept_app);

    my %closed;
    my ($client, $header_blocks, $data_frames) = create_tracking_client(
        on_stream_close => sub { my ($sid, $code) = @_; $closed{$sid} = $code; return 0 },
    );
    h2c_handshake($client, $client_sock);

    my $stream_id = $client->submit_request(
        method    => 'GET',
        path      => '/trailers',
        scheme    => 'http',
        authority => 'localhost',
        headers   => [],
    );
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 30);

    $stream_io->close_now;
    $loop->remove($server);

    ok(@$data_frames >= 1, 'at least one DATA frame observed');
    my $last_data = $data_frames->[-1];
    ok(!($last_data->{flags} & NGHTTP2_FLAG_END_STREAM),
        'final DATA frame does NOT carry END_STREAM');

    my @trailer_blocks = grep { $_->{category} == NGHTTP2_HCAT_HEADERS() } @$header_blocks;
    is(scalar(@trailer_blocks), 1, 'exactly one trailing HEADERS block (NGHTTP2_HCAT_HEADERS) received');
    my $trailer = $trailer_blocks[0];
    ok(($trailer->{flags} & NGHTTP2_FLAG_END_STREAM), 'trailing HEADERS block carries END_STREAM');
    is($trailer->{pairs},
        [['x-checksum', 'abc'], ['set-cookie', 'a=1'], ['set-cookie', 'b=2']],
        'trailer fields arrive in order, duplicates preserved');

    is($closed{$stream_id}, 0, 'stream closes cleanly (error code 0)');
    ok($accept_complete, 'on_complete fired');
    is($accept_disconnect, undef, 'on_disconnect did not fire');
};

# ============================================================
# Empty body + empty trailers
# ============================================================
# start(trailers=>1) -> terminal EMPTY body -> EMPTY trailers. The
# single-shot terminal body must still route through the streaming path
# (the step-6 CRITICAL DESIGN POINT) even though there is no payload to
# push, and an empty/absent trailers 'headers' list still submits a
# (terminal) trailing HEADERS block per the Interfaces contract.

my $empty_app = async sub {
    my ($scope, $receive, $send) = @_;
    while (1) {
        my $e = await $receive->();
        last if $e->{type} ne 'http.request' || !$e->{more};
    }
    await $send->({ type => 'http.response.start', status => 200, trailers => 1,
                     headers => [['content-type', 'text/plain']] });
    await $send->({ type => 'http.response.body', body => '', more => 0 });
    await $send->({ type => 'http.response.trailers', headers => [] });
    return;
};

subtest 'empty body + empty trailers' => sub {
    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(app => $empty_app);

    my (%closed, $body);
    $body = '';
    my ($client, $header_blocks, $data_frames) = create_tracking_client(
        on_data_chunk_recv => sub { my ($sid, $d) = @_; $body .= $d; return 0 },
        on_stream_close    => sub { my ($sid, $c) = @_; $closed{$sid} = $c; return 0 },
    );
    h2c_handshake($client, $client_sock);

    my $stream_id = $client->submit_request(
        method    => 'GET',
        path      => '/empty',
        scheme    => 'http',
        authority => 'localhost',
        headers   => [],
    );
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 20);

    $stream_io->close_now;
    $loop->remove($server);

    is($body, '', 'no body bytes delivered');
    for my $df (@$data_frames) {
        ok(!($df->{flags} & NGHTTP2_FLAG_END_STREAM),
            'no DATA frame (if any were sent) carries END_STREAM');
    }
    my @trailer_blocks = grep { $_->{category} == NGHTTP2_HCAT_HEADERS() } @$header_blocks;
    is(scalar(@trailer_blocks), 1, 'exactly one trailing HEADERS block received');
    ok(($trailer_blocks[0]{flags} & NGHTTP2_FLAG_END_STREAM), 'trailing HEADERS carries END_STREAM');
    is($trailer_blocks[0]{pairs}, [], 'trailer block is empty');
    is($closed{$stream_id}, 0, 'stream closes cleanly (error code 0)');
};

# ============================================================
# file-body / fh-body + trailers under withheld flow control
# ============================================================
# The provider path (submit_response_streaming + $data_callback), not the
# single-shot arm, is what carries trailers for file/fh bodies -- and it
# must hold the trailing HEADERS behind ALL queued DATA even when nghttp2
# itself is deferred on an exhausted per-stream flow-control window, not
# just when our own send_queue is momentarily empty.

my $flow_unit = join('', map { chr(32 + ($_ % 95)) } 0..96);   # 97 printable bytes
my $flow_pattern = $flow_unit x (int(200_000 / length($flow_unit)) + 1);
$flow_pattern = substr($flow_pattern, 0, 200_000);
my ($flow_fh, $flow_file) = tempfile(UNLINK => 1);
binmode $flow_fh;
print $flow_fh $flow_pattern;
close $flow_fh;

for my $mode (qw(file fh)) {
    subtest "$mode-body + trailers under withheld flow control" => sub {
        my $app = async sub {
            my ($scope, $receive, $send) = @_;
            while (1) {
                my $e = await $receive->();
                last if $e->{type} ne 'http.request' || !$e->{more};
            }
            await $send->({ type => 'http.response.start', status => 200, trailers => 1,
                             headers => [['content-type', 'application/octet-stream']] });
            if ($mode eq 'file') {
                await $send->({ type => 'http.response.body', file => $flow_file });
            } else {
                open my $fh, '<:raw', $flow_file or die "Cannot open: $!";
                await $send->({ type => 'http.response.body', fh => $fh });
                close $fh;
            }
            await $send->({ type => 'http.response.trailers',
                             headers => [['x-checksum', 'abc']] });
            return;
        };

        my $server = create_test_server(app => $app, h2_initial_window_size => 5000);
        my ($conn, $stream_io, $client_sock, undef) =
            create_h2c_connection(app => $app, server => $server);

        my $body = '';
        my ($client, $header_blocks, $data_frames) = create_tracking_client(
            on_data_chunk_recv => sub { my ($sid, $d) = @_; $body .= $d; return 0 },
        );
        h2c_handshake($client, $client_sock);

        my $stream_id = $client->submit_request(
            method    => 'GET',
            path      => "/$mode-flow",
            scheme    => 'http',
            authority => 'localhost',
            headers   => [],
        );
        $client_sock->syswrite($client->mem_send);

        # Withhold phase: drain server->client only, never forward the
        # client's own generated bytes back -- that output would include
        # the h2-level auto WINDOW_UPDATE nghttp2 generates on receipt of
        # DATA, so withholding it keeps the server's per-stream send window
        # exhausted after the initial window's worth of data (idiom from
        # t/http2/29-fullflush-cancel.t).
        for (1..30) {
            $loop->loop_once(0.02);
            my $buf = '';
            $client_sock->sysread($buf, 16384);
            $client->mem_recv($buf) if length($buf);
        }

        my $partial_len = length($body);
        ok($partial_len > 0, "$mode: some DATA arrived before the window was exhausted");
        ok($partial_len < length($flow_pattern),
            "$mode: NOT all DATA arrived yet -- window withheld ($partial_len / " . length($flow_pattern) . ')')
            or diag('received the full body already -- flow control was not provoked this run');
        my @early_trailer_blocks = grep { $_->{category} == NGHTTP2_HCAT_HEADERS() } @$header_blocks;
        is(scalar(@early_trailer_blocks), 0,
            "$mode: no trailing HEADERS block received yet (still behind withheld DATA)");

        # Release: resume normal bidirectional pumping (the client's
        # WINDOW_UPDATE now flows) until the transfer settles.
        exchange_frames($client, $client_sock, 150);

        $stream_io->close_now;
        $loop->remove($server);

        is($body, $flow_pattern, "$mode: full body byte-exact after release");
        my @trailer_blocks = grep { $_->{category} == NGHTTP2_HCAT_HEADERS() } @$header_blocks;
        is(scalar(@trailer_blocks), 1, "$mode: exactly one trailing HEADERS block received");
        ok(($trailer_blocks[0]{flags} & NGHTTP2_FLAG_END_STREAM),
            "$mode: trailing HEADERS carries END_STREAM");
    };
}

# ============================================================
# Native-failure rollback: submit_trailer throws
# ============================================================
# The client-RST-before-h2_closed-processing race isn't deterministically
# provokable from the outside, so spy-wrap submit_trailer (on the session
# wrapper class, same pattern as t/http2/12-error-handling.t's
# submit_response spy) to throw once, simulating an immediate nghttp2-level
# rejection. Asserts: the app's send() Future fails; $seq/mirror rolled
# back to 'awaiting_trailers' (observed indirectly -- the incomplete-
# response arm only RSTs when seq_state is NOT 'complete', so an
# INTERNAL_ERROR RST here IS the proof the rollback happened, not a
# leftover 'complete' the wrapper silently ignored); the subsequent
# incomplete-response RST uses NGHTTP2_INTERNAL_ERROR (the same machinery a
# dropped body chunk already relies on -- not duplicated by the trailers
# arm).

my ($native_fail_err, $native_fail_returned);
my $native_fail_app = async sub {
    my ($scope, $receive, $send) = @_;
    while (1) {
        my $e = await $receive->();
        last if $e->{type} ne 'http.request' || !$e->{more};
    }
    await $send->({ type => 'http.response.start', status => 200, trailers => 1,
                     headers => [['content-type', 'text/plain']] });
    await $send->({ type => 'http.response.body', body => 'body', more => 0 });
    $native_fail_err = do {
        local $@;
        eval { await $send->({ type => 'http.response.trailers', headers => [['x-t', '1']] }) };
        $@;
    };
    $native_fail_returned = 1;
    return;   # the trailers never actually landed -- the incomplete arm must fire
};

subtest 'native submit_trailer failure rolls back $seq and RSTs INTERNAL_ERROR' => sub {
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };

    my $submit_trailer_calls = 0;
    my ($conn, $stream_io, $client_sock, $server, $stream_id, %closed);
    {
        no warnings 'redefine';
        local *PAGI::Server::Protocol::HTTP2::Session::submit_trailer = sub {
            my ($self, $sid, %args) = @_;
            $submit_trailer_calls++;
            die "spy: simulated native submit_trailer rejection\n" if $submit_trailer_calls == 1;
            return $self->{nghttp2}->submit_trailer($sid, headers => $args{headers} // []);
        };

        ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(app => $native_fail_app);
        my ($client) = create_tracking_client(
            on_stream_close => sub { my ($sid, $c) = @_; $closed{$sid} = $c; return 0 },
        );
        h2c_handshake($client, $client_sock);

        $stream_id = $client->submit_request(
            method    => 'GET',
            path      => '/native-fail',
            scheme    => 'http',
            authority => 'localhost',
            headers   => [],
        );
        $client_sock->syswrite($client->mem_send);
        exchange_frames($client, $client_sock, 30);
    }

    ok($native_fail_returned, 'the app reached its return after the failed trailers send');
    is($submit_trailer_calls, 1, 'the spy intercepted exactly one submit_trailer call');
    like($native_fail_err, qr/spy: simulated native submit_trailer rejection/,
        "the app's send() Future failed with the native rejection");

    is($closed{$stream_id}, H2_INTERNAL_ERROR_CODE,
        'the incomplete-response arm reset the stream with NGHTTP2_INTERNAL_ERROR '
        . '(proof the rollback left seq_state at awaiting_trailers, not complete)');
    ok((grep { /incomplete response.*trailers were declared but never sent/ } @warnings),
        'the incomplete-response warning fired with the trailers-specific note')
        or diag("warnings: @warnings");

    $stream_io->close_now;
    $loop->remove($server);

    is(scalar keys %{ $conn->{h2_streams} }, 0, 'no leaked stream state after the native failure');
};

# ============================================================
# Disconnect before trailers: send() resolves as a silent no-op
# ============================================================
# The h2_closed carve-out at the top of the send closure governs every
# arm, including the new trailers one: a trailers send arriving after the
# client has already reset the stream must resolve successfully instead
# of reaching nghttp2 a second time on a stream id it no longer owns
# (extends t/http2/12-error-handling.t's post-413 "send after close is a
# silent no-op" idiom to the trailers arm).

my $disconnect_pause;
my ($disconnect_err, $disconnect_resolved);
my $disconnect_app = async sub {
    my ($scope, $receive, $send) = @_;
    while (1) {
        my $e = await $receive->();
        last if $e->{type} ne 'http.request' || !$e->{more};
    }
    await $send->({ type => 'http.response.start', status => 200, trailers => 1,
                     headers => [['content-type', 'text/plain']] });
    await $send->({ type => 'http.response.body', body => 'x', more => 0 });
    await $disconnect_pause;   # released by the test once the reset has settled
    $disconnect_err = do {
        local $@;
        eval { await $send->({ type => 'http.response.trailers', headers => [['x-t', '1']] }) };
        $@;
    };
    $disconnect_resolved = 1;
    return;
};

subtest 'disconnect before trailers: trailers send resolves as a silent no-op' => sub {
    $disconnect_pause = Future->new;
    $disconnect_err = undef;
    $disconnect_resolved = 0;

    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };

    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(app => $disconnect_app);
    my ($client) = create_tracking_client();
    h2c_handshake($client, $client_sock);

    my $stream_id = $client->submit_request(
        method    => 'GET',
        path      => '/disconnect',
        scheme    => 'http',
        authority => 'localhost',
        headers   => [],
    );
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 20);   # body delivered, app now parked on $disconnect_pause

    $client->submit_rst_stream($stream_id, H2_CANCEL_CODE);
    $client_sock->syswrite($client->mem_send);

    # Bounded wait for the connection to actually reclaim the stream state
    # (the loop->later-deferred delete in _h2_on_close) before releasing
    # the app -- the carve-out is exercised whether the entry is merely
    # h2_closed or already gone, but waiting for the deferred delete
    # exercises the harder (fully-reclaimed) case.
    my $reclaimed = 0;
    for (1..50) {
        $loop->loop_once(0.02);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        if (!$conn->{h2_streams}{$stream_id}) { $reclaimed = 1; last; }
    }
    ok($reclaimed, 'stream state reclaimed after the client reset');

    $disconnect_pause->done unless $disconnect_pause->is_ready;
    for (1..20) { $loop->loop_once(0.02); }

    ok($disconnect_resolved, "the app's post-disconnect trailers send resolved instead of hanging");
    ok(!$disconnect_err, 'no error was raised by the post-disconnect trailers send')
        or diag("error: $disconnect_err");
    ok(!(grep { /PAGI application error/ } @warnings),
        'no app-error log for the post-disconnect no-op');

    $stream_io->close_now;
    $loop->remove($server);

    is(scalar keys %{ $conn->{h2_streams} }, 0, 'no leaked stream state');
};

done_testing;
