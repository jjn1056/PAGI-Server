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
        log_level => 'warn',
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
        # Connection->new reads its own write_high_watermark/etc. from its
        # OWN args (not from $server), so a caller that needs to raise it
        # (see park_in_deferred_trailers below) must pass it here too.
        %{ $overrides{connection_args} // {} },
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
# Connection-specific headers must be stripped from trailers too
# ============================================================
# design 13.3 / RFC 9113 8.2.2 forbid connection-specific fields from an
# HTTP/2 response; the strip already applied to http.response.start,
# sse.start, websocket.accept, and the WebSocket/SSE decline paths, but not
# to http.response.trailers. Reproduced live: nghttp2 rejects a trailer
# HEADERS block containing a forbidden field by discarding the WHOLE block
# -- the peer sees zero trailing HEADERS (on_stream_close never fires) even
# though the server's own submit_trailer call and $send Future report
# success. This pins the fix: the six forbidden names are stripped from the
# trailer list exactly like the response-header paths, with one difference
# -- 'te' has no 'trailers'-token carve-out inside a trailer block itself
# (RFC 9110 6.6.2: connection-specific fields are banned from trailers
# outright), so a trailer-borne 'te: trailers' is stripped too.

my ($strip_cs, $strip_complete, $strip_disconnect);
my $strip_app = async sub {
    my ($scope, $receive, $send) = @_;
    my $cs = $scope->{'pagi.connection'};
    $strip_cs = $cs;
    $cs->on_complete(sub { $strip_complete = 1 });
    $cs->on_disconnect(sub { $strip_disconnect = $_[0] });
    while (1) {
        my $e = await $receive->();
        last if $e->{type} ne 'http.request' || !$e->{more};
    }
    await $send->({ type => 'http.response.start', status => 200, trailers => 1,
                     headers => [['content-type', 'text/plain']] });
    await $send->({ type => 'http.response.body', body => 'ok', more => 0 });
    await $send->({ type => 'http.response.trailers',
                     headers => [
                         ['connection',        'close'],
                         ['keep-alive',        'timeout=5'],
                         ['proxy-connection',  'keep-alive'],
                         ['transfer-encoding', 'chunked'],
                         ['upgrade',           'h2c'],
                         ['te',                'trailers'],
                         ['x-checksum',        'abc'],
                     ] });
    return;
};

subtest 'connection-specific headers are stripped from trailers (RFC 9113), including a bare te: trailers' => sub {
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };

    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(app => $strip_app);

    my %closed;
    my ($client, $header_blocks, $data_frames) = create_tracking_client(
        on_stream_close => sub { my ($sid, $c) = @_; $closed{$sid} = $c; return 0 },
    );
    h2c_handshake($client, $client_sock);

    my $stream_id = $client->submit_request(
        method    => 'GET',
        path      => '/strip-trailers',
        scheme    => 'http',
        authority => 'localhost',
        headers   => [],
    );
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 30);

    $stream_io->close_now;
    $loop->remove($server);

    my @trailer_blocks = grep { $_->{category} == NGHTTP2_HCAT_HEADERS() } @$header_blocks;
    is(scalar(@trailer_blocks), 1,
        'exactly one trailing HEADERS block received (not silently destroyed by the forbidden fields)');
    is($trailer_blocks[0]{pairs}, [['x-checksum', 'abc']],
        'the legitimate field survives; all six connection-specific fields (including te: trailers) are stripped');
    ok(($trailer_blocks[0]{flags} & NGHTTP2_FLAG_END_STREAM),
        'trailing HEADERS still carries END_STREAM');

    my @strip_warnings = grep { /connection-specific header/ } @warnings;
    is(scalar(@strip_warnings), 6, 'one warning per stripped trailer field, including te');

    is($closed{$stream_id}, 0, 'stream closes cleanly (error code 0)');
    ok($strip_complete, 'on_complete fired');
    is($strip_disconnect, undef, 'on_disconnect did not fire');
};

# ============================================================
# Empty body + empty/absent trailers
# ============================================================
# start(trailers=>1) -> terminal EMPTY body -> EMPTY/ABSENT trailers. The
# single-shot terminal body must still route through the streaming path
# (the step-6 CRITICAL DESIGN POINT) even though there is no payload to
# push, and an empty/absent trailers 'headers' list still submits a
# (terminal) trailing HEADERS block per the Interfaces contract (Changes /
# Compliance.pod both say "empty or absent" -- pin both shapes, not just the
# empty-list one; the map/grep at :1468-1470 defaults a missing key to []
# via `$event->{headers} // []`, but that default was previously untested).

for my $headers_mode (qw(empty_list absent_key)) {
    my $empty_app = async sub {
        my ($scope, $receive, $send) = @_;
        while (1) {
            my $e = await $receive->();
            last if $e->{type} ne 'http.request' || !$e->{more};
        }
        await $send->({ type => 'http.response.start', status => 200, trailers => 1,
                         headers => [['content-type', 'text/plain']] });
        await $send->({ type => 'http.response.body', body => '', more => 0 });
        if ($headers_mode eq 'empty_list') {
            await $send->({ type => 'http.response.trailers', headers => [] });
        } else {
            await $send->({ type => 'http.response.trailers' });   # 'headers' key omitted entirely
        }
        return;
    };

    subtest "empty body + $headers_mode trailers" => sub {
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
            path      => "/empty-$headers_mode",
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
}

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

# file mode is temporarily absent from this loop: the first
# IO::Async::Function worker call takes ~0.84s before its first result
# flows (upstream, RT#181086), so the file arm's AsyncFile path delivers
# zero DATA inside the withhold budget and the subtest fails
# deterministically. The full file+fh version is preserved on branch
# hold/rt181086-file-flow-withhold-test; restore it when the upstream fix
# lands. The fh arm reads inline and keeps covering the
# trailers-behind-withheld-DATA contract meanwhile.
for my $mode (qw(fh)) {
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

# ============================================================
# Teardown while parked in the deferred trailers branch
# ============================================================
# The deferred branch (see the closure-top comment in _h2_create_send)
# stages the trailer headers and PARKS the send() on a Future
# ($ss->{trailer_wait}) that only $deliver_trailer_eof -- the data
# callback's own terminal invocation -- normally resolves. Two teardown
# paths must release that park instead of leaving it hanging forever:
#
#   - a per-stream RST (client_closed): _h2_on_close already releases
#     stream_drain_waiters; it must release trailer_wait the same way.
#   - a whole-connection drop (FIN / idle timeout / shutdown): no h2
#     protocol event fires at all (on_stream_close never runs), so only
#     _close's own sweep can release a park like this one -- _handle_
#     disconnect_and_close -> _close is the only code that ever runs.
#
# Both use the SAME 200,000-byte fixture and default 65535-byte client
# window as the subtests above, but via an `fh` body (not `file`): the
# file arm's AsyncFile reads cross real IO::Async::Function worker-process
# round trips, which take longer than a bounded 30-round/0.02s withhold
# window to complete -- under that timing the app would still be parked
# inside the FILE arm's OWN backpressure wait (already covered above),
# never having reached the trailers arm at all. The fh arm reads
# synchronously in-process, so raising write_high_watermark (passed
# straight to Connection->new -- it does not inherit from $server) lets
# our OWN send-queue backpressure get out of the way entirely, while
# nghttp2's real per-stream flow control (unaffected by that setting)
# still caps what actually drains at the default window. That reliably
# lands the app in the trailers arm's `await $f` -- not the fh arm's own
# wait -- well within the withhold budget. Confirmed with a standalone
# instrumented harness before writing this: pre-fix, the app reaches
# `await $f` and then genuinely never resumes; post-fix, it resumes
# within the very next loop tick after the teardown.

my ($park_cs, $park_complete, $park_disconnect, $park_err, $park_returned);
my $park_app = async sub {
    my ($scope, $receive, $send) = @_;
    my $cs = $scope->{'pagi.connection'};
    $park_cs = $cs;
    $cs->on_complete(sub { $park_complete = 1 });
    $cs->on_disconnect(sub { $park_disconnect = $_[0] });
    while (1) {
        my $e = await $receive->();
        last if $e->{type} ne 'http.request' || !$e->{more};
    }
    await $send->({ type => 'http.response.start', status => 200, trailers => 1,
                     headers => [['content-type', 'application/octet-stream']] });
    open my $fh, '<:raw', $flow_file or die "Cannot open: $!";
    await $send->({ type => 'http.response.body', fh => $fh });
    close $fh;
    $park_err = do {
        local $@;
        eval { await $send->({ type => 'http.response.trailers', headers => [['x-checksum', 'abc']] }) };
        $@;
    };
    $park_returned = 1;
    return;
};

sub reset_park_vars {
    ($park_cs, $park_complete, $park_disconnect, $park_err, $park_returned) = (undef, 0, undef, undef, 0);
}

# Drives the app into the deferred branch: submit the request, then
# withhold the client's own generated bytes (same idiom as the
# flow-control subtests above) so the fh body's data callback can't
# drain far enough to ever see EOF -- $data_eof_delivered stays false,
# so the trailers send() that follows takes the deferred (parking) path.
sub park_in_deferred_trailers {
    my ($path, %opts) = @_;
    my $server = create_test_server(app => $park_app);
    my ($conn, $stream_io, $client_sock) =
        create_h2c_connection(
            app             => $park_app,
            server          => $server,
            connection_args => { write_high_watermark => 300_000 },
        );
    my ($client) = create_tracking_client();
    h2c_handshake($client, $client_sock);

    my $stream_id = $client->submit_request(
        method    => 'GET',
        path      => $path,
        scheme    => 'http',
        authority => 'localhost',
        headers   => [],
    );
    $client_sock->syswrite($client->mem_send);

    for (1..30) {
        $loop->loop_once(0.02);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
    }

    return ($server, $conn, $stream_io, $client, $client_sock, $stream_id);
}

subtest 'teardown while parked: client RST during withheld flow control resolves the parked send' => sub {
    reset_park_vars();
    my ($server, $conn, $stream_io, $client, $client_sock, $stream_id) =
        park_in_deferred_trailers('/park-rst');

    ok(!$park_returned, 'app is genuinely parked in the deferred trailers branch before the RST')
        or diag('app already returned before the RST -- this run cannot exercise the deferred park');

    $client->submit_rst_stream($stream_id, H2_CANCEL_CODE);
    $client_sock->syswrite($client->mem_send);

    # Bounded wait -- the parked send must resolve, not hang.
    my $resolved = 0;
    for (1..50) {
        $loop->loop_once(0.02);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        if ($park_returned) { $resolved = 1; last; }
    }
    ok($resolved, 'the parked trailers send resolved after the client RST (no hang)')
        or diag('app never returned -- the parked send() hung after the client RST');

    ok(!$park_err, 'no error was raised by the released trailers send') or diag("error: $park_err");
    ok($park_cs, 'app captured a connection_state');
    is($park_cs->disconnect_reason, 'client_closed', 'the RST is attributed to the client');
    is($park_disconnect, 'client_closed', 'on_disconnect fired with client_closed');
    is($park_complete, 0, 'on_complete never fired');

    $stream_io->close_now;
    $loop->remove($server);
};

subtest 'teardown while parked: dropping the connection during withheld flow control resolves the parked send (C1)' => sub {
    reset_park_vars();
    my ($server, $conn, $stream_io, $client, $client_sock, $stream_id) =
        park_in_deferred_trailers('/park-close');

    ok(!$park_returned, 'app is genuinely parked in the deferred trailers branch before the drop')
        or diag('app already returned before the connection drop -- this run cannot exercise C1');

    # Drop the whole connection -- not a stream-level RST. Closing the
    # client's own socket end drives the server's read handler to observe
    # EOF on its next loop tick, which is _handle_disconnect_and_close's
    # own trigger (see Connection.pm ~:310-313); no h2 frame is involved,
    # so on_stream_close never fires and _h2_on_close's release never
    # runs -- only _close's own sweep (the C1 fix) can release this park.
    $client_sock->close;

    # Bounded wait -- pre-fix, this hangs for the whole budget (the bug
    # this subtest exists to catch); it must never be allowed to spin
    # unboundedly.
    my $resolved = 0;
    for (1..50) {
        $loop->loop_once(0.02);
        if ($park_returned) { $resolved = 1; last; }
    }
    ok($resolved, 'the parked trailers send resolved after the connection dropped (no hang) -- C1')
        or diag('app never returned -- the parked send() hung after the connection was dropped');

    ok(!$park_err, 'no error was raised by the released trailers send') or diag("error: $park_err");

    $loop->remove($server);
};

# ============================================================
# Deferred submit_trailer failure (native rejection on the DEFERRED path)
# ============================================================
# Subtest 5 above ("native submit_trailer failure...") only exercises the
# DIRECT branch (the spy's first call always lands there for a tiny,
# unblocked body). This exercises the DEFERRED branch specifically: the
# spy fires from inside $deliver_trailer_eof -- i.e. from inside the data
# callback itself -- once the withheld window is released and the body
# finally drains. Same assertions as subtest 5: the app's send() Future
# fails, the incomplete-response arm resets with NGHTTP2_INTERNAL_ERROR
# (indirect proof of the $seq/mirror rollback), and the warning fires.

subtest 'deferred submit_trailer failure rolls back $seq and RSTs INTERNAL_ERROR' => sub {
    reset_park_vars();

    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };

    my $submit_trailer_calls = 0;
    my ($server, $conn, $stream_io, $client, $client_sock, $stream_id, %closed);
    {
        no warnings 'redefine';
        local *PAGI::Server::Protocol::HTTP2::Session::submit_trailer = sub {
            my ($self, $sid, %args) = @_;
            $submit_trailer_calls++;
            die "spy: simulated deferred submit_trailer rejection\n" if $submit_trailer_calls == 1;
            return $self->{nghttp2}->submit_trailer($sid, headers => $args{headers} // []);
        };

        $server = create_test_server(app => $park_app);
        ($conn, $stream_io, $client_sock) =
            create_h2c_connection(
                app             => $park_app,
                server          => $server,
                connection_args => { write_high_watermark => 300_000 },
            );
        ($client) = create_tracking_client(
            on_stream_close => sub { my ($sid, $c) = @_; $closed{$sid} = $c; return 0 },
        );
        h2c_handshake($client, $client_sock);

        $stream_id = $client->submit_request(
            method    => 'GET',
            path      => '/deferred-fail',
            scheme    => 'http',
            authority => 'localhost',
            headers   => [],
        );
        $client_sock->syswrite($client->mem_send);

        for (1..30) {
            $loop->loop_once(0.02);
            my $buf = '';
            $client_sock->sysread($buf, 16384);
            $client->mem_recv($buf) if length($buf);
        }
        ok(!$park_returned, 'app is genuinely parked in the deferred trailers branch before release')
            or diag('app already returned before release -- this run cannot exercise the deferred spy');
        is($submit_trailer_calls, 0, 'submit_trailer has not been called yet -- still parked');

        # Release: the body finishes draining, $deliver_trailer_eof fires
        # from inside the data callback, and the spy throws there.
        exchange_frames($client, $client_sock, 150);
    }

    ok($park_returned, 'the app reached its return after the failed deferred trailers send');
    is($submit_trailer_calls, 1, 'the spy intercepted exactly one submit_trailer call, from the deferred path');
    like($park_err, qr/spy: simulated deferred submit_trailer rejection/,
        "the app's send() Future failed via the deferred path's loop->later resolution");

    is($closed{$stream_id}, H2_INTERNAL_ERROR_CODE,
        'the incomplete-response arm reset the stream with NGHTTP2_INTERNAL_ERROR '
        . '(proof the rollback left seq_state at awaiting_trailers, not complete)');
    ok((grep { /incomplete response.*trailers were declared but never sent/ } @warnings),
        'the incomplete-response warning fired with the trailers-specific note')
        or diag("warnings: @warnings");

    $stream_io->close_now;
    $loop->remove($server);

    is(scalar keys %{ $conn->{h2_streams} }, 0, 'no leaked stream state after the deferred native failure');
};

done_testing;
