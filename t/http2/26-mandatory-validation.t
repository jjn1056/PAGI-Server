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
# Test: HTTP/2 mandatory validation and sequencing
# ============================================================
# The h2 twin of t/52-mandatory-validation.t: every h2 send path (http,
# websocket, sse) validates and sequences unconditionally, even with
# validate_events => 0. Two h2-specific wrinkles covered here that t/52
# doesn't need to:
#  - Phase-1 stubs (fh body / fullflush) fail BEFORE the sequence machine
#    advances, so a conforming app that probes them can still finish its
#    response normally instead of being stranded. (file bodies stream for
#    real as of Task 2 -- /file-body below is now a positive control, not
#    a stub probe. Trailers stream for real as of Phase 2b Task 4 --
#    /trailers below now probes the real sequence guard: sending trailers
#    before the body is terminal still fails, via advance_http's own
#    'awaiting_trailers' precondition rather than a stub.)
#  - Once a stream's terminal state is reached (http 'complete', sse
#    'closed'/'decline_complete'), the h2_streams entry for that stream is
#    reclaimed asynchronously; a further send must still raise through the
#    state machine, not silently no-op just because the entry is gone.

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;

my $loop = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# ============================================================
# Helpers (lifted verbatim from t/http2/11-streaming.t)
# ============================================================

sub create_test_server {
    my (%args) = @_;
    my $server = PAGI::Server->new(
        app   => $args{app} // sub { },
        host  => '127.0.0.1',
        port  => 0,
        quiet => 1,
        http2 => 1,
        validate_events => 0,   # THE POINT: validation must run anyway
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
# Request helpers
# ============================================================

sub get_h2 {
    my ($path, %opts) = @_;
    my ($conn, $stream_io, $client_sock, $server) =
        create_h2c_connection(app => $opts{app});

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
    return (\%headers, $body);
}

# ============================================================
# HTTP: mandatory validation and sequencing
# ============================================================
# Each path exercises one send-contract violation. The app catches the
# failed $send Future and reports what happened in a valid response, so
# the test observes validation through real server behavior, never source
# inspection.

# Small known fixture for the /file-body positive control below.
my $FILE_BODY_CONTENT = "file-ok:mandatory-validation\n";
my ($fbfh, $FILE_BODY_FIXTURE) = tempfile(UNLINK => 1);
print $fbfh $FILE_BODY_CONTENT;
close $fbfh;

my $after_complete_err;

my $http_app = async sub {
    my ($scope, $receive, $send) = @_;
    die "unsupported scope" if $scope->{type} ne 'http';
    while (1) {
        my $e = await $receive->();
        last if $e->{type} ne 'http.request' || !$e->{more};
    }
    my $path = $scope->{path} // '/';

    my $report = async sub {
        my ($err) = @_;
        $err //= 'NO-ERROR';
        $err =~ s/\n/ /g;
        await $send->({ type => 'http.response.start', status => 200,
                        headers => [['content-type','text/plain']] });
        await $send->({ type => 'http.response.body', body => $err, more => 0 });
    };

    if ($path eq '/bad-status') {
        my $err = do { local $@; eval { await $send->({ type => 'http.response.start', status => "50\n0" }) }; $@ };
        return await $report->($err);
    }
    if ($path eq '/body-before-start') {
        my $err = do { local $@; eval { await $send->({ type => 'http.response.body', body => 'x' }) }; $@ };
        return await $report->($err);
    }
    if ($path eq '/unknown-type') {
        my $err = do { local $@; eval { await $send->({ type => 'http.response.bod', body => 'x' }) }; $@ };
        return await $report->($err);
    }
    if ($path eq '/duplicate-start') {
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-type','text/plain']] });
        my $err = do { local $@; eval { await $send->({ type => 'http.response.start', status => 500 }) }; $@ };
        $err =~ s/\n/ /g;
        await $send->({ type => 'http.response.body', body => "dup:$err", more => 0 });
        return;
    }
    if ($path eq '/file-body') {
        # Positive control (Task 2): file bodies stream for real now, so this
        # sends a known fixture and the response completes on its own -- a
        # file body is terminal per advance_http, so there is no follow-up
        # body event to send (unlike the other probes on this path).
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-type','text/plain']] });
        await $send->({ type => 'http.response.body', file => $FILE_BODY_FIXTURE });
        return;
    }
    if ($path eq '/trailers') {
        # Declare trailers, stream one chunk (not yet terminal), then probe
        # http.response.trailers too early: advance_http's own
        # 'awaiting_trailers' precondition rejects it (state is still
        # 'started_t', the body isn't complete yet) and, being a pure
        # sequence check, never advances $seq -- so the machine is still
        # 'started_t' afterward, and the terminal body below is legal.
        await $send->({ type => 'http.response.start', status => 200, trailers => 1, headers => [['content-type','text/plain']] });
        await $send->({ type => 'http.response.body', body => 'x', more => 1 });
        my $err = do { local $@; eval { await $send->({ type => 'http.response.trailers', headers => [['x-t','1']] }) }; $@ };
        $err =~ s/\n/ /g;
        await $send->({ type => 'http.response.body', body => "trailers:$err", more => 0 });
        return;
    }
    if ($path eq '/after-complete') {
        # Once the response is complete there's no valid way left to carry
        # the error back in-band, so stash it in a closure var: the point is
        # that this send raises through advance_http's terminal 'complete'
        # state rather than silently no-op'ing because the h2_streams entry
        # for a finished stream may already have been reclaimed.
        await $send->({ type => 'http.response.start', status => 200, headers => [['content-type','text/plain']] });
        await $send->({ type => 'http.response.body', body => 'done', more => 0 });
        my $err = do { local $@; eval { await $send->({ type => 'http.response.body', body => 'extra', more => 0 }) }; $@ };
        $after_complete_err = $err;
        return;
    }
    await $report->(undef);   # control path: no violation
};

my (undef, $body);

(undef, $body) = get_h2('/bad-status', app => $http_app);
like( $body, qr/must be a non-negative integer/,
    'malformed status fails the send Future with validate_events => 0' );

(undef, $body) = get_h2('/body-before-start', app => $http_app);
like( $body, qr/before http\.response\.start/, 'body before start fails' );

(undef, $body) = get_h2('/unknown-type', app => $http_app);
like( $body, qr/Unrecognized event type/, 'unknown type fails' );

(undef, $body) = get_h2('/duplicate-start', app => $http_app);
like( $body, qr/dup:.*duplicate http\.response\.start/,
    'duplicate start fails without disturbing the real response' );

(undef, $body) = get_h2('/file-body', app => $http_app);
is( $body, $FILE_BODY_CONTENT,
    'file body on h2 streams the real file content (Task 2)' );

(undef, $body) = get_h2('/trailers', app => $http_app);
like( $body, qr/trailers:.*trailers were not declared or body is not complete/,
    'trailers sent before the body is terminal fail loudly (Phase 2b Task 4: real trailers, not a stub)' );

get_h2('/after-complete', app => $http_app);
like( $after_complete_err // '', qr/response already complete/,
    'send after http response complete raises on h2, not silently swallowed' );

(undef, $body) = get_h2('/ok', app => $http_app);
is( $body, 'NO-ERROR', 'a conforming app is unaffected' );

# ============================================================
# SSE: mis-sequencing after a terminal state raises, not swallowed
# ============================================================
# Once a stream is 'closed' (sse.close) or 'decline_complete', its
# h2_streams entry is reclaimed asynchronously by _h2_on_close. These probe
# sends happen on the very next tick of the same app coroutine -- before
# that reclaim can plausibly have run -- but the sequence check must not
# rely on that timing: it consults the closure-local $seq, not the h2_streams
# entry, precisely so a send after the entry is actually gone still raises.

sub sse_probe {
    my (%args) = @_;
    my ($conn, $stream_io, $client_sock, $server) =
        create_h2c_connection(app => $args{app});
    my $client = create_client();
    h2c_handshake($client, $client_sock);
    $client->submit_request(
        method    => 'GET',
        path      => $args{path} // '/events',
        scheme    => 'http',
        authority => 'localhost',
        headers   => [['accept', 'text/event-stream']],
    );
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 20);
    $stream_io->close_now;
    $loop->remove($server);
}

my $sse_after_close_err;
my $sse_app1 = async sub {
    my ($scope, $receive, $send) = @_;
    await $send->({ type => 'sse.start', status => 200 });
    await $send->({ type => 'sse.send', data => 'one' });
    await $send->({ type => 'sse.close' });
    my $err = do { local $@; eval { await $send->({ type => 'sse.send', data => 'late' }) }; $@ };
    $sse_after_close_err = $err;
};
sse_probe(app => $sse_app1);
like( $sse_after_close_err // '', qr/after sse\.close/,
    'sse.send after sse.close raises on h2, not silently swallowed' );

my $sse_decline_complete_err;
my $sse_app2 = async sub {
    my ($scope, $receive, $send) = @_;
    await $send->({ type => 'sse.http.response.start', status => 200, headers => [['content-type','text/plain']] });
    await $send->({ type => 'sse.http.response.body', body => 'done', more => 0 });
    my $err = do { local $@; eval { await $send->({ type => 'sse.http.response.body', body => 'extra', more => 0 }) }; $@ };
    $sse_decline_complete_err = $err;
};
sse_probe(app => $sse_app2);
like( $sse_decline_complete_err // '', qr/decline response already complete/,
    'sse.http.response.body after a completed decline raises on h2, not silently swallowed' );

# ============================================================
# WebSocket: mis-sequencing after a terminal state raises, not swallowed
# ============================================================
# Same class of bug as the SSE/HTTP carve-outs above: websocket.close itself
# ends the stream (submit_data with END_STREAM), and _h2_on_close reclaims
# the h2_streams entry for that stream asynchronously -- independent of
# protocol family. A post-close/post-denial-complete send must still raise
# through advance_websocket, not silently no-op on a "stream gone" check.

sub ws_probe {
    my (%args) = @_;
    my ($conn, $stream_io, $client_sock, $server) =
        create_h2c_connection(app => $args{app});
    my $client = create_client();
    h2c_handshake($client, $client_sock);
    $client->submit_request(
        method    => 'CONNECT',
        path      => $args{path} // '/ws',
        scheme    => 'http',
        authority => 'localhost',
        headers   => [
            [':protocol', 'websocket'],
            ['sec-websocket-version', '13'],
        ],
        body      => sub { return undef },   # streaming: keep open
    );
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 20);
    $stream_io->close_now;
    $loop->remove($server);
}

my $ws_after_close_err;
my $ws_app1 = async sub {
    my ($scope, $receive, $send) = @_;
    await $send->({ type => 'websocket.accept' });
    await $send->({ type => 'websocket.close' });
    my $err = do { local $@; eval { await $send->({ type => 'websocket.send', text => 'late' }) }; $@ };
    $ws_after_close_err = $err;
};
ws_probe(app => $ws_app1);
like( $ws_after_close_err // '', qr/after websocket\.close/,
    'websocket.send after websocket.close raises on h2, not silently swallowed' );

my $ws_denial_complete_err;
my $ws_app2 = async sub {
    my ($scope, $receive, $send) = @_;
    await $send->({ type => 'websocket.http.response.start', status => 403,
                    headers => [['content-type','text/plain']] });
    await $send->({ type => 'websocket.http.response.body', body => 'no', more => 0 });
    my $err = do { local $@; eval { await $send->({ type => 'websocket.http.response.body', body => 'extra', more => 0 }) }; $@ };
    $ws_denial_complete_err = $err;
};
ws_probe(app => $ws_app2);
like( $ws_denial_complete_err // '', qr/denial response already complete/,
    'websocket.http.response.body after a completed denial raises on h2, not silently swallowed' );

done_testing;
