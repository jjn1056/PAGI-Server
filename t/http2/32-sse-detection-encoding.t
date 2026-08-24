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

use constant H2_CANCEL => 8;   # RST_STREAM error code CANCEL (RFC 9113)

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

# ============================================================
# Design section 11.1: truthful SSE request bodies (h2)
# ============================================================
# The h2 SSE receive closure's first call must not return a POST body still
# in flight behind a truthful more=>0. Wait for body_complete (set on
# END_STREAM or stream close) and deliver the whole body in one terminal
# event -- the smaller change relative to reworking the one-shot sse.request
# shape into a truthful multi-chunk stream.

subtest 'h2 GET-SSE: single empty terminal sse.request event (unchanged)' => sub {
    my $first_event;
    my $second_event;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        $first_event = await $receive->();
        await $send->({ type => 'sse.start', status => 200 });
        $second_event = await $receive->();
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

    exchange_frames($client, $client_sock, 10);

    is($first_event->{type}, 'sse.request', 'first event is sse.request');
    is($first_event->{body}, '', 'GET body is empty');
    is($first_event->{more}, 0, 'GET terminal event has more=>0');

    # Close the client side to simulate disconnect and let the server
    # process it.
    close($client_sock);
    for (1 .. 10) {
        $loop->loop_once(0.1);
    }

    is($second_event->{type}, 'sse.disconnect',
        'second receive() is sse.disconnect, not a second sse.request');

    $stream_io->close_now;
    $loop->remove($server);
};

subtest 'h2 POST-SSE: body delivered complete with truthful more when END_STREAM is delayed past the first pending receive()' => sub {
    my $received_body;
    my $received_more;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        my $event = await $receive->();
        $received_body = $event->{body};
        $received_more = $event->{more};
        await $send->({ type => 'sse.start', status => 200 });
        await $send->({ type => 'sse.send', data => 'ack' });
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

    # Streaming request body under our control: the callback defers (returns
    # undef) whenever the queue is empty, so no more DATA -- and no
    # END_STREAM -- goes out until we explicitly push and resume_stream.
    my @queue = ('first-chunk-');
    my $eof_ready = 0;
    my $data_cb = sub {
        my ($sid, $max_len) = @_;
        return (shift(@queue), 0) if @queue;
        return ('', 1) if $eof_ready;
        return undef;   # defer
    };

    my $stream_id = $client->submit_request(
        method    => 'POST',
        path      => '/events',
        scheme    => 'http',
        authority => 'localhost',
        headers   => [['accept', 'text/event-stream']],
        body      => $data_cb,
    );
    $client_sock->syswrite($client->mem_send);

    # Let the server dispatch the stream and reach the app's first
    # receive() call. Nothing else is queued to send, so with the fix that
    # call must now be blocked awaiting body_pending -- not yet returned.
    exchange_frames($client, $client_sock, 10);
    ok(!defined $received_body, 'first receive() has not returned yet -- still awaiting the full body')
        or diag "received_body so far: " . (defined $received_body ? $received_body : '(undef)');

    push @queue, 'second-chunk';
    $client->resume_stream($stream_id);
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 5);

    $eof_ready = 1;
    $client->resume_stream($stream_id);
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 10);

    is($received_body, 'first-chunk-second-chunk', 'full body delivered across both DATA frames');
    is($received_more, 0, 'more flag is truthfully 0 on the terminal sse.request event');

    $stream_io->close_now;
    $loop->remove($server);
};

subtest 'h2 POST-SSE: body already complete before the first receive() call (pin)' => sub {
    my $received_body;
    my $received_more;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        my $event = await $receive->();
        $received_body = $event->{body};
        $received_more = $event->{more};
        await $send->({ type => 'sse.start', status => 200 });
        await $send->({ type => 'sse.send', data => 'ack' });
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(app => $app);

    my $client = create_client();

    h2c_handshake($client, $client_sock);

    # Whole body handed to submit_request as a static string: HEADERS +
    # DATA(END_STREAM) all go out together, ahead of the server's deferred
    # dispatch, so body_complete is already true before the app's first
    # receive() call ever runs.
    $client->submit_request(
        method    => 'POST',
        path      => '/events',
        scheme    => 'http',
        authority => 'localhost',
        headers   => [['accept', 'text/event-stream']],
        body      => 'already-here',
    );
    $client_sock->syswrite($client->mem_send);

    exchange_frames($client, $client_sock, 10);

    is($received_body, 'already-here', 'full body delivered on the first receive() call');
    is($received_more, 0, 'more=>0 on the terminal sse.request event');

    $stream_io->close_now;
    $loop->remove($server);
};

subtest 'h2 POST-SSE: RST_STREAM while first receive() is pending delivers sse.disconnect, not a truncated sse.request' => sub {
    my $received_event;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        $received_event = await $receive->();

        # Respond regardless of what came back, like a real app would --
        # this is what this subtest is pinned on, not the (separate,
        # pre-existing) dispatcher behavior for an app that returns having
        # never started a response.
        eval { await $send->({ type => 'sse.start', status => 200 }) };
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(app => $app);

    my $client = create_client();

    h2c_handshake($client, $client_sock);

    # Streaming request body under our control (see the subtest above):
    # the callback defers forever, so no END_STREAM ever goes out on its
    # own -- only the RST_STREAM below ends the stream.
    my @queue = ('partial-chunk');
    my $data_cb = sub {
        my ($sid, $max_len) = @_;
        return (shift(@queue), 0) if @queue;
        return undef;   # defer -- never sends END_STREAM
    };

    my $stream_id = $client->submit_request(
        method    => 'POST',
        path      => '/events',
        scheme    => 'http',
        authority => 'localhost',
        headers   => [['accept', 'text/event-stream']],
        body      => $data_cb,
    );
    $client_sock->syswrite($client->mem_send);

    # Let the server dispatch and reach the app's first pending receive() --
    # only the partial chunk has gone out, so the body is not complete.
    exchange_frames($client, $client_sock, 10);
    ok(!defined $received_event, 'first receive() has not returned yet -- still awaiting the full body');

    # Abnormally terminate the stream while the app is still waiting for
    # the rest of the body. _h2_on_close sets body_complete=1 AND queues
    # sse.disconnect in the same call, before waking body_pending -- the
    # queued disconnect must win over falling through to a truncated
    # terminal sse.request.
    $client->submit_rst_stream($stream_id, H2_CANCEL);
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 10);

    is($received_event->{type}, 'sse.disconnect',
        'app got sse.disconnect, not a truncated sse.request, when the stream was reset mid-body')
        or diag "received_event: " . (defined $received_event ? "type=$received_event->{type} body=" . ($received_event->{body} // '(undef)') : '(undef)');

    $stream_io->close_now;
    $loop->remove($server);
};

done_testing;
