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

done_testing;
