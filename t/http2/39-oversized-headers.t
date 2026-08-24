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
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.009+ required)');
}

# ============================================================
# Test: HTTP/2 oversized request headers -> real 431 response
# ============================================================
# RFC 9113 section 10.5.1: "a server that receives a larger header block
# than it is willing to handle can send an HTTP 431". Previously, an
# oversized HEADERS block on the initial request caused PAGI::HTTP2's
# on_header callback to abort HPACK decoding immediately
# (NGHTTP2_ERR_TEMPORAL_CALLBACK_FAILURE), which nghttp2 turns into a bare
# RST_STREAM: the client never sees a :status at all. HTTP/1.1 already
# returns a real 431 for the same condition (t/10-http-compliance.t). This
# file pins the Go net/http2-style fix: HPACK decoding is allowed to run to
# completion (fields are discarded once oversized, not stored), and once the
# HEADERS frame commits, a real 431 is synthesized on the stream instead of
# dispatching to the application.
#
# The REQUEST-block overflow path is what changed. Trailer-block overflow
# (a second, later HEADERS block on an already-dispatched stream) keeps its
# existing behavior: the pending block is deleted and the stream is reset --
# see subtest (c) below.

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;
use Net::HTTP2::nghttp2 ();

my $loop = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# ============================================================
# Helpers (same pattern as t/http2/12-error-handling.t and
# t/http2/34-request-trailers.t)
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
    my $server = $overrides{server} // create_test_server(app => $app, %overrides);

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
        max_body_size => $server->{max_body_size},
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
        $loop->loop_once(0.1);
        my $buf = '';
        $client_sock->sysread($buf, 16384);
        $client->mem_recv($buf) if length($buf);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length($out);
    }
}

sub hpack_literal_new_name {
    my ($name, $value) = @_;
    # RFC 7541 section 6.2.2: Literal Header Field without Indexing --
    # New Name. Only correct for short ASCII lengths (no HPACK integer
    # continuation), which is all this file needs.
    return "\x00" . chr(length($name)) . $name . chr(length($value)) . $value;
}

sub raw_headers_frame {
    my ($stream_id, $header_pairs, %opts) = @_;
    my $payload = join('', map { hpack_literal_new_name(@$_) } @$header_pairs);
    my $flags = 0x4;                       # END_HEADERS
    $flags |= 0x1 if $opts{end_stream};    # END_STREAM
    my $len24 = substr(pack('N', length($payload)), 1, 3);
    return $len24 . chr(0x1) . chr($flags) . pack('N', $stream_id & 0x7fffffff) . $payload;
}

# ============================================================
# (a) oversized REQUEST headers -> real 431, not a bare RST_STREAM
# ============================================================
subtest 'oversized request headers get a real 431 instead of RST_STREAM' => sub {
    my $invocations = 0;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        $invocations++;
    };

    my (@warnings, $conn, %response_headers, $response_body, %closed);
    {
        local $SIG{__WARN__} = sub { push @warnings, $_[0] };

        my ($c, $stream_io, $client_sock, $server) = create_h2c_connection(
            app                   => $app,
            h2_max_header_list_size => 200,
        );
        $conn = $c;
        $response_body = '';

        my $client = create_client(
            on_header => sub {
                my ($sid, $name, $value) = @_;
                $response_headers{$name} = $value;
                return 0;
            },
            on_data_chunk_recv => sub {
                my ($sid, $data) = @_;
                $response_body .= $data;
                return 0;
            },
            on_stream_close => sub {
                my ($sid, $error_code) = @_;
                $closed{$sid} = $error_code;
                return 0;
            },
        );
        h2c_handshake($client, $client_sock);

        # A single field well over the 200-byte budget.
        $client->submit_request(
            method    => 'GET',
            path      => '/',
            scheme    => 'http',
            authority => 'localhost',
            headers   => [['x-big', 'A' x 500]],
        );
        $client_sock->syswrite($client->mem_send);
        exchange_frames($client, $client_sock, 20);

        $stream_io->close_now;
        $loop->remove($server);
    }

    is( $response_headers{':status'}, 431, 'server responded 431 (not a bare RST_STREAM)' );
    is( lc($response_headers{'content-type'} // ''), 'text/plain', 'content-type: text/plain' );
    ok( exists $response_headers{'date'}, '431 response carries a Date header' );
    ok( length($response_body) > 0, '431 response has a body' );

    is( $invocations, 0, 'application was never dispatched for the oversized request' );

    my ($only_stream_id) = keys %closed;
    is( $closed{$only_stream_id}, 0, 'stream closed cleanly (error code 0), not via RST' )
        if defined $only_stream_id;

    is( scalar keys %{ $conn->{h2_streams} }, 0, 'no leaked stream state' );
    is( scalar(@warnings), 0, 'no warnings logged' ) or diag(@warnings);
};

# ============================================================
# (b) oversized REQUEST headers with a trailer block following:
#     no crash, no second dispatch, still exactly one 431.
# ============================================================
subtest 'oversized request headers followed by a trailer block: no crash, no second dispatch' => sub {
    my $invocations = 0;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        $invocations++;
    };

    my (@warnings, $conn, %response_headers, %closed);
    my $died;
    {
        local $SIG{__WARN__} = sub { push @warnings, $_[0] };

        my ($c, $stream_io, $client_sock, $server) = create_h2c_connection(
            app                     => $app,
            h2_max_header_list_size => 200,
        );
        $conn = $c;

        my $client = create_client(
            on_header => sub {
                my ($sid, $name, $value) = @_;
                $response_headers{$name} = $value;
                return 0;
            },
            on_stream_close => sub {
                my ($sid, $error_code) = @_;
                $closed{$sid} = $error_code;
                return 0;
            },
        );
        h2c_handshake($client, $client_sock);

        my @chunks = ('request-body-data');
        eval {
            my $stream_id = $client->submit_request(
                method    => 'POST',
                path      => '/oversized-with-trailer',
                scheme    => 'http',
                authority => 'localhost',
                headers   => [['x-big', 'A' x 500]],
                body      => sub {
                    my ($sid, $max_length) = @_;
                    my $chunk = shift @chunks;
                    return undef unless defined $chunk;
                    $client->submit_trailer($sid, headers => [['x-checksum', 'abc']]);
                    # eof=1, no_end_stream=1: reserve END_STREAM for the
                    # trailer HEADERS block, not this DATA frame.
                    return ($chunk, 1, 1);
                },
            );
            $client_sock->syswrite($client->mem_send);
            exchange_frames($client, $client_sock, 30);
            1;
        } or $died = $@;

        $stream_io->close_now;
        $loop->remove($server);
    }

    ok( !$died, 'no crash while processing oversized headers + a trailing trailer block' )
        or diag($died);

    is( $response_headers{':status'}, 431, 'still gets the 431 for the oversized request' );
    is( $invocations, 0, 'application was never dispatched (no second dispatch either)' );
    is( scalar keys %{ $conn->{h2_streams} }, 0, 'no leaked stream state' );
    is( scalar(@warnings), 0, 'no warnings logged' ) or diag(@warnings);
};

# ============================================================
# (c) regression: TRAILER-block overflow on an otherwise normal request
#     keeps its existing behavior (pending block deleted, stream reset) --
#     this file's fix is the REQUEST-block path only.
# ============================================================
subtest 'oversized TRAILER block still resets the stream (unchanged behavior)' => sub {
    my $invocations = 0;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        $invocations++;
        my $e = await $receive->();
        # Body never completes (trailer overflow tears the stream down
        # before END_STREAM), so this receive() simply hangs until the
        # stream is reset out from under it; nothing to assert here beyond
        # having been invoked once.
    };

    my ($conn, %closed, @frames);
    my ($c, $stream_io, $client_sock, $server) = create_h2c_connection(app => $app,
        h2_max_header_list_size => 300);
    $conn = $c;

    my $client = create_client(
        on_frame_recv   => sub { push @frames, { %{$_[0]} }; return 0 },
        on_stream_close => sub {
            my ($sid, $error_code) = @_;
            $closed{$sid} = $error_code;
            return 0;
        },
    );
    h2c_handshake($client, $client_sock);

    # Small request headers -- well within the 300-byte budget, so this
    # dispatches normally.
    my @chunks = ('body');
    my $stream_id = $client->submit_request(
        method    => 'POST',
        path      => '/trailer-overflow',
        scheme    => 'http',
        authority => 'localhost',
        headers   => [],
        body      => sub {
            my ($sid, $max_length) = @_;
            my $chunk = shift @chunks;
            return undef unless defined $chunk;
            # Not eof: leave the stream open for the raw oversized trailer
            # frame below (mirrors t/http2/34-request-trailers.t).
            return ($chunk, 0);
        },
    );
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 10);

    is( $invocations, 1, 'the normal-sized request dispatched before the oversized trailer arrived' );

    # Several trailer fields whose combined RFC 7541 accounting (each
    # value kept under 128 bytes so the literal-string HPACK length prefix
    # here -- a single byte, no continuation -- stays correct) pushes well
    # over the 300-byte budget.
    $client_sock->syswrite(raw_headers_frame(
        $stream_id,
        [ map { ["x-trailer-$_", 'B' x 100] } (1..4) ],
        end_stream => 1,
    ));
    exchange_frames($client, $client_sock, 10);

    $stream_io->close_now;
    $loop->remove($server);

    ok( (grep { $_->{type} == 3 } @frames),   # RFC 9113 section 6.4: RST_STREAM = 0x3
        'client observes an RST_STREAM for the oversized trailer block (unchanged behavior)' );
    ok( !(grep { $_->{type} == 1 } @frames),  # RFC 9113 section 6.2: HEADERS = 0x1
        'no response HEADERS (e.g. a 431) is synthesized for a trailer overflow' );
    is( scalar keys %{ $conn->{h2_streams} }, 0, 'no leaked stream state' );
};

done_testing;
