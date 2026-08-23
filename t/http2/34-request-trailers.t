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
# Test: HTTP/2 received-HEADERS classification (request trailers)
# ============================================================
# A client that sends request trailers (a second HEADERS block on an
# established stream, carrying END_STREAM) used to destroy the in-flight
# request: on_begin_headers unconditionally reinitialized the per-stream
# accumulator on EVERY HEADERS frame (wiping client_end_stream), and
# on_frame_recv re-invoked on_request for that second block with an empty
# pseudo hash -- a second app dispatch on the same stream, and the
# original request's body_complete never fired (END_STREAM landed on the
# trailer HEADERS, not on DATA).
#
# The fix (protocol layer, HTTP2.pm): on_begin_headers now only
# reinitializes the stream's main accumulator for the FIRST HEADERS block;
# a later HEADERS block on an established stream accumulates into a
# {pending} sub-block instead. on_frame_recv classifies the committed
# block via $frame->{headers_category}: NGHTTP2_HCAT_REQUEST commits the
# request (as before); NGHTTP2_HCAT_HEADERS on an established stream is
# request trailers -- validated (no pseudo-headers allowed, RFC 9113
# section 8.1), then discarded (PAGI defines no request-trailer receive
# event yet), honoring END_STREAM via the same on_body(..., 1) path DATA
# uses.
#
# Connection.pm's _h2_on_request also gained a defensive second layer:
# refuse to overwrite an existing h2_streams entry (this should be
# unreachable after the protocol-layer fix -- it exists so a future
# protocol-layer regression can't silently destroy an in-flight request).

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;
use Net::HTTP2::nghttp2 ();

my $loop = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# ============================================================
# Helpers (lifted verbatim from t/http2/29-fullflush-cancel.t)
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

# ============================================================
# Request-with-trailer client helper
# ============================================================
# Sends a POST with one DATA chunk followed by a trailer HEADERS block
# (which carries END_STREAM) -- the client-side session API is symmetric:
# submit_trailer() plus the three-value data-provider return
# ($chunk, $eof, $no_end_stream) reserves END_STREAM for the trailing
# HEADERS block instead of the final DATA frame.

sub post_h2_with_trailer {
    my (%opts) = @_;
    my ($conn, $stream_io, $client_sock, $server) =
        create_h2c_connection(app => $opts{app}, server => $opts{server});

    my %headers;
    my $body = '';
    my $client = create_client(
        on_header          => sub { my ($sid, $n, $v) = @_; $headers{$n} = $v; return 0 },
        on_data_chunk_recv => sub { my ($sid, $d)    = @_; $body .= $d;         return 0 },
    );
    h2c_handshake($client, $client_sock);

    my @chunks = ($opts{request_body} // 'request-body-data');
    my $trailer_headers = $opts{trailer_headers} // [['x-checksum', 'abc']];
    my $stream_id = $client->submit_request(
        method    => 'POST',
        path      => $opts{path} // '/',
        scheme    => 'http',
        authority => 'localhost',
        headers   => $opts{headers} // [],
        body      => sub {
            my ($sid, $max_length) = @_;
            my $chunk = shift @chunks;
            return undef unless defined $chunk;
            $client->submit_trailer($sid, headers => $trailer_headers);
            # Last DATA chunk: eof=1 with no_end_stream=1 reserves END_STREAM
            # for the trailer HEADERS block instead of this DATA frame.
            return ($chunk, 1, 1);
        },
    );
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, $opts{rounds} // 20);

    $stream_io->close_now;
    $loop->remove($server);
    return {
        conn      => $conn,
        stream_id => $stream_id,
        headers   => \%headers,
        body      => $body,
    };
}

# ============================================================
# (a) request with body + trailer HEADERS(+END_STREAM):
#     single app invocation, pseudo intact, body delivered complete,
#     trailers discarded, response completes cleanly.
# ============================================================

my $invocations_a = 0;
my @received_a;
my ($seen_method_a, $seen_path_a);
my $trailer_app = async sub {
    my ($scope, $receive, $send) = @_;
    $invocations_a++;
    $seen_method_a = $scope->{method};
    $seen_path_a   = $scope->{path};
    while (1) {
        my $e = await $receive->();
        push @received_a, $e;
        last if $e->{type} ne 'http.request' || !$e->{more};
    }
    await $send->({ type => 'http.response.start', status => 200,
                     headers => [['content-type', 'text/plain']] });
    await $send->({ type => 'http.response.body', body => 'ok', more => 0 });
    return;
};

my $result_a = post_h2_with_trailer(path => '/with-trailer', app => $trailer_app);

is( $invocations_a, 1, 'trailer HEADERS does not cause a second app dispatch' );
is( $seen_method_a, 'POST', 'pseudo headers intact on the single dispatch' );
is( $seen_path_a, '/with-trailer', 'pseudo :path intact on the single dispatch' );

my $body_seen_a = join('', map { $_->{body} // '' }
    grep { $_->{type} eq 'http.request' } @received_a);
is( $body_seen_a, 'request-body-data', 'request body delivered complete despite trailers' );

my @request_events_a = grep { $_->{type} eq 'http.request' } @received_a;
ok( scalar(@request_events_a) > 0, 'at least one http.request receive event observed' );
is( $request_events_a[-1]{more}, 0,
    'final http.request receive event reports more => 0 (body_complete fired)' );

ok( !(grep { $_->{type} =~ /trailer/i } @received_a),
    'no request-trailer receive event delivered (PAGI defines none yet)' );

is( $result_a->{headers}{':status'}, 200, 'response status delivered' );
is( $result_a->{body}, 'ok', 'response completes cleanly after trailers' );

is( scalar keys %{ $result_a->{conn}{h2_streams} }, 0,
    'no leaked stream state after a trailer-bearing request' );

# ============================================================
# (b) pseudo-header in request trailers: malformed per RFC 9113 section
#     8.1. Net::HTTP2::nghttp2's own submit_trailer() refuses (with a
#     Perl-level croak) to build a trailer block containing a
#     pseudo-header name -- a conforming client cannot be made to send
#     one through the public API. A raw, hand-crafted HEADERS frame on
#     the wire is the only way to reach the server's own rejection path.
#
# nghttp2's OWN default HTTP-messaging validation (independent of PAGI's
# pseudo-header check added to on_frame_recv) rejects the pseudo-header
# field BEFORE PAGI's on_header callback ever sees it, and tears down the
# WHOLE CONNECTION (GOAWAY, frame type 7) rather than issuing a per-stream
# RST_STREAM (frame type 3) -- the client's own on_frame_recv is pinned on
# the wire below. PAGI's own TEMPORAL_CALLBACK_FAILURE check is legitimate
# defense-in-depth (it would fire if nghttp2's own validation were ever
# bypassed, disabled, or changed in a future version) but is unreachable
# via nghttp2's normal request path today -- this test pins the
# actually-observable behavior, not the defense-in-depth code path.
# ============================================================

sub hpack_literal_new_name {
    my ($name, $value) = @_;
    # RFC 7541 section 6.2.2: Literal Header Field without Indexing --
    # New Name. Fine for hand-built test fixtures (short ASCII strings,
    # no huffman, no dynamic table).
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

my $invocations_b = 0;
my @received_b;
my $pseudo_trailer_app = async sub {
    my ($scope, $receive, $send) = @_;
    $invocations_b++;
    while (1) {
        my $e = await $receive->();
        push @received_b, $e;
        last if $e->{type} ne 'http.request' || !$e->{more};
    }
    await $send->({ type => 'http.response.start', status => 200,
                     headers => [['content-type', 'text/plain']] });
    await $send->({ type => 'http.response.body', body => 'ok', more => 0 });
    return;
};

my (@warnings_b, $conn_b, $status_b, @frames_b);
{
    local $SIG{__WARN__} = sub { push @warnings_b, $_[0] };

    my ($conn, $stream_io, $client_sock, $server) =
        create_h2c_connection(app => $pseudo_trailer_app);

    my %headers_b;
    my $client = create_client(
        on_header     => sub { my ($sid, $n, $v) = @_; $headers_b{$n} = $v; return 0 },
        on_frame_recv => sub { push @frames_b, { %{$_[0]} }; return 0 },
    );
    h2c_handshake($client, $client_sock);

    my @chunks = ('request-body-data');
    my $stream_id = $client->submit_request(
        method    => 'POST',
        path      => '/bad-trailer',
        scheme    => 'http',
        authority => 'localhost',
        headers   => [],
        body      => sub {
            my ($sid, $max_length) = @_;
            my $chunk = shift @chunks;
            return undef unless defined $chunk;
            # Not eof: leave the stream open for the raw trailer frame below
            # (a real client streaming a trailer would do the same, via the
            # three-value no_end_stream return -- see post_h2_with_trailer).
            return ($chunk, 0);
        },
    );
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 5);

    $client_sock->syswrite(raw_headers_frame(
        $stream_id,
        [[':path', '/smuggled'], ['x-checksum', 'abc']],
        end_stream => 1,
    ));
    exchange_frames($client, $client_sock, 10);

    $conn_b   = $conn;
    $status_b = $headers_b{':status'};
    $stream_io->close_now;
    $loop->remove($server);
}

is( $invocations_b, 1,
    'the original request was already dispatched once, before the malformed trailer arrived' );
ok( !(grep { $_->{type} eq 'http.request' && !$_->{more} } @received_b),
    'the app never observes a clean body-complete for this stream' );
ok( (grep { $_->{type} eq 'http.disconnect' } @received_b),
    'the app observes http.disconnect instead' );
is( $status_b, undef, 'no response is delivered to the client for the malformed stream' );
ok( !(grep { /PAGI application error/ } @warnings_b),
    'no app-error log -- the malformed trailer is a protocol-level teardown, not an app fault' );
is( scalar keys %{ $conn_b->{h2_streams} }, 0,
    'stream state is not leaked after a malformed (pseudo-header) trailer block' );

# Wire-level pin for the comment above: nghttp2's own HTTP-messaging
# validation, not PAGI's TEMPORAL_CALLBACK_FAILURE check, is what actually
# tears the connection down for this malformed trailer -- a whole-connection
# GOAWAY, not a per-stream RST_STREAM.
ok( (grep { $_->{type} == Net::HTTP2::nghttp2::NGHTTP2_GOAWAY() } @frames_b),
    'client observes a GOAWAY frame (whole-connection teardown)' );
ok( !(grep { $_->{type} == 3 } @frames_b),   # RFC 9113 section 6.4: RST_STREAM = 0x3
    'client does not observe an RST_STREAM frame (not a per-stream reset)' );

# ============================================================
# (c) normal request without trailers: zero behavior change (regression).
# ============================================================

my $invocations_c = 0;
my $plain_app = async sub {
    my ($scope, $receive, $send) = @_;
    $invocations_c++;
    while (1) {
        my $e = await $receive->();
        last if $e->{type} ne 'http.request' || !$e->{more};
    }
    await $send->({ type => 'http.response.start', status => 200,
                     headers => [['content-type', 'text/plain']] });
    await $send->({ type => 'http.response.body', body => 'plain-ok', more => 0 });
    return;
};

sub get_h2_plain {
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

my (undef, $plain_body) = get_h2_plain('/plain', app => $plain_app);
is( $invocations_c, 1, 'a normal request (no trailers) still dispatches exactly once' );
is( $plain_body, 'plain-ok', 'a normal request (no trailers) still completes cleanly' );

# ============================================================
# (d) combined: client-sent request trailers AND a response that
#     declares and sends its own trailers, on the same stream -- request
#     and response trailers are independent features (received-HEADERS
#     classification vs. outbound http.response.trailers) and must not
#     interfere with each other: single dispatch, request body complete
#     via the trailer-borne END_STREAM, response terminal DATA withholds
#     END_STREAM, exactly one trailing HEADERS block carries it instead.
# ============================================================

my $combined_invocations = 0;
my @combined_received;
my $combined_app = async sub {
    my ($scope, $receive, $send) = @_;
    $combined_invocations++;
    while (1) {
        my $e = await $receive->();
        push @combined_received, $e;
        last if $e->{type} ne 'http.request' || !$e->{more};
    }
    await $send->({ type => 'http.response.start', status => 200, trailers => 1,
                     headers => [['content-type', 'text/plain']] });
    await $send->({ type => 'http.response.body', body => 'combined-ok', more => 0 });
    await $send->({ type => 'http.response.trailers',
                     headers => [['x-resp-trailer', 'yes']] });
    return;
};

subtest 'combined: request trailers AND response trailers on the same stream' => sub {
    my ($conn, $stream_io, $client_sock, $server) =
        create_h2c_connection(app => $combined_app);

    my (@current, @header_blocks, %closed);
    my $body = '';
    my $client = create_client(
        on_begin_headers => sub { @current = (); return 0 },
        on_header        => sub {
            my ($sid, $n, $v) = @_;
            push @current, [$n, $v];
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
            return 0;
        },
        on_data_chunk_recv => sub { my ($sid, $d) = @_; $body .= $d; return 0 },
        on_stream_close    => sub { my ($sid, $c) = @_; $closed{$sid} = $c; return 0 },
    );
    h2c_handshake($client, $client_sock);

    my @chunks = ('combined-request-body');
    my $stream_id = $client->submit_request(
        method    => 'POST',
        path      => '/combined-trailers',
        scheme    => 'http',
        authority => 'localhost',
        headers   => [],
        body      => sub {
            my ($sid, $max_length) = @_;
            my $chunk = shift @chunks;
            return undef unless defined $chunk;
            $client->submit_trailer($sid, headers => [['x-req-trailer', 'sent']]);
            # eof=1, no_end_stream=1: reserve END_STREAM for the client's
            # own trailer HEADERS block instead of this DATA frame.
            return ($chunk, 1, 1);
        },
    );
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 30);

    $stream_io->close_now;
    $loop->remove($server);

    is( $combined_invocations, 1, 'single dispatch despite trailers on both directions' );

    my $body_seen = join('', map { $_->{body} // '' }
        grep { $_->{type} eq 'http.request' } @combined_received);
    is( $body_seen, 'combined-request-body',
        'request body delivered complete -- END_STREAM rode the client trailer HEADERS' );

    my @request_events = grep { $_->{type} eq 'http.request' } @combined_received;
    is( $request_events[-1]{more}, 0, 'final http.request event reports more => 0' );
    ok( !(grep { $_->{type} =~ /trailer/i } @combined_received),
        'no request-trailer receive event delivered (PAGI defines none yet)' );

    is( $body, 'combined-ok', 'response body arrived in full' );

    my @response_trailer_blocks = grep {
        $_->{category} == Net::HTTP2::nghttp2::NGHTTP2_HCAT_HEADERS()
    } @header_blocks;
    is( scalar(@response_trailer_blocks), 1,
        'exactly one trailing HEADERS block (the response trailers)' );
    ok( ($response_trailer_blocks[0]{flags} & Net::HTTP2::nghttp2::NGHTTP2_FLAG_END_STREAM()),
        'response trailing HEADERS carries END_STREAM' );
    is( $response_trailer_blocks[0]{pairs}, [['x-resp-trailer', 'yes']],
        'response trailer field arrives' );

    is( $closed{$stream_id}, 0, 'stream closes cleanly (error code 0)' );

    is( scalar keys %{ $conn->{h2_streams} }, 0,
        'no leaked stream state after a combined request+response trailers exchange' );
};

done_testing;
