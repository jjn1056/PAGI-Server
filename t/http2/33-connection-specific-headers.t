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
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.008+ required)');
}

# ============================================================
# Test: HTTP/2 strips connection-specific response headers (design 13.3)
# ============================================================
# RFC 9113 section 8.2.2 forbids connection-specific header fields on
# HTTP/2: connection, keep-alive, proxy-connection, transfer-encoding, and
# upgrade must never appear, and te must not appear with any value other
# than 'trailers'. The Phase 5 final review live-reproduced what happens
# when an app violates this on any h2 response path: the response is
# destroyed at the framing layer -- the client receives only the :status
# pseudo-header, with no body.

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;

my $loop = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

sub create_test_server {
    my (%args) = @_;
    my $server = PAGI::Server->new(
        app => $args{app} // sub { }, host => '127.0.0.1', port => 0,
        quiet => 1, http2 => 1, %args,
    );
    $loop->add($server);
    return $server;
}

sub create_h2c_connection {
    my (%overrides) = @_;
    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $sock_a->blocking(0);
    $sock_b->blocking(0);
    my $app = $overrides{app} // sub { };
    my $server = $overrides{server} // create_test_server(app => $app, %overrides);
    my $stream = IO::Async::Stream->new(
        read_handle => $sock_a, write_handle => $sock_a, on_read => sub { 0 },
    );
    my $conn = PAGI::Server::Connection->new(
        stream => $stream, app => $app, protocol => $protocol, server => $server,
        h2_protocol   => $server->{http2_protocol},
        h2c_enabled   => $server->{h2c_enabled},
        max_body_size => $overrides{max_body_size} // $server->{max_body_size},
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
            on_begin_headers   => sub { 0 },
            on_header          => $overrides{on_header}          // sub { 0 },
            on_frame_recv      => sub { 0 },
            on_data_chunk_recv => $overrides{on_data_chunk_recv} // sub { 0 },
            on_stream_close    => $overrides{on_stream_close}    // sub { 0 },
        },
    );
}

sub pump {
    my ($client, $client_sock, $cond) = @_;
    for (1 .. 200) {
        $loop->loop_once(0.02);
        my $buf = '';
        $client_sock->sysread($buf, 65536);
        $client->mem_recv($buf) if length($buf);
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length($out);
        last if $cond && $cond->();
    }
}

# The six RFC 9113 section 8.2.2 forbidden names, each with a value that
# would (pre-fix) corrupt the response if handed straight to nghttp2. 'te'
# carries a non-'trailers' value here, so it is stripped like the rest.
my @FORBIDDEN_HEADERS = (
    ['connection',       'keep-alive'],
    ['keep-alive',       'timeout=5'],
    ['proxy-connection', 'keep-alive'],
    ['transfer-encoding', 'chunked'],
    ['upgrade',          'h2c'],
    ['te',                'gzip'],
);

subtest 'HTTP/2 http.response.start strips connection-specific headers' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();
        await $send->({
            type => 'http.response.start', status => 200,
            headers => [ @FORBIDDEN_HEADERS, ['content-type', 'text/plain'] ],
        });
        await $send->({ type => 'http.response.body', body => 'Hello World', more => 0 });
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(app => $app);

    my %headers;
    my $body = '';
    my $stream_closed = 0;
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };
    my $client = create_client(
        on_header => sub { my ($sid, $n, $v) = @_; $headers{lc $n} = $v; return 0 },
        on_data_chunk_recv => sub { my ($sid, $data) = @_; $body .= $data; return 0 },
        on_stream_close => sub { $stream_closed = 1; return 0 },
    );

    $client->send_connection_preface;
    $client_sock->syswrite($client->mem_send);
    pump($client, $client_sock);

    $client->submit_request(
        method => 'GET', path => '/', scheme => 'http', authority => 'localhost',
    );
    $client_sock->syswrite($client->mem_send);
    pump($client, $client_sock, sub { $stream_closed });

    ok($stream_closed, 'request completed');
    is($headers{':status'}, '200', 'status arrived');
    is($body, 'Hello World', 'full response body arrived intact');
    is($headers{'content-type'}, 'text/plain', 'ordinary app header preserved');

    for my $forbidden (@FORBIDDEN_HEADERS) {
        my ($name) = @$forbidden;
        ok(!exists $headers{lc $name}, "'$name' stripped from the response");
    }

    my @strip_warnings = grep { /connection-specific header/ } @warnings;
    is(scalar(@strip_warnings), scalar(@FORBIDDEN_HEADERS),
        'one warning per stripped header name');
    for my $forbidden (@FORBIDDEN_HEADERS) {
        my ($name) = @$forbidden;
        ok((grep { /'\Q$name\E' stripped from HTTP\/2 response \(RFC 9113\)/ } @strip_warnings),
            "warning names '$name'");
    }

    $stream_io->close_now;
    $loop->remove($server);
};

subtest "te: 'trailers' is preserved, not stripped" => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();
        await $send->({
            type => 'http.response.start', status => 200,
            headers => [['te', 'Trailers']],
        });
        await $send->({ type => 'http.response.body', body => 'ok', more => 0 });
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(app => $app);

    my %headers;
    my $stream_closed = 0;
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };
    my $client = create_client(
        on_header => sub { my ($sid, $n, $v) = @_; $headers{lc $n} = $v; return 0 },
        on_stream_close => sub { $stream_closed = 1; return 0 },
    );

    $client->send_connection_preface;
    $client_sock->syswrite($client->mem_send);
    pump($client, $client_sock);

    $client->submit_request(
        method => 'GET', path => '/', scheme => 'http', authority => 'localhost',
    );
    $client_sock->syswrite($client->mem_send);
    pump($client, $client_sock, sub { $stream_closed });

    ok($stream_closed, 'request completed');
    is($headers{':status'}, '200', 'status arrived');
    is($headers{'te'}, 'Trailers', "te: trailers passed through (case-insensitive value match)");
    ok(!(grep { /connection-specific header/ } @warnings), 'no strip warning for te: trailers');

    $stream_io->close_now;
    $loop->remove($server);
};

subtest "te: 'trailers' with surrounding OWS is not treated as a violation" => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();
        await $send->({
            type => 'http.response.start', status => 200,
            headers => [['te', "  trailers  "], ['x-marker', 'ok']],
        });
        await $send->({ type => 'http.response.body', body => 'unharmed', more => 0 });
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(app => $app);

    my %headers;
    my $body = '';
    my $stream_closed = 0;
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };
    my $client = create_client(
        on_header => sub { my ($sid, $n, $v) = @_; $headers{lc $n} = $v; return 0 },
        on_data_chunk_recv => sub { my ($sid, $data) = @_; $body .= $data; return 0 },
        on_stream_close => sub { $stream_closed = 1; return 0 },
    );

    $client->send_connection_preface;
    $client_sock->syswrite($client->mem_send);
    pump($client, $client_sock);

    $client->submit_request(
        method => 'GET', path => '/', scheme => 'http', authority => 'localhost',
    );
    $client_sock->syswrite($client->mem_send);
    pump($client, $client_sock, sub { $stream_closed });

    ok($stream_closed, 'request completed');
    is($headers{':status'}, '200', 'status arrived');
    is($body, 'unharmed', 'response is not corrupted by an OWS-padded te: trailers');
    is($headers{'x-marker'}, 'ok', 'a sibling app header still arrives intact');
    ok(!(grep { /connection-specific header/ } @warnings),
        "no strip warning for te with OWS around trailers -- the compare correctly recognizes the token");

    # nghttp2 itself silently drops header field values carrying leading or
    # trailing whitespace (RFC 9113 8.2.1 forbids OWS in field values) before
    # the frame reaches the wire -- confirmed by direct inspection, this is
    # independent of and unaffected by the connection-specific-header strip
    # under test here, so the 'te' header itself is not observed by the
    # client even though our code never flagged it as a violation.
    ok(!exists $headers{'te'}, "the 'te' header itself does not survive nghttp2's own OWS field-value rule");

    $stream_io->close_now;
    $loop->remove($server);
};

subtest "te: a compound value ('trailers, gzip') is still stripped" => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();
        await $send->({
            type => 'http.response.start', status => 200,
            headers => [['te', 'trailers, gzip'], ['content-type', 'text/plain']],
        });
        await $send->({ type => 'http.response.body', body => 'ok', more => 0 });
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(app => $app);

    my %headers;
    my $stream_closed = 0;
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };
    my $client = create_client(
        on_header => sub { my ($sid, $n, $v) = @_; $headers{lc $n} = $v; return 0 },
        on_stream_close => sub { $stream_closed = 1; return 0 },
    );

    $client->send_connection_preface;
    $client_sock->syswrite($client->mem_send);
    pump($client, $client_sock);

    $client->submit_request(
        method => 'GET', path => '/', scheme => 'http', authority => 'localhost',
    );
    $client_sock->syswrite($client->mem_send);
    pump($client, $client_sock, sub { $stream_closed });

    ok($stream_closed, 'request completed');
    is($headers{':status'}, '200', 'status arrived');
    ok(!exists $headers{'te'}, "compound te value is stripped, not just the bare token");
    is(scalar(grep { /connection-specific header/ } @warnings), 1, 'one strip warning for the compound te value');

    $stream_io->close_now;
    $loop->remove($server);
};

subtest 'HTTP/2 sse.start strips connection-specific headers' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();
        await $send->({
            type => 'sse.start', status => 200,
            headers => [['connection', 'keep-alive'], ['transfer-encoding', 'chunked']],
        });
        await $send->({ type => 'sse.send', data => 'hello' });
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(app => $app);

    my %headers;
    my $body = '';
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };
    my $client = create_client(
        on_header          => sub { my ($sid, $n, $v) = @_; $headers{lc $n} = $v; return 0 },
        on_data_chunk_recv => sub { my ($sid, $data) = @_; $body .= $data; return 0 },
    );

    $client->send_connection_preface;
    $client_sock->syswrite($client->mem_send);
    pump($client, $client_sock);

    $client->submit_request(
        method => 'GET', path => '/events', scheme => 'http', authority => 'localhost',
        headers => [['accept', 'text/event-stream']],
    );
    $client_sock->syswrite($client->mem_send);
    pump($client, $client_sock, sub { length($body) });

    is($headers{':status'}, '200', 'status arrived');
    like($body, qr/data: hello\n/, 'SSE event body arrived intact');
    ok(!exists $headers{'connection'}, "'connection' stripped from the SSE response");
    ok(!exists $headers{'transfer-encoding'}, "'transfer-encoding' stripped from the SSE response");

    my @strip_warnings = grep { /connection-specific header/ } @warnings;
    is(scalar(@strip_warnings), 2, 'one warning per stripped header name');

    $stream_io->close_now;
    $loop->remove($server);
};

subtest 'HTTP/2 websocket.accept strips connection-specific headers' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();  # websocket.connect
        await $send->({
            type => 'websocket.accept',
            headers => [['connection', 'keep-alive'], ['transfer-encoding', 'chunked'], ['x-custom', 'ok']],
        });
        return;
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(app => $app);

    my %headers;
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };
    my $client = create_client(
        on_header => sub { my ($sid, $n, $v) = @_; $headers{lc $n} = $v; return 0 },
    );

    $client->send_connection_preface;
    $client_sock->syswrite($client->mem_send);
    pump($client, $client_sock);

    $client->submit_request(
        method => 'CONNECT', path => '/ws/accept-test', scheme => 'https', authority => 'localhost',
        headers => [[':protocol', 'websocket'], ['sec-websocket-version', '13']],
        body    => sub { return undef },  # streaming: keep open
    );
    $client_sock->syswrite($client->mem_send);
    pump($client, $client_sock, sub { defined $headers{':status'} });

    is($headers{':status'}, '200', 'websocket.accept succeeds with a 200 response');
    is($headers{'x-custom'}, 'ok', 'ordinary app header preserved on accept');
    ok(!exists $headers{'connection'}, "'connection' stripped from the accept response");
    ok(!exists $headers{'transfer-encoding'}, "'transfer-encoding' stripped from the accept response");

    my @strip_warnings = grep { /connection-specific header/ } @warnings;
    is(scalar(@strip_warnings), 2, 'one warning per stripped header name');

    $stream_io->close_now;
    $loop->remove($server);
};

subtest 'HTTP/2 WebSocket denial response strips connection-specific headers' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();  # websocket.connect
        await $send->({
            type => 'websocket.http.response.start', status => 401,
            headers => [['connection', 'keep-alive'], ['transfer-encoding', 'chunked'], ['x-deny', 'auth']],
        });
        await $send->({ type => 'websocket.http.response.body', body => 'nope' });
        return;
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(app => $app);

    my %headers;
    my $body = '';
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };
    my $client = create_client(
        on_header          => sub { my ($sid, $n, $v) = @_; $headers{lc $n} = $v; return 0 },
        on_data_chunk_recv => sub { my ($sid, $data) = @_; $body .= $data; return 0 },
    );

    $client->send_connection_preface;
    $client_sock->syswrite($client->mem_send);
    pump($client, $client_sock);

    $client->submit_request(
        method => 'CONNECT', path => '/ws/test', scheme => 'https', authority => 'localhost',
        headers => [[':protocol', 'websocket'], ['sec-websocket-version', '13']],
        body    => sub { return undef },  # streaming: keep open
    );
    $client_sock->syswrite($client->mem_send);
    pump($client, $client_sock, sub { defined $headers{':status'} && $headers{':status'} eq '401' && length($body) });

    is($headers{':status'}, '401', 'custom denial status used');
    is($body, 'nope', 'full denial body arrived intact');
    is($headers{'x-deny'}, 'auth', 'ordinary app header preserved');
    ok(!exists $headers{'connection'}, "'connection' stripped from the denial response");
    ok(!exists $headers{'transfer-encoding'}, "'transfer-encoding' stripped from the denial response");

    my @strip_warnings = grep { /connection-specific header/ } @warnings;
    is(scalar(@strip_warnings), 2, 'one warning per stripped header name');

    $stream_io->close_now;
    $loop->remove($server);
};

subtest 'HTTP/2 SSE decline response strips connection-specific headers' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $send->({
            type => 'sse.http.response.start', status => 404,
            headers => [['connection', 'keep-alive'], ['transfer-encoding', 'chunked'], ['content-type', 'text/plain']],
        });
        await $send->({ type => 'sse.http.response.body', body => 'No such stream', more => 0 });
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(app => $app);

    my %headers;
    my $body = '';
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };
    my $client = create_client(
        on_header          => sub { my ($sid, $n, $v) = @_; $headers{lc $n} = $v; return 0 },
        on_data_chunk_recv => sub { my ($sid, $data) = @_; $body .= $data; return 0 },
    );

    $client->send_connection_preface;
    $client_sock->syswrite($client->mem_send);
    pump($client, $client_sock);

    $client->submit_request(
        method => 'GET', path => '/events', scheme => 'http', authority => 'localhost',
        headers => [['accept', 'text/event-stream']],
    );
    $client_sock->syswrite($client->mem_send);
    pump($client, $client_sock, sub { defined $headers{':status'} && length($body) });

    is($headers{':status'}, '404', 'SSE decline uses the app-declared status');
    is($body, 'No such stream', 'full decline body arrived intact');
    ok(!exists $headers{'connection'}, "'connection' stripped from the decline response");
    ok(!exists $headers{'transfer-encoding'}, "'transfer-encoding' stripped from the decline response");

    my @strip_warnings = grep { /connection-specific header/ } @warnings;
    is(scalar(@strip_warnings), 2, 'one warning per stripped header name');

    $stream_io->close_now;
    $loop->remove($server);
};

subtest 'HTTP/2 HEAD response strips connection-specific headers (shares the GET header list)' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $receive->();
        await $send->({
            type => 'http.response.start', status => 200,
            headers => [['connection', 'keep-alive'], ['transfer-encoding', 'chunked'], ['content-length', '11']],
        });
        await $send->({ type => 'http.response.body', body => 'Hello World', more => 0 });
    };

    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(app => $app);

    my %headers;
    my $body = '';
    my $stream_closed = 0;
    my @warnings;
    local $SIG{__WARN__} = sub { push @warnings, $_[0] };
    my $client = create_client(
        on_header          => sub { my ($sid, $n, $v) = @_; $headers{lc $n} = $v; return 0 },
        on_data_chunk_recv => sub { my ($sid, $data) = @_; $body .= $data; return 0 },
        on_stream_close    => sub { $stream_closed = 1; return 0 },
    );

    $client->send_connection_preface;
    $client_sock->syswrite($client->mem_send);
    pump($client, $client_sock);

    $client->submit_request(
        method => 'HEAD', path => '/', scheme => 'http', authority => 'localhost',
    );
    $client_sock->syswrite($client->mem_send);
    pump($client, $client_sock, sub { $stream_closed });

    ok($stream_closed, 'request completed');
    is($headers{':status'}, '200', 'status arrived');
    is($body, '', 'HEAD suppresses the body as usual');
    is($headers{'content-length'}, '11', 'Content-Length passes through untouched (HEAD contract)');
    ok(!exists $headers{'connection'}, "'connection' stripped from the HEAD response");
    ok(!exists $headers{'transfer-encoding'}, "'transfer-encoding' stripped from the HEAD response");

    my @strip_warnings = grep { /connection-specific header/ } @warnings;
    is(scalar(@strip_warnings), 2, 'one warning per stripped header name');

    $stream_io->close_now;
    $loop->remove($server);
};

done_testing;
