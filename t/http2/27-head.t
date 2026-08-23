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
# Test: HTTP/2 HEAD suppression (PAGI Www.pod "HEAD Requests")
# ============================================================
# The app answers HEAD exactly like GET; the server must suppress the body:
# no DATA frames, Content-Length passes through untouched, file/fh is never
# opened or statted, and trailers are accepted and discarded. GET on the same
# handler is unaffected.

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;

my $loop = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# ============================================================
# Helpers (lifted verbatim from t/http2/26-mandatory-validation.t)
# ============================================================

sub create_test_server {
    my (%args) = @_;
    my $server = PAGI::Server->new(
        app   => $args{app} // sub { },
        host  => '127.0.0.1',
        port  => 0,
        quiet => 1,
        http2 => 1,
        validate_events => 0,
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
# Request helper (get_h2-style, but with a selectable :method)
# ============================================================

sub h2_request {
    my ($method, $path, %opts) = @_;
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
        method    => $method,
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

sub head_response {
    my ($path, %opts) = @_;
    return h2_request('HEAD', $path, app => $opts{app} // $Head::app);
}

sub get_response {
    my ($path, %opts) = @_;
    return h2_request('GET', $path, app => $opts{app} // $Head::app);
}

# ============================================================
# App: every path answers like a GET would; the server must suppress
# the body when the request method is HEAD.
# ============================================================

package Head;
our $FILE_SEND_ERROR;
our $TRAILERS_ERROR;
our $FH_SEND_ERROR;
our $FH_TELL_AFTER;
package main;

$Head::app = async sub {
    my ($scope, $receive, $send) = @_;
    die "unsupported scope" if $scope->{type} ne 'http';
    while (1) {
        my $e = await $receive->();
        last if $e->{type} ne 'http.request' || !$e->{more};
    }
    my $path = $scope->{path} // '/';

    if ($path eq '/plain' || $path eq '/ok-get') {
        await $send->({ type => 'http.response.start', status => 200,
                         headers => [['content-type','text/plain'], ['content-length','5']] });
        await $send->({ type => 'http.response.body', body => 'hello', more => 0 });
        return;
    }
    if ($path eq '/streaming') {
        await $send->({ type => 'http.response.start', status => 200,
                         headers => [['content-type','text/plain']] });
        await $send->({ type => 'http.response.body', body => 'he', more => 1 });
        await $send->({ type => 'http.response.body', body => 'llo', more => 0 });
        return;
    }
    if ($path eq '/file') {
        await $send->({ type => 'http.response.start', status => 200,
                         headers => [['content-type','text/plain']] });
        my ($fh, $tmpfile) = tempfile(UNLINK => 0);
        print $fh 'hello';
        close $fh;
        # Unlink BEFORE the send: if the server ever opened/statted this file
        # for a HEAD request, the send below would fail with ENOENT.
        unlink $tmpfile;
        $Head::FILE_SEND_ERROR = do {
            local $@;
            eval { await $send->({ type => 'http.response.body', file => $tmpfile }) };
            $@;
        };
        return;
    }
    if ($path eq '/fh') {
        await $send->({ type => 'http.response.start', status => 200,
                         headers => [['content-type','text/plain']] });
        open my $fh, '<', \"hello" or die "Cannot open scalar handle: $!";
        # Nonzero offset: if the server ever seeked/read this handle for a
        # HEAD request, $fh's position would move off 0 below.
        $Head::FH_SEND_ERROR = do {
            local $@;
            eval { await $send->({ type => 'http.response.body', fh => $fh, offset => 3 }) };
            $@;
        };
        $Head::FH_TELL_AFTER = tell($fh);
        return;
    }
    if ($path eq '/trailers') {
        await $send->({ type => 'http.response.start', status => 200, trailers => 1,
                         headers => [['content-type','text/plain']] });
        await $send->({ type => 'http.response.body', body => '', more => 0 });
        $Head::TRAILERS_ERROR = do {
            local $@;
            eval { await $send->({ type => 'http.response.trailers', headers => [['x-t','1']] }) };
            $@;
        };
        return;
    }
    die "unknown path $path";
};

# ============================================================
# HEAD suppression
# ============================================================

is( head_response('/plain')->{body}, '', 'HEAD /plain: no DATA' );
is( head_response('/plain')->{headers}{'content-length'}, '5',
    'app Content-Length passes through untouched' );
is( head_response('/streaming')->{body}, '', 'HEAD streaming: all chunks discarded' );
is( head_response('/file')->{body}, '', 'HEAD file: no body' );
ok( !$Head::FILE_SEND_ERROR, 'file body event on HEAD resolves (file was never opened/statted)' );

is( head_response('/fh')->{body}, '', 'HEAD fh: no body' );
ok( !$Head::FH_SEND_ERROR, 'fh body event on HEAD resolves' );
is( $Head::FH_TELL_AFTER, 0, 'fh was never seeked/read for a HEAD request' );

is( head_response('/trailers')->{body}, '', 'HEAD trailers: no body' );
ok( !$Head::TRAILERS_ERROR, 'trailers event on HEAD is accepted and discarded' );

# control: GET still gets the body
is( get_response('/ok-get')->{body}, 'hello', 'GET unaffected' );

done_testing;
