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
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.011+ required)');
}

# ============================================================
# Test: HTTP/2 suppression for 204 and 304 (PAGI Www.pod "Statuses Without Content")
# ============================================================
# A 204 or 304 response cannot contain content (RFC 9110 S6.4.1; RFC 9113
# S8.1.1). The app may send body events; the server must suppress them as for
# HEAD: no DATA frames, the stream ends cleanly (no RST_STREAM), headers pass
# through (a 304 keeps its Content-Length), file/fh is never opened, trailers
# are accepted and discarded, and every send resolves.

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;

my $loop = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# ============================================================
# Helpers (lifted verbatim from t/http2/27-head.t)
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

sub h2_get {
    my ($path) = @_;
    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(app => $NoBody::app);
    my (%headers, $close_code);
    my $body = '';
    my $client = create_client(
        on_header          => sub { my ($sid, $n, $v) = @_; $headers{$n} = $v; return 0 },
        on_data_chunk_recv => sub { my ($sid, $d)    = @_; $body .= $d;         return 0 },
        on_stream_close    => sub { my ($sid, $code) = @_; $close_code = $code if $sid == 1; return 0 },
    );
    h2c_handshake($client, $client_sock);
    $client->submit_request(method => 'GET', path => $path, scheme => 'http', authority => 'localhost', headers => []);
    $client_sock->syswrite($client->mem_send);
    exchange_frames($client, $client_sock, 20);
    $stream_io->close_now;
    $loop->remove($server);
    return { headers => \%headers, body => $body, close_code => $close_code };
}

# ============================================================
# App: answers with the status in the path, and a body anyway.
# ============================================================

package NoBody;
our %ERROR;
our $FH_TELL_AFTER;
package main;

$NoBody::app = async sub {
    my ($scope, $receive, $send) = @_;
    die "unsupported scope" if $scope->{type} ne 'http';
    while (1) {
        my $e = await $receive->();
        last if $e->{type} ne 'http.request' || !$e->{more};
    }
    my ($status, $kind) = ($scope->{path} // '/') =~ m{\A/(\d+)/(\w+)\z} or die "unknown path";
    my $try = async sub { my ($event) = @_; local $@; eval { await $send->($event) }; $ERROR{"$status/$kind"} .= $@ // '' };
    if ($kind eq 'body') {
        await $send->({ type => 'http.response.start', status => $status,
                        headers => [['etag', '"v1"'], ($status == 304 ? ['content-length', '5'] : ())] });
        await $try->({ type => 'http.response.body', body => 'he', more => 1 });
        await $try->({ type => 'http.response.body', body => 'llo', more => 0 });
        return;
    }
    if ($kind eq 'file') {
        await $send->({ type => 'http.response.start', status => $status, headers => [] });
        my ($fh, $tmpfile) = tempfile(UNLINK => 0);
        print $fh 'hello';
        close $fh;
        unlink $tmpfile;   # if the server opened or statted it, the send fails
        await $try->({ type => 'http.response.body', file => $tmpfile });
        return;
    }
    if ($kind eq 'fh') {
        await $send->({ type => 'http.response.start', status => $status, headers => [] });
        open my $fh, '<', \"hello" or die "Cannot open scalar handle: $!";
        await $try->({ type => 'http.response.body', fh => $fh, offset => 3 });
        $NoBody::FH_TELL_AFTER = tell($fh);
        return;
    }
    if ($kind eq 'trailers') {
        await $send->({ type => 'http.response.start', status => $status, trailers => 1, headers => [] });
        await $try->({ type => 'http.response.body', body => 'hello', more => 0 });
        await $try->({ type => 'http.response.trailers', headers => [['x-t', '1']] });
        return;
    }
    die "unknown kind $kind";
};

# ============================================================
# Suppression
# ============================================================

for my $status (204, 304) {
    my $res = h2_get("/$status/body");
    is( $res->{headers}{':status'}, $status, "$status: the status arrives" );
    is( $res->{body}, '', "$status: no DATA" );
    is( $res->{close_code}, 0, "$status: the stream ends cleanly (no RST_STREAM)" );
    is( $NoBody::ERROR{"$status/body"} // '', '', "$status: every body send resolves" );
    is( $res->{headers}{etag}, '"v1"', "$status: headers pass through" );

    is( h2_get("/$status/file")->{body}, '', "$status file: no body" );
    is( $NoBody::ERROR{"$status/file"} // '', '', "$status file: never opened or statted" );

    is( h2_get("/$status/fh")->{body}, '', "$status fh: no body" );
    is( $NoBody::FH_TELL_AFTER, 0, "$status fh: never seeked or read" );

    my $trailed = h2_get("/$status/trailers");
    is( $trailed->{body}, '', "$status trailers: no body" );
    is( $trailed->{headers}{'x-t'}, undef, "$status trailers: discarded" );
    is( $NoBody::ERROR{"$status/trailers"} // '', '', "$status trailers: accepted" );
}
is( h2_get('/304/body')->{headers}{'content-length'}, '5', '304 keeps the Content-Length its 200 would have had' );

# control: a 200 still gets its body
is( h2_get('/200/body')->{body}, 'hello', '200 unaffected' );

done_testing;
