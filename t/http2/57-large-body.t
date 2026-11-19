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
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.013+ required)');
}

# ============================================================
# Test: HTTP/2 bodies larger than one DATA frame arrive whole
# ============================================================
# A DATA frame carries at most SETTINGS_MAX_FRAME_SIZE bytes (16384 unless
# the client raises it; RFC 9113 S4.2), and flow control may allow fewer. A
# body the application sends in one event, terminal or not, must still reach
# the client whole, over as many frames as it takes, and the stream must end
# cleanly (RST_STREAM never, END_STREAM once).

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;

my $loop = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

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
    my $server = create_test_server(app => $app);
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

# Moves frames both ways until $done says so, or the round limit is hit.
sub exchange_until {
    my ($client, $client_sock, $done, $rounds) = @_;
    for (1 .. $rounds) {
        $loop->loop_once(0.01);
        my $buf = '';
        $client_sock->sysread($buf, 1 << 20);
        $client->mem_recv($buf) if length $buf;
        my $out = $client->mem_send;
        $client_sock->syswrite($out) if length $out;
        return 1 if $done->();
    }
    return 0;
}

# GET $path; returns the status, the body, the trailers, and how the stream closed.
sub h2_get {
    my ($path) = @_;
    my ($conn, $stream_io, $client_sock, $server) = create_h2c_connection(app => $Large::app);
    my (%headers, $close_code, $trailers_seen);
    my $body = '';
    require Net::HTTP2::nghttp2::Session;
    my $client = Net::HTTP2::nghttp2::Session->new_client(callbacks => {
        on_begin_headers   => sub { 0 },
        on_header          => sub { my ($sid, $n, $v) = @_; $headers{$n} = $v; return 0 },
        on_frame_recv      => sub { 0 },
        on_data_chunk_recv => sub { my ($sid, $d) = @_; $body .= $d; return 0 },
        on_stream_close    => sub { my ($sid, $code) = @_; $close_code = $code if $sid == 1; return 0 },
    });
    $client->send_connection_preface;
    $client_sock->syswrite($client->mem_send);
    exchange_until($client, $client_sock, sub { 0 }, 5);
    $client->submit_request(method => 'GET', path => $path, scheme => 'http', authority => 'localhost', headers => []);
    $client_sock->syswrite($client->mem_send);
    my $closed = exchange_until($client, $client_sock, sub { defined $close_code }, 5000);
    $stream_io->close_now;
    $loop->remove($server);
    return { closed => $closed, close_code => $close_code, status => $headers{':status'},
             length => length $body, intact => ($body eq 'x' x length $body) ? 1 : 0, headers => \%headers };
}

package Large;
our %RESOLVED;   # each path's sends all resolved
package main;

# /one/N: the body in one terminal event. /chunk/N: one non-terminal event of
# N bytes, then an empty terminal one. /trailers/N: as /one, with trailers.
$Large::app = async sub {
    my ($scope, $receive, $send) = @_;
    die "unsupported scope" if $scope->{type} ne 'http';
    my ($kind, $size) = ($scope->{path} // '') =~ m{\A/(\w+)/(\d+)\z} or die "unknown path";
    my $trailers = $kind eq 'trailers';
    await $send->({ type => 'http.response.start', status => 200,
                    headers => [['content-type', 'application/octet-stream']],
                    ($trailers ? (trailers => 1) : ()) });
    if ($kind eq 'chunk') {
        await $send->({ type => 'http.response.body', body => 'x' x $size, more => 1 });
        await $send->({ type => 'http.response.body', body => '', more => 0 });
    }
    else {
        await $send->({ type => 'http.response.body', body => 'x' x $size, more => 0 });
    }
    await $send->({ type => 'http.response.trailers', headers => [['x-done', 'yes']] }) if $trailers;
    $Large::RESOLVED{ $scope->{path} } = 1;
};

for my $size (16384, 16385, 65535, 70000, 1 << 20) {
    for my $kind (qw(one chunk trailers)) {
        my $path = "/$kind/$size";
        subtest "$kind, $size bytes" => sub {
            my $res = h2_get($path);
            ok $res->{closed}, 'the stream closed';
            is $res->{close_code}, 0, 'cleanly: NO_ERROR, no reset';
            is $res->{status}, 200, 'status 200';
            is $res->{length}, $size, 'every byte arrived';
            ok $res->{intact}, 'unchanged';
            is $res->{headers}{'x-done'}, 'yes', 'and the trailers after it' if $kind eq 'trailers';
            ok $Large::RESOLVED{$path}, 'every send resolved';
        };
    }
}

done_testing;
