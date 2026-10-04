use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use Future::AsyncAwait;
use Time::HiRes qw(time);
use FindBin;
use lib "$FindBin::Bin/../../lib";
use Socket qw(AF_UNIX SOCK_STREAM);

# The connection's idle timeout covers an HTTP/2 connection with no open
# stream: while any stream is open it is paused, so a handler slower than the
# timeout still answers, and it starts over when the last stream closes.

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.011+ required)');
}

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use Net::HTTP2::nghttp2::Session;

my $loop     = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

my $app = async sub {
    my ($scope, $receive, $send) = @_;
    return unless $scope->{type} eq 'http';
    await $receive->();
    await $loop->delay_future(after => 0.8) if $scope->{path} eq '/slow';
    await $send->({ type => 'http.response.start', status => 200,
        headers => [['content-type', 'text/plain']] });
    await $send->({ type => 'http.response.body', body => "done $scope->{path}" });
};

my $server = PAGI::Server->new(app => $app, host => '127.0.0.1', port => 0,
    quiet => 1, http2 => 1);
$loop->add($server);

sub h2_connection {
    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $sock_a->blocking(0);
    $sock_b->blocking(0);
    my $stream = IO::Async::Stream->new(
        read_handle => $sock_a, write_handle => $sock_a, on_read => sub { 0 });
    my $conn = PAGI::Server::Connection->new(
        stream => $stream, app => $app, protocol => $protocol, server => $server,
        h2_protocol => $server->{http2_protocol}, alpn_protocol => 'h2',
        timeout => 0.4,
    );
    $server->add_child($stream);
    $conn->start;
    return ($conn, $stream, $sock_b);
}

# Exchanges bytes between the client session and the socket for $seconds,
# or until $done->() is true. Returns 1 if the server closed the socket.
sub pump {
    my ($client, $sock, $seconds, $done) = @_;
    my $deadline = time + $seconds;
    while (time < $deadline) {
        my $out = $client->mem_send;
        $sock->syswrite($out) if length $out;
        $loop->loop_once(0.02);
        my $n = $sock->sysread(my $data, 65536);
        return 1 if defined $n && $n == 0;
        $client->mem_recv($data) if $n;
        return 0 if $done && $done->();
    }
    return 0;
}

sub client {
    my ($bodies) = @_;
    return Net::HTTP2::nghttp2::Session->new_client(callbacks => {
        on_begin_headers   => sub { 0 },
        on_header          => sub { 0 },
        on_frame_recv      => sub { 0 },
        on_data_chunk_recv => sub { my ($sid, $data) = @_; $bodies->{$sid} .= $data; 0 },
        on_stream_close    => sub { 0 },
    });
}

subtest 'a stream whose handler outlasts the idle timeout still answers' => sub {
    my ($conn, $stream, $sock) = h2_connection();
    my %bodies;
    my $client = client(\%bodies);
    $client->send_connection_preface;
    my $sid = $client->submit_request(method => 'GET', path => '/slow',
        scheme => 'http', authority => 'localhost');
    my $closed = pump($client, $sock, 3, sub { ($bodies{$sid} // '') eq 'done /slow' });
    ok(!$closed, 'the connection stays open while the stream is handled');
    is($bodies{$sid}, 'done /slow', 'the response arrives');
    $stream->close_now;
};

subtest 'after the last stream closes the idle timeout starts over' => sub {
    my ($conn, $stream, $sock) = h2_connection();
    my %bodies;
    my $client = client(\%bodies);
    $client->send_connection_preface;
    my $sid = $client->submit_request(method => 'GET', path => '/slow',
        scheme => 'http', authority => 'localhost');
    pump($client, $sock, 3, sub { ($bodies{$sid} // '') eq 'done /slow' });
    is($bodies{$sid}, 'done /slow', 'the response arrived first');
    my $t0 = time;
    my $closed = pump($client, $sock, 3);
    ok($closed, 'the idle connection is closed');
    cmp_ok(time - $t0, '<', 1.5, 'about one timeout after the response');
    $stream->close_now;
};

done_testing;
