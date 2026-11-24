use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use Future;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../../lib";
use Socket qw(AF_UNIX SOCK_STREAM);
use Time::HiRes qw(time);

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';
BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.014+ required)');
}

# PAGI::Spec::Www 0.7, run_until_end, over HTTP/2: the end of an http or an
# sse scope reaches a pending call. The client resets its stream while the
# work waits; the call resolves with 0, and the work is already
# cancelled on the first line after the await. The contract itself is
# t/run-until-end-contract.t; this file checks only that the protocol
# delivers its end to the method.

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;
use Net::HTTP2::nghttp2::Session;

my $loop = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;
my @log;

sub create_h2_connection {
    my ($app) = @_;
    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0)
        or die "socketpair: $!";
    $sock_a->blocking(0);
    $sock_b->blocking(0);
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, quiet => 1, access_log => undef,
        http2 => 1, shutdown_timeout => 1, logger => sub { push @log, $_[0] },
    );
    $loop->add($server);
    my $stream = IO::Async::Stream->new(
        read_handle => $sock_a, write_handle => $sock_a, on_read => sub { 0 },
    );
    my $conn = PAGI::Server::Connection->new(
        stream => $stream, app => $app, protocol => $protocol, server => $server,
        h2_protocol => $server->{http2_protocol}, alpn_protocol => 'h2',
    );
    $server->add_child($stream);
    $conn->start;
    return ($sock_b, $server, $conn);   # the caller keeps the connection alive
}

sub stop_server {
    my ($server) = @_;
    $server->shutdown->get;
    $loop->remove($server);
}

sub complete_h2_handshake {
    my ($client, $client_sock) = @_;
    $loop->loop_once(0.1);
    my $server_settings = '';
    $client_sock->sysread($server_settings, 4096);
    $client->send_connection_preface;
    $client_sock->syswrite($client->mem_send);
    $loop->loop_once(0.1);
    $client->mem_recv($server_settings);
    $loop->loop_once(0.1);
    my $ack = '';
    $client_sock->sysread($ack, 4096);
    $client->mem_recv($ack) if length $ack;
    my $client_ack = $client->mem_send;
    $client_sock->syswrite($client_ack) if length $client_ack;
    $loop->loop_once(0.1);
    my $extra = '';
    $client_sock->sysread($extra, 4096);
    $client->mem_recv($extra) if length $extra;
}

sub open_stream {
    my ($client_sock, %request) = @_;
    my $client = Net::HTTP2::nghttp2::Session->new_client(callbacks => {
        map { $_ => sub { 0 } }
            qw(on_begin_headers on_header on_frame_recv on_data_chunk_recv on_stream_close)
    });
    complete_h2_handshake($client, $client_sock);
    my $stream_id = $client->submit_request(
        method => 'GET', path => '/', scheme => 'https', authority => 'localhost', %request,
    );
    $client_sock->syswrite($client->mem_send);
    return ($client, $stream_id);
}

sub reset_stream {
    my ($client, $client_sock, $stream_id) = @_;
    $client->submit_rst_stream($stream_id, 8);   # CANCEL
    $client_sock->syswrite($client->mem_send);
}

sub pump_until {
    my ($cond, $secs) = @_;
    my $deadline = time + ($secs // 10);
    $loop->loop_once(0.01) until $cond->() || time > $deadline;
    return $cond->();
}

for my $type ('http', 'sse') {
    subtest "$type: the client resets its stream while the work waits" => sub {
        my %obs;
        my $app = async sub {
            my ($scope, $receive, $send) = @_;
            die "unsupported scope type: $scope->{type}\n" unless $scope->{type} eq $type;
            my $conn = $scope->{'pagi.connection'};
            await $send->($type eq 'sse'
                ? { type => 'sse.start', status => 200 }
                : { type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] });
            $obs{started} = 1;
            my $work;
            my @r = await $conn->run_until_end(sub { $work = $loop->delay_future(after => 30) });
            $obs{seen} = { result => \@r, cancelled => $work->is_cancelled ? 1 : 0,
                           connected => $conn->is_connected, reason => $conn->disconnect_reason };
        };

        my ($client_sock, $server, $h2conn) = create_h2_connection($app);
        my ($client, $stream_id) = open_stream($client_sock,
            $type eq 'sse' ? (headers => [['accept', 'text/event-stream']]) : ());
        ok(pump_until(sub { $obs{started} }), 'the response started');
        reset_stream($client, $client_sock, $stream_id);

        ok(pump_until(sub { $obs{seen} }), 'the call returned');
        is($obs{seen}, { result => [0], cancelled => 1, connected => 0, reason => 'client_closed' },
            '0; the work cancelled on the first line after await; the end reported');
        stop_server($server);
    };
}

is([ grep { !/\A(?:listening|Using default max_connections)/ } map { $_->{message} // '' } @log ], [],
    'nothing logged');
done_testing;
