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
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.011+ required)');
}

# ============================================================
# Test: the write guard on an HTTP/2 connection
# ============================================================
# HTTP/2 frames go out through the same IO::Async::Stream writer as HTTP/1.1,
# so a die inside it must end only that connection -- logged, closed, the
# write not retried -- while the server goes on serving new connections.
# The fault is armed for the server's end of the socketpair only.

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;

our ($ARM_FD, $DIES) = (-1, 0);
{
    no warnings 'redefine';
    my $syswrite = \&IO::Handle::syswrite;
    *IO::Handle::syswrite = sub {
        if ($main::ARM_FD >= 0 && fileno($_[0]) == $main::ARM_FD) {
            $main::ARM_FD = -1; $main::DIES++; die "armed h2 write failure\n";
        }
        goto &$syswrite;
    };
}

my $loop = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;
$SIG{ALRM} = sub { die "t/http2/51 exceeded its time bound\n" };
alarm 120;

my $app = async sub {
    my ($scope, $receive, $send) = @_;
    return unless $scope->{type} eq 'http';
    await $send->({ type => 'http.response.start', status => 200,
                    headers => [['content-type', 'text/plain']] });
    await $send->({ type => 'http.response.body', body => "ok $scope->{path}", more => 0 });
};

my @log;
my $server = PAGI::Server->new(
    app => $app, host => '127.0.0.1', port => 0, quiet => 1, http2 => 1,
    access_log => undef, logger => sub { push @log, $_[0] },
);
$loop->add($server);

# ============================================================
# Helpers (lifted from t/http2/26-mandatory-validation.t)
# ============================================================

sub create_h2c_connection {
    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $sock_a->blocking(0);
    $sock_b->blocking(0);
    my $stream = IO::Async::Stream->new(read_handle => $sock_a, write_handle => $sock_a, on_read => sub { 0 });
    my $conn = PAGI::Server::Connection->new(
        stream => $stream, app => $app, protocol => $protocol, server => $server,
        h2_protocol => $server->{http2_protocol}, h2c_enabled => $server->{h2c_enabled},
    );
    $server->add_child($stream);
    $conn->start;
    return ($conn, $stream, $sock_a, $sock_b);
}

sub create_client {
    my (%cb) = @_;
    require Net::HTTP2::nghttp2::Session;
    return Net::HTTP2::nghttp2::Session->new_client(callbacks => {
        on_begin_headers   => sub { 0 },
        on_header          => $cb{on_header} // sub { 0 },
        on_frame_recv      => sub { 0 },
        on_data_chunk_recv => $cb{on_data_chunk_recv} // sub { 0 },
        on_stream_close    => sub { 0 },
    });
}

sub exchange_frames {
    my ($client, $client_sock, $rounds) = @_;
    my $eof = 0;
    for (1 .. ($rounds // 10)) {
        $loop->loop_once(0.1);
        my $n = sysread($client_sock, my $buf, 16384);
        $eof = 1 if defined $n && $n == 0;
        $client->mem_recv($buf) if $n;
        my $out = $client->mem_send;
        syswrite($client_sock, $out) if length $out;
    }
    return $eof;
}

# One GET on a fresh h2c connection; arm the server's end first if asked.
sub get_h2 {
    my ($path, %o) = @_;
    my ($conn, $stream, $server_sock, $client_sock) = create_h2c_connection();
    my ($status, $body) = (undef, '');
    my $client = create_client(
        on_header          => sub { $status = $_[2] if $_[1] eq ':status'; 0 },
        on_data_chunk_recv => sub { $body .= $_[1]; 0 },
    );
    $client->send_connection_preface;
    syswrite($client_sock, $client->mem_send);
    exchange_frames($client, $client_sock, 5);
    $ARM_FD = fileno($server_sock) if $o{arm};
    $client->submit_request(method => 'GET', path => $path, scheme => 'http', authority => 'localhost');
    syswrite($client_sock, $client->mem_send);
    my $eof = exchange_frames($client, $client_sock, 10);
    $ARM_FD = -1;
    $stream->close_now if $stream->read_handle;
    return ($status, $body, $eof);
}

my ($status, $body) = get_h2('/before');
is($status, '200', 'an HTTP/2 request works through the guard');

$DIES = 0;
my ($boom_status, $boom_body, $eof) = get_h2('/boom', arm => 1);
ok(1, 'the server process survived an HTTP/2 write that died');
is($DIES, 1, 'the armed write died once and was not retried');
ok($eof, 'the connection whose write died is closed');
isnt($boom_body, 'ok /boom', 'its response did not arrive');
my @died = grep { $_->{message} =~ /socket write died/ } @log;
is(scalar @died, 1, 'the server logs it once');
like($died[0]{message} // '', qr/\Asocket write died: armed h2 write failure; the connection was closed\z/,
    'naming the exception');

($status, $body) = get_h2('/after');
is($status, '200', 'a new HTTP/2 connection is still served');
is($body, 'ok /after', 'with its body');

done_testing;
