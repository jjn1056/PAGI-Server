use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Async::Stream;
use Future::AsyncAwait;
use Time::HiRes ();
use FindBin;
use lib "$FindBin::Bin/../../lib";
use Socket qw(AF_UNIX SOCK_STREAM);
use Scalar::Util qw(refaddr);

BEGIN {
    require PAGI::Server::Protocol::HTTP2;
    PAGI::Server::Protocol::HTTP2->available
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.011+ required)');
}

# HTTP/2 twin of t/shutdown-waits-for-apps.t: a shutting-down server waits,
# until its shutdown_timeout deadline, for each stream's application to return
# -- here, cleanup that awaits after its server_shutdown disconnect -- before
# the shutdown completes.

use PAGI::Server;
use PAGI::Server::Connection;
use PAGI::Server::Protocol::HTTP1;
use Protocol::WebSocket::Frame;

my $loop     = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# Every server built here logs into this collector rather than STDERR, so a
# case can assert over what was logged as well as over what was sent.
my @LOG;

sub create_h2_connection {
    my (%o) = @_;
    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0) or die "socketpair: $!";
    $sock_a->blocking(0);
    $sock_b->blocking(0);
    my $app    = $o{app};
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, http2 => 1,
        shutdown_timeout => $o{shutdown_timeout} // 10,
        log_level => 'debug', access_log => undef,
        logger => sub { push @LOG, $_[0] },
    );
    $loop->add($server);
    my $stream = IO::Async::Stream->new(
        read_handle => $sock_a, write_handle => $sock_a, on_read => sub { 0 });
    my $conn = PAGI::Server::Connection->new(
        stream => $stream, app => $app, protocol => $protocol, server => $server,
        h2_protocol => $server->{http2_protocol}, alpn_protocol => 'h2',
    );
    $server->add_child($stream);
    $conn->start;
    # The listener is what registers a connection and marks the server running;
    # this harness builds the connection directly, so it does both itself --
    # PAGI::Server::shutdown returns at once on a server that never ran, and
    # _drain_connections only sees connections the server knows about.
    $server->{connections}{refaddr($conn)} = $conn;
    $server->{running} = 1;
    return ($conn, $stream, $sock_b, $server);
}

# What the client saw, in the order it saw it: the GOAWAY announcement, the
# close of its own request stream (the response completing), and the server's
# EOF. The client never closes its end, so an 'eof' in here is the server's
# close and nothing else.
my @EVENT;
my $BODY = '';

sub create_client {
    require Net::HTTP2::nghttp2::Session;
    @EVENT = ();
    $BODY  = '';
    return Net::HTTP2::nghttp2::Session->new_client(callbacks => {
        on_begin_headers   => sub { 0 },
        on_header          => sub { 0 },
        on_frame_recv      => sub { push @EVENT, 'goaway' if $_[0]{type} == 7; 0 },
        on_data_chunk_recv => sub { $BODY .= $_[1]; 0 },
        on_stream_close    => sub { push @EVENT, 'response'; 0 },
    });
}

sub first_at {
    my ($what) = @_;
    for my $i (0 .. $#EVENT) { return $i if $EVENT[$i] eq $what }
    return -1;
}

# Run the loop and the client together until $cond holds or the server closes,
# bounded by wall clock rather than by a round count.
sub pump {
    my ($client, $sock, $seconds, $cond) = @_;
    my $deadline = Time::HiRes::time() + $seconds;
    while (Time::HiRes::time() < $deadline) {
        $loop->loop_once(0.02);
        my $buf = '';
        my $n = $sock->sysread($buf, 65536);
        if (defined $n && $n == 0) { push @EVENT, 'eof'; last }
        if (length $buf) {
            eval { $client->mem_recv($buf) };
        }
        my $out = eval { $client->mem_send };
        $sock->syswrite($out) if defined $out && length $out;
        last if $cond && $cond->();
    }
    return;
}

sub handshake {
    my ($client, $sock) = @_;
    $loop->loop_once(0.1);
    my $settings = '';
    $sock->sysread($settings, 65536);
    $client->send_connection_preface;
    $sock->syswrite($client->mem_send);
    $loop->loop_once(0.1);
    $client->mem_recv($settings) if length $settings;
    pump($client, $sock, 0.5);
    return;
}

sub open_ws_stream {
    my ($client, $sock) = @_;
    my $sid = $client->submit_request(
        method => 'CONNECT', path => '/ws', scheme => 'http', authority => 'localhost',
        headers => [[':protocol', 'websocket'], ['sec-websocket-version', '13']],
        body => sub { return undef },
    );
    my $out = $client->mem_send;
    $sock->syswrite($out) if length $out;
    return $sid;
}

my @ORDER;
my $app = async sub {
    my ($scope, $receive, $send) = @_;
    return unless $scope->{type} eq 'websocket';
    await $receive->();
    await $send->({ type => 'websocket.accept' });
    push @ORDER, 'accepted';
    while (1) {
        my $event = await $receive->();
        next unless $event->{type} eq 'websocket.disconnect';
        push @ORDER, "disconnect:$event->{reason}";
        await $loop->delay_future(after => 0.3);     # cleanup, e.g. a database write
        push @ORDER, 'cleanup finished';
        return;
    }
};

subtest 'shutdown completes only after the stream application finished its cleanup' => sub {
    my ($conn, $stream, $sock, $server) = create_h2_connection(app => $app, shutdown_timeout => 5);
    my $client = create_client();
    handshake($client, $sock);
    open_ws_stream($client, $sock);
    pump($client, $sock, 2, sub { grep { $_ eq 'accepted' } @ORDER });
    ok((grep { $_ eq 'accepted' } @ORDER), 'the app accepted the WebSocket');

    my $done = $server->shutdown;
    $done->on_ready(sub { push @ORDER, 'shutdown complete' });
    my $started = Time::HiRes::time();
    pump($client, $sock, 6, sub { $done->is_ready });
    $loop->loop_once(0.05) until $done->is_ready || Time::HiRes::time() - $started > 6;
    ok($done->is_ready, 'shutdown completed');
    is([grep { $_ ne 'accepted' } @ORDER],
       ['disconnect:server_shutdown', 'cleanup finished', 'shutdown complete'],
       'the cleanup finished before the shutdown did');
    eval { $loop->remove($server) };
};

done_testing;
