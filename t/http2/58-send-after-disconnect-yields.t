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
        or plan(skip_all => 'HTTP/2 not available (Net::HTTP2::nghttp2 0.013+ required)');
}

# PAGI::Spec "Send Completion Contract", over HTTP/2: a send issued after
# an abnormal end does no work and resolves successfully, but from the event
# loop, never within the call to $send. Its Future settles after the scope's
# terminal notifications, which were queued on the loop first.
#
# What that buys: a body whose only waits are sends, raced against
# disconnect_future, stops once the client leaves, whether it resets the
# stream or drops the whole connection. Were each such send done at once,
# the body would never return to the loop, the news could not be delivered,
# and it would run to its end.
#
# The HTTP/1.1 twin is t/94-send-after-disconnect-yields.t.

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;
use Net::HTTP2::nghttp2::Session;
use Protocol::WebSocket::Frame;

my $loop = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;
my @log;

# Small watermarks so a body the client stops reading parks quickly.
sub create_h2_connection {
    my ($app) = @_;
    socketpair(my $sock_a, my $sock_b, AF_UNIX, SOCK_STREAM, 0)
        or die "socketpair: $!";
    $sock_a->blocking(0);
    $sock_b->blocking(0);

    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, quiet => 1, access_log => undef,
        http2 => 1, shutdown_timeout => 1, logger => sub { push @log, $_[0] },
        write_high_watermark => 8192, write_low_watermark => 2048,
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

# A client that opens one GET stream, then reads nothing and sends no
# WINDOW_UPDATEs: the stream's flow-control window runs out and a body
# writing to it parks.
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

# A body of 20,000 one-kilobyte sends and nothing else, counting the sends
# it makes once the client is known to have gone.
sub export_body {
    my ($send, $conn, $obs, $chunk) = @_;
    $chunk //= { type => 'http.response.body', body => 'x' x 1024, more => 1 };
    return async sub {
        for my $n (1 .. 20_000) {
            $obs->{sent}++;
            $obs->{sent_after_leave}++ if defined $conn->disconnect_reason;
            await $send->({ %$chunk });
        }
    }->();
}

subtest 'a send-only body raced against disconnect_future stops when the client resets the stream' => sub {
    my %obs = (sent => 0, sent_after_leave => 0);
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported scope type: $scope->{type}\n" unless $scope->{type} eq 'http';
        my $conn = $scope->{'pagi.connection'};
        await $send->({ type => 'http.response.start', status => 200,
            headers => [['content-type', 'text/plain']] });
        my $export = export_body($send, $conn, \%obs);
        await Future->wait_any($export, $conn->disconnect_future);
        $obs{export_cancelled} = $export->is_cancelled ? 1 : 0;
        $obs{done} = 1;
    };

    my ($client_sock, $server, $h2conn) = create_h2_connection($app);
    my ($client, $stream_id) = open_stream($client_sock);

    # The window runs out and the body parks: its count of sends stops
    # moving for half a second.
    my ($last, $since) = (-1, time);
    my $parked = pump_until(sub {
        ($last, $since) = ($obs{sent}, time) if $obs{sent} != $last;
        return $obs{sent} > 0 && time - $since > 0.5;
    }, 15);
    ok($parked, 'the body is parked on the exhausted window') or diag "sent=$obs{sent}";
    reset_stream($client, $client_sock, $stream_id);

    ok(pump_until(sub { $obs{done} }, 30), 'the application finished');
    is($obs{export_cancelled}, 1, 'the body was cancelled, not run to its end')
        or diag "sent=$obs{sent} of 20000";
    cmp_ok($obs{sent_after_leave}, '<=', 1, 'at most one send after the client left')
        or diag "sent_after_leave=$obs{sent_after_leave}";

    stop_server($server);
};

# The client gives up during a slow query, by resetting its stream or by
# dropping the connection; the body begins after the server has seen it go.
for my $leave (['resets the stream', \&reset_stream], ['drops the connection', sub { close $_[1] }]) {
    my ($how, $go) = @$leave;
    subtest "a body that starts after the client $how stops at its first send" => sub {
        my %obs = (sent => 0, sent_after_leave => 0);
        my $gate = $loop->new_future;
        my $app = async sub {
            my ($scope, $receive, $send) = @_;
            die "unsupported scope type: $scope->{type}\n" unless $scope->{type} eq 'http';
            my $conn = $obs{conn} = $scope->{'pagi.connection'};
            await $send->({ type => 'http.response.start', status => 200,
                headers => [['content-type', 'text/plain']] });
            $obs{started} = 1;
            await $gate;   # a slow query, during which the client gives up
            my $export = export_body($send, $conn, \%obs);
            await Future->wait_any($export, $conn->disconnect_future);
            $obs{export_cancelled} = $export->is_cancelled ? 1 : 0;
            $obs{done} = 1;
        };

        my ($client_sock, $server, $h2conn) = create_h2_connection($app);
        my ($client, $stream_id) = open_stream($client_sock);
        ok(pump_until(sub { $obs{started} }), 'the response started');
        $go->($client, $client_sock, $stream_id);
        ok(pump_until(sub { defined $obs{conn}->disconnect_reason }),
            'the server saw the client leave');

        $loop->later(sub { $gate->done });
        ok(pump_until(sub { $obs{done} }, 30), 'the application finished');
        is($obs{export_cancelled}, 1, 'the body was cancelled, not run to its end')
            or diag "sent=$obs{sent} of 20000";
        is($obs{sent_after_leave}, 1, 'it made one send, which did not settle at once');

        stop_server($server);
    };
}

# The protocol scopes: each opens, its start is sent, and its body is a run
# of sends of its own kind.
my %SCOPE = (
    sse => {
        request => [ path => '/events', headers => [['accept', 'text/event-stream']] ],
        start   => { type => 'sse.start', status => 200 },
        chunk   => { type => 'sse.send', data => 'x' x 1024 },
    },
    websocket => {
        request => [ method => 'CONNECT', path => '/socket', body => sub { undef },
            headers => [[':protocol', 'websocket'], ['sec-websocket-version', '13']] ],
        start   => { type => 'websocket.accept' },
        chunk   => { type => 'websocket.send', text => 'x' x 1024 },
    },
);

for my $type (sort keys %SCOPE) {
    my $kind = $SCOPE{$type};
    for my $leave (['resets the stream', \&reset_stream], ['drops the connection', sub { close $_[1] }]) {
        my ($how, $go) = @$leave;
        subtest "a $type body that starts after the client $how stops at its first send" => sub {
            my %obs = (sent => 0, sent_after_leave => 0);
            my $gate = $loop->new_future;
            my $app = async sub {
                my ($scope, $receive, $send) = @_;
                die "unsupported scope type: $scope->{type}\n" unless $scope->{type} eq $type;
                my $conn = $obs{conn} = $scope->{'pagi.connection'};
                await $receive->();   # the scope's opening event
                await $send->({ %{ $kind->{start} } });
                $obs{started} = 1;
                await $gate;   # a slow query, during which the client gives up
                my $export = export_body($send, $conn, \%obs, $kind->{chunk});
                await Future->wait_any($export, $conn->disconnect_future);
                $obs{export_cancelled} = $export->is_cancelled ? 1 : 0;
                $obs{done} = 1;
            };

            my ($client_sock, $server, $h2conn) = create_h2_connection($app);
            my ($client, $stream_id) = open_stream($client_sock, @{ $kind->{request} });
            ok(pump_until(sub { $obs{started} }), 'the stream started');
            $go->($client, $client_sock, $stream_id);
            ok(pump_until(sub { defined $obs{conn}->disconnect_reason }),
                'the server saw the client leave');

            $loop->later(sub { $gate->done });
            ok(pump_until(sub { $obs{done} }, 30), 'the application finished');
            is($obs{export_cancelled}, 1, 'the body was cancelled, not run to its end')
                or diag "sent=$obs{sent} of 20000";
            is($obs{sent_after_leave}, 1, 'it made one send, which did not settle at once');

            stop_server($server);
        };
    }
}

# A clean end is no different: once a WebSocket's closing handshake, begun
# by the client (its Close with END_STREAM), has completed and the stream
# has closed, a send settles from the loop too, and a body raced against
# end_future stops at its first.
subtest 'a websocket body that starts after a clean close stops at its first send' => sub {
    my %obs = (sent => 0, sent_after_end => 0);
    my $gate = $loop->new_future;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported scope type: $scope->{type}\n" unless $scope->{type} eq 'websocket';
        my $conn = $obs{conn} = $scope->{'pagi.connection'};
        await $receive->();   # websocket.connect
        await $send->({ type => 'websocket.accept' });
        $obs{started} = 1;
        await $gate;   # a slow query, during which the client says goodbye
        my $export = async sub {
            for my $n (1 .. 20_000) {
                $obs{sent}++;
                $obs{sent_after_end}++ if $conn->response_complete;
                await $send->({ type => 'websocket.send', text => 'x' x 1024 });
            }
        }->();
        await Future->wait_any($export, $conn->end_future);
        $obs{export_cancelled} = $export->is_cancelled ? 1 : 0;
        $obs{done} = 1;
    };

    my ($client_sock, $server, $h2conn) = create_h2_connection($app);
    my ($client, $stream_id) = open_stream($client_sock, @{ $SCOPE{websocket}{request} });
    ok(pump_until(sub { $obs{started} }), 'the socket was accepted');
    $client->submit_data($stream_id, Protocol::WebSocket::Frame->new(
        type => 'close', buffer => pack('n', 1000), masked => 1)->to_bytes, 1);
    $client_sock->syswrite($client->mem_send);
    ok(pump_until(sub { $obs{conn}->response_complete }), 'the socket ended cleanly');

    $loop->later(sub { $gate->done });
    ok(pump_until(sub { $obs{done} }, 30), 'the application finished');
    is($obs{export_cancelled}, 1, 'the body was cancelled, not run to its end')
        or diag "sent=$obs{sent} of 20000";
    is($obs{sent_after_end}, 1, 'it made one send, which did not settle at once');

    stop_server($server);
};

is(\@log, [], 'nothing logged');

done_testing;
