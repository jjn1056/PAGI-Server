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
# Test: RST_STREAM settles pending I/O (spec 0.5 / Www 0.4)
# ============================================================
# The spec maps RST_STREAM to the standard abnormal-disconnect transition
# for that stream's scope. Pinned here, end-to-end over a real nghttp2
# client session:
# 1. A send Future parked on the stream (flow-control window exhausted, no
#    client WINDOW_UPDATEs) settles by RESOLVING when the client resets the
#    stream -- never fails, never hangs.
# 2. The resumed coroutine observes is_connected() false with the reason
#    set, and on_disconnect fired outside the application send call frame.
# 3. The application Future is NOT cancelled: post-disconnect cleanup that
#    must be resumed by the event loop still runs to completion.
# 4. A receive Future pending at RST_STREAM resolves with http.disconnect.
#
# h1/WebSocket/SSE settlement coverage lives in
# t/61-pending-io-at-disconnect.t.

use PAGI::Server::Connection;
use PAGI::Server;
use PAGI::Server::Protocol::HTTP1;
use PAGI::Server::Protocol::HTTP2;

my $loop = IO::Async::Loop->new;
my $protocol = PAGI::Server::Protocol::HTTP1->new;

# ============================================================
# Helpers (lifted verbatim from t/http2/05-request-lifecycle.t, plus
# access_log silencing and small write watermarks so a flooding producer
# parks quickly).
# ============================================================

sub create_test_server {
    my (%args) = @_;
    my $server = PAGI::Server->new(
        app                  => $args{app} // sub { },
        host                 => '127.0.0.1',
        port                 => 0,
        quiet                => 1,
        access_log           => undef,
        http2                => 1,
        write_high_watermark => 8192,
        write_low_watermark  => 2048,
        %args,
    );
    $loop->add($server);
    return $server;
}

sub create_h2_connection {
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
        alpn_protocol => 'h2',
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

sub complete_h2_handshake {
    my ($client, $client_sock) = @_;

    # Read server's initial SETTINGS
    $loop->loop_once(0.1);
    my $server_settings = '';
    $client_sock->sysread($server_settings, 4096);

    # Client sends connection preface + SETTINGS
    $client->send_connection_preface;
    my $data = $client->mem_send;
    $client_sock->syswrite($data);
    $loop->loop_once(0.1);

    # Feed server settings to client
    $client->mem_recv($server_settings);

    # Server sends SETTINGS ACK
    $loop->loop_once(0.1);
    my $ack = '';
    $client_sock->sysread($ack, 4096);
    $client->mem_recv($ack) if length($ack);

    # Client sends SETTINGS ACK
    my $client_ack = $client->mem_send;
    $client_sock->syswrite($client_ack) if length($client_ack);
    $loop->loop_once(0.1);

    # Consume any remaining server data
    my $extra = '';
    $client_sock->sysread($extra, 4096);
    $client->mem_recv($extra) if length($extra);
}

# Drive the loop until $cond returns true or $timeout expires.
sub pump_until {
    my ($cond, $timeout) = @_;
    $timeout //= 10;
    my $deadline = time + $timeout;
    while (time < $deadline) {
        return 1 if $cond->();
        $loop->loop_once(0.05);
    }
    return $cond->() ? 1 : 0;
}

# ============================================================
# RST_STREAM settles a parked send; app future is not cancelled
# ============================================================

subtest 'RST_STREAM settles a parked send; cleanup still runs' => sub {
    my %obs = (started => 0, completed => 0, cb_count => 0);
    my $chunk = 'h' x 32768;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "Unsupported scope type: $scope->{type}" unless $scope->{type} eq 'http';
        my $conn = $scope->{'pagi.connection'};

        my $in_send_frame = 0;
        $conn->on_disconnect(sub {
            my ($reason) = @_;
            $obs{cb_count}++;
            $obs{cb_reason}        = $reason;
            $obs{cb_in_send_frame} = $in_send_frame;
        });

        await $send->({
            type    => 'http.response.start',
            status  => 200,
            headers => [ [ 'content-type', 'application/octet-stream' ] ],
        });

        for my $n (1 .. 64) {
            $obs{started}++;
            $in_send_frame = 1;
            my $f = $send->({ type => 'http.response.body', body => $chunk, more => 1 });
            $in_send_frame = 0;
            my $ok = eval { await $f; 1 };
            unless ($ok) {
                $obs{send_failed} = "$@";
                last;
            }
            $obs{completed}++;
            if (!$conn->is_connected && !$obs{resumed}) {
                $obs{resumed} = {
                    connected => $conn->is_connected ? 1 : 0,
                    reason    => $conn->disconnect_reason,
                };
                last;
            }
        }

        # Post-disconnect cleanup that requires the event loop to resume
        # this coroutine again: only reachable if the application Future
        # was settled-with, not cancelled out from under, the app.
        await $loop->delay_future(after => 0.05);
        $obs{cleanup_ran}   = 1;
        $obs{app_completed} = 1;
        return;
    };

    my ($conn, $stream, $client_sock, $server) = create_h2_connection(app => $app);
    my $client = create_client();
    complete_h2_handshake($client, $client_sock);

    my $stream_id = $client->submit_request(
        method    => 'GET',
        path      => '/stream',
        scheme    => 'https',
        authority => 'localhost',
    );
    ok($stream_id, 'client opened a stream');
    $client_sock->syswrite($client->mem_send);

    # Do not read the socket and send no WINDOW_UPDATEs: the stream's
    # flow-control window exhausts and the producer parks.
    my $parked = 0;
    pump_until(sub {
        return 0 unless $obs{started} == $obs{completed} + 1;
        my ($s, $c) = ($obs{started}, $obs{completed});
        $loop->loop_once(0.2);
        $parked = ($obs{started} == $s && $obs{completed} == $c);
        return $parked;
    }, 8);
    ok($parked, 'a send is parked on the exhausted stream window')
        or diag("started=$obs{started} completed=$obs{completed}");

    # Client resets the stream (CANCEL).
    $client->submit_rst_stream($stream_id, 8);
    $client_sock->syswrite($client->mem_send);

    ok(pump_until(sub { $obs{app_completed} }, 10),
        'application completed after RST_STREAM (parked send did not hang)');
    ok(!$obs{send_failed}, 'the parked send resolved successfully, not failed')
        or diag("send failed with: $obs{send_failed}");
    ok($obs{resumed}, 'application resumed from the parked send and observed disconnect');
    is($obs{resumed}{connected}, 0, 'resumed coroutine sees is_connected false');
    is($obs{resumed}{reason}, 'client_closed', 'RST_STREAM reports client_closed');
    is($obs{cb_count}, 1, 'on_disconnect fired exactly once');
    is($obs{cb_in_send_frame}, 0,
        'on_disconnect was not invoked inside the application send call frame');
    is($obs{cleanup_ran}, 1,
        'post-disconnect cleanup ran (application Future was not cancelled)');

    $server->shutdown->get;
    eval { $loop->remove($server) };
};

# ============================================================
# RST_STREAM resolves a pending receive with http.disconnect
# ============================================================

subtest 'RST_STREAM resolves a pending receive with http.disconnect' => sub {
    my %obs;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "Unsupported scope type: $scope->{type}" unless $scope->{type} eq 'http';

        # Drain the request fully first: h2 delivers a bodyless request as
        # separate http.request events (headers, then end-of-stream).
        my $ev = await $receive->();
        $obs{first_type} = $ev->{type};
        while ($ev->{type} eq 'http.request' && $ev->{more}) {
            $ev = await $receive->();
        }
        $obs{request_drained} = 1;

        my $next = await $receive->();
        $obs{next_type}     = $next->{type};
        $obs{app_completed} = 1;
        return;
    };

    my ($conn, $stream, $client_sock, $server) = create_h2_connection(app => $app);
    my $client = create_client();
    complete_h2_handshake($client, $client_sock);

    my $stream_id = $client->submit_request(
        method    => 'GET',
        path      => '/',
        scheme    => 'https',
        authority => 'localhost',
    );
    ok($stream_id, 'client opened a stream');
    $client_sock->syswrite($client->mem_send);

    ok(pump_until(sub { $obs{request_drained} }, 5), 'app drained the request events');
    is($obs{first_type}, 'http.request', 'first event is http.request');

    $client->submit_rst_stream($stream_id, 8);
    $client_sock->syswrite($client->mem_send);

    ok(pump_until(sub { $obs{app_completed} }, 10),
        'application completed after RST_STREAM (pending receive did not hang)');
    is($obs{next_type}, 'http.disconnect', 'pending receive resolved with http.disconnect');

    $server->shutdown->get;
    eval { $loop->remove($server) };
};

done_testing;
