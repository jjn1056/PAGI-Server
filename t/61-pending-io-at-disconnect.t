#!/usr/bin/env perl

# =============================================================================
# Test: Settlement of pending I/O at disconnect (spec 0.5 / Www 0.4)
#
# Pins the now-normative contract end-to-end over real sockets:
# 1. A send Future parked on backpressure when the client disconnects
#    settles by RESOLVING successfully -- never fails, never hangs.
# 2. The resumed coroutine observes the connection-state transition already
#    complete: is_connected() false, disconnect_reason() set (Www.pod
#    State Transition Order invariant).
# 3. on_disconnect callbacks are never invoked synchronously within the
#    application's call into $send (callback invocation context).
# 4. A receive Future pending at disconnect resolves with the protocol's
#    disconnect event (http.disconnect / websocket.disconnect /
#    sse.disconnect).
#
# HTTP/2 coverage (RST_STREAM settlement) lives in
# t/http2/40-pending-io-at-disconnect.t.
# =============================================================================

use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
use Socket qw(SO_RCVBUF SOL_SOCKET);
use Future::AsyncAwait;

use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

my $loop = IO::Async::Loop->new;

# Small server-side watermarks so a flooding producer parks on the drain
# waiter quickly once the kernel buffers stop absorbing writes.
sub start_server {
    my ($app) = @_;
    my $server = PAGI::Server->new(
        app                  => $app,
        host                 => '127.0.0.1',
        port                 => 0,
        quiet                => 1,
        access_log           => undef,
        write_high_watermark => 8192,
        write_low_watermark  => 2048,
    );
    $loop->add($server);
    $server->listen->get;
    return $server;
}

sub connect_client {
    my ($port) = @_;
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1',
        PeerPort => $port,
        Proto    => 'tcp',
        Timeout  => 2,
    ) or die "Cannot connect: $!";
    # Shrink the client's receive window so an unread response backs up
    # into the server quickly.
    $sock->sockopt(SO_RCVBUF, 4096);
    $sock->blocking(0);
    return $sock;
}

# Drive the loop until $cond returns true or $timeout expires; returns the
# final truth of $cond.
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

sub shutdown_server {
    my ($server) = @_;
    $server->shutdown->get;
    eval { $loop->remove($server) };
}

# =============================================================================
# 1. HTTP: parked send resolves; state updated on resume; callback context
# =============================================================================

subtest 'h1 http: parked send resolves at abrupt disconnect' => sub {
    my %obs = (started => 0, completed => 0, cb_count => 0);
    my $chunk = 'x' x 65536;

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

        for my $n (1 .. 400) {
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
                # The invariant under test: by the time the awaiting
                # coroutine resumes, the transition is already complete.
                $obs{resumed} = {
                    connected => $conn->is_connected ? 1 : 0,
                    reason    => $conn->disconnect_reason,
                };
                last;
            }
        }
        $obs{app_completed} = 1;
        return;
    };

    my $server = start_server($app);
    my $sock   = connect_client($server->port);
    syswrite($sock, "GET /stream HTTP/1.1\r\nHost: localhost\r\n\r\n");

    # Read a little of the response so the exchange is established, then
    # stop reading entirely so the server's buffers fill.
    my $got = '';
    pump_until(sub {
        my $buf;
        my $n = sysread($sock, $buf, 4096);
        $got .= $buf if defined $n && $n > 0;
        return $got =~ /200 OK/;
    }, 5);
    like($got, qr/200 OK/, 'response started');

    # Wait until the producer is genuinely parked: one send started and not
    # completing across two consecutive observations.
    my $parked = 0;
    pump_until(sub {
        return 0 unless $obs{started} == $obs{completed} + 1;
        my ($s, $c) = ($obs{started}, $obs{completed});
        $loop->loop_once(0.2);
        $parked = ($obs{started} == $s && $obs{completed} == $c);
        return $parked;
    }, 8);
    ok($parked, 'a send is parked on backpressure')
        or diag("started=$obs{started} completed=$obs{completed}");

    close($sock);

    ok(pump_until(sub { $obs{app_completed} }, 10),
        'application completed after disconnect (parked send did not hang)');
    ok(!$obs{send_failed}, 'the parked send resolved successfully, not failed')
        or diag("send failed with: $obs{send_failed}");
    ok($obs{resumed}, 'application resumed from the parked send and observed disconnect');
    is($obs{resumed}{connected}, 0, 'resumed coroutine sees is_connected false');
    like($obs{resumed}{reason}, qr/^(client_closed|write_error|read_error)$/,
        'resumed coroutine sees a standard disconnect reason');
    is($obs{cb_count}, 1, 'on_disconnect fired exactly once');
    is($obs{cb_in_send_frame}, 0,
        'on_disconnect was not invoked inside the application send call frame');
    is($obs{cb_reason}, $obs{resumed}{reason}, 'callback and accessor agree on the reason');

    shutdown_server($server);
};

# =============================================================================
# 2. HTTP: pending receive resolves with http.disconnect
# =============================================================================

subtest 'h1 http: pending receive resolves with http.disconnect' => sub {
    my %obs;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "Unsupported scope type: $scope->{type}" unless $scope->{type} eq 'http';

        my $first = await $receive->();
        $obs{first_type} = $first->{type};
        $obs{first_done} = 1;

        my $second = await $receive->();
        $obs{second_type} = $second->{type};
        $obs{app_completed} = 1;
        return;
    };

    my $server = start_server($app);
    my $sock   = connect_client($server->port);
    syswrite($sock, "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");

    ok(pump_until(sub { $obs{first_done} }, 5), 'app consumed the request event');
    is($obs{first_type}, 'http.request', 'first event is http.request');

    close($sock);

    ok(pump_until(sub { $obs{app_completed} }, 10),
        'application completed after disconnect (pending receive did not hang)');
    is($obs{second_type}, 'http.disconnect', 'pending receive resolved with http.disconnect');

    shutdown_server($server);
};

# =============================================================================
# 3. WebSocket: parked send resolves; disconnect reported via receive
# =============================================================================

subtest 'websocket: parked send resolves at abrupt disconnect' => sub {
    my %obs = (started => 0, completed => 0);
    my $payload = 'w' x 16384;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "Unsupported scope type: $scope->{type}" unless $scope->{type} eq 'websocket';

        my $connect = await $receive->();
        $obs{connect_type} = $connect->{type};
        await $send->({ type => 'websocket.accept' });

        for my $n (1 .. 400) {
            $obs{started}++;
            my $ok = eval {
                await $send->({ type => 'websocket.send', bytes => $payload });
                1;
            };
            unless ($ok) {
                $obs{send_failed} = "$@";
                last;
            }
            $obs{completed}++;
        }

        my $event = await $receive->();
        $obs{disconnect_type} = $event->{type};
        $obs{disconnect_code} = $event->{code};
        $obs{app_completed}   = 1;
        return;
    };

    my $server = start_server($app);
    my $sock   = connect_client($server->port);
    my $key    = 'dGhlIHNhbXBsZSBub25jZQ==';
    syswrite($sock,
          "GET / HTTP/1.1\r\n"
        . "Host: localhost\r\n"
        . "Upgrade: websocket\r\n"
        . "Connection: Upgrade\r\n"
        . "Sec-WebSocket-Key: $key\r\n"
        . "Sec-WebSocket-Version: 13\r\n"
        . "\r\n");

    my $got = '';
    pump_until(sub {
        my $buf;
        my $n = sysread($sock, $buf, 4096);
        $got .= $buf if defined $n && $n > 0;
        return $got =~ /HTTP\/1\.1 101/;
    }, 5);
    like($got, qr/HTTP\/1\.1 101/, 'websocket upgrade succeeded');

    # Stop reading; wait for the flood to park.
    my $parked = 0;
    pump_until(sub {
        return 0 unless $obs{started} == $obs{completed} + 1;
        my ($s, $c) = ($obs{started}, $obs{completed});
        $loop->loop_once(0.2);
        $parked = ($obs{started} == $s && $obs{completed} == $c);
        return $parked;
    }, 8);
    ok($parked, 'a websocket send is parked on backpressure')
        or diag("started=$obs{started} completed=$obs{completed}");

    close($sock);

    ok(pump_until(sub { $obs{app_completed} }, 10),
        'application completed after disconnect (parked send did not hang)');
    ok(!$obs{send_failed}, 'the parked websocket send resolved successfully, not failed')
        or diag("send failed with: $obs{send_failed}");
    is($obs{disconnect_type}, 'websocket.disconnect',
        'receive reported websocket.disconnect');
    is($obs{disconnect_code}, 1006, 'abnormal closure reports code 1006');

    shutdown_server($server);
};

# =============================================================================
# 4. SSE: parked send resolves; disconnect reported via receive
# =============================================================================

subtest 'sse: parked send resolves at abrupt disconnect' => sub {
    my %obs = (started => 0, completed => 0);
    my $payload = 's' x 16384;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "Unsupported scope type: $scope->{type}" unless $scope->{type} eq 'sse';

        await $send->({ type => 'sse.start' });

        for my $n (1 .. 400) {
            $obs{started}++;
            my $ok = eval {
                await $send->({ type => 'sse.send', data => $payload });
                1;
            };
            unless ($ok) {
                $obs{send_failed} = "$@";
                last;
            }
            $obs{completed}++;
        }

        my $event = await $receive->();
        $obs{disconnect_type}   = $event->{type};
        $obs{disconnect_reason} = $event->{reason};
        $obs{app_completed}     = 1;
        return;
    };

    my $server = start_server($app);
    my $sock   = connect_client($server->port);
    syswrite($sock,
          "GET /events HTTP/1.1\r\n"
        . "Host: localhost\r\n"
        . "Accept: text/event-stream\r\n"
        . "\r\n");

    my $got = '';
    pump_until(sub {
        my $buf;
        my $n = sysread($sock, $buf, 4096);
        $got .= $buf if defined $n && $n > 0;
        return $got =~ /200 OK/;
    }, 5);
    like($got, qr/200 OK/, 'sse stream started');

    my $parked = 0;
    pump_until(sub {
        return 0 unless $obs{started} == $obs{completed} + 1;
        my ($s, $c) = ($obs{started}, $obs{completed});
        $loop->loop_once(0.2);
        $parked = ($obs{started} == $s && $obs{completed} == $c);
        return $parked;
    }, 8);
    ok($parked, 'an sse send is parked on backpressure')
        or diag("started=$obs{started} completed=$obs{completed}");

    close($sock);

    ok(pump_until(sub { $obs{app_completed} }, 10),
        'application completed after disconnect (parked send did not hang)');
    ok(!$obs{send_failed}, 'the parked sse send resolved successfully, not failed')
        or diag("send failed with: $obs{send_failed}");
    is($obs{disconnect_type}, 'sse.disconnect', 'receive reported sse.disconnect');
    like($obs{disconnect_reason}, qr/^(client_closed|write_error|read_error|write_timeout)$/,
        'sse.disconnect carries a standard reason');

    shutdown_server($server);
};

done_testing;
