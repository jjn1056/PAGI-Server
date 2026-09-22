#!/usr/bin/env perl

# =============================================================================
# Test: close_code() / close_reason() expose the PEER's WebSocket Close on h1,
# populated before the terminal callback (Www.pod "Connection State" and
# "State Transition Order").
#
# Each app reads close_code/close_reason INSIDE its on_complete / on_disconnect
# callback, so the assertions pin the F4 requirement: every terminal fact is
# populated before the callback runs. A naive populate-after-mark ordering
# would make the callback see undef (see the F4 mutation in the task report).
#
# Cases:
#   1. Peer Close with code+reason -> close_code == code, close_reason == text,
#      read in on_complete (a completed handshake is a clean end).
#   2. Peer Close with no code (empty payload) -> close_code 1005, reason undef.
#   3. Abrupt transport drop, no Close -> close_code 1006, reason undef, read in
#      on_disconnect (abnormal end).
#   4. Server-initiated close (app sends websocket.close 1011), peer never
#      replies, transport drops -> close_code 1006, NOT the server's 1011.
#   5. http and sse scopes -> close_code/close_reason undef at their clean end.
# =============================================================================

use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
use Future;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";

use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

my $loop = IO::Async::Loop->new;

# A masked WebSocket frame (client -> server frames MUST be masked, RFC 6455).
# Lifted from t/82-ws-parked-terminal-notification.t.
sub make_websocket_frame {
    my ($opcode, $payload) = @_;

    my $frame = chr(0x80 | $opcode);

    my $len = length($payload);
    if ($len < 126) {
        $frame .= chr(0x80 | $len);
    }
    else {
        $frame .= chr(0x80 | 126) . pack('n', $len);
    }

    my $mask = pack('N', int(rand(0xFFFFFFFF)));
    $frame .= $mask;

    my $masked_payload = '';
    for my $i (0 .. length($payload) - 1) {
        $masked_payload .= chr(ord(substr($payload, $i, 1)) ^ ord(substr($mask, $i % 4, 1)));
    }
    $frame .= $masked_payload;

    return $frame;
}

sub start_server {
    my ($app) = @_;
    my $server = PAGI::Server->new(
        app              => $app,
        host             => '127.0.0.1',
        port             => 0,
        quiet            => 1,
        access_log       => undef,
        shutdown_timeout => 1,
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
    $sock->blocking(0);
    return $sock;
}

# Drive the loop until $cond is true or $timeout expires; returns its final
# truth. No wall-clock sleeps: the loop is pumped in short slices.
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
    eval { $server->shutdown->get };
    eval { $loop->remove($server) };
}

sub ws_upgrade {
    my ($sock) = @_;
    my $key = 'dGhlIHNhbXBsZSBub25jZQ==';
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
    return $got;
}

sub ws_refusal_request {
    my ($sock) = @_;
    my $key = 'dGhlIHNhbXBsZSBub25jZQ==';
    syswrite($sock,
          "GET / HTTP/1.1\r\n"
        . "Host: localhost\r\n"
        . "Upgrade: websocket\r\n"
        . "Connection: Upgrade\r\n"
        . "Sec-WebSocket-Key: $key\r\n"
        . "Sec-WebSocket-Version: 13\r\n"
        . "\r\n");
}

# A parked accepted-WebSocket app that records close_code/close_reason at the
# instant each terminal callback fires. $pre_park, if given, runs after the
# callbacks are registered and before the app parks (used to have the app send
# its own websocket.close for the server-initiated case).
sub build_ws_app {
    my (%opt) = @_;
    my %obs;
    my $park = $loop->new_future;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        my $conn = $scope->{'pagi.connection'};

        await $receive->();                          # websocket.connect
        await $send->({ type => 'websocket.accept' });

        $conn->on_complete(sub {
            $obs{complete}++;
            $obs{code}   = $conn->close_code;
            $obs{reason} = $conn->close_reason;
        });
        $conn->on_disconnect(sub {
            $obs{disconnect}++;
            $obs{code}   = $conn->close_code;
            $obs{reason} = $conn->close_reason;
        });

        if ($opt{pre_park}) {
            await $opt{pre_park}->($send);
        }

        $obs{parked} = 1;
        await $park;
        $obs{app_returned} = 1;
        return;
    };

    return ($app, \%obs, $park);
}

sub build_refusing_ws_app {
    my %seen = (complete => 0, end => 0, disconnect => 0);
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'websocket';
        my $conn = $scope->{'pagi.connection'};
        await $receive->();
        my $snapshot = sub {
            return [ $conn->close_code, $conn->close_reason,
                $conn->response_complete ? 1 : 0, $conn->is_connected ? 1 : 0,
                $conn->disconnect_reason, $conn->disconnect_detail ];
        };
        $conn->on_complete(sub { ++$seen{complete}; $seen{at_complete} = $snapshot->() });
        $conn->on_end(sub { ++$seen{end}; $seen{at_end} = $snapshot->() });
        $conn->on_disconnect(sub { ++$seen{disconnect} });
        my $end = $conn->end_future;
        await $send->({ type => 'http.response.start', status => 403,
                        headers => [['content-length', length 'Access denied']] });
        $seen{before_body} = $conn->close_code;
        await $send->({ type => 'http.response.body', body => 'Access denied' });
        $seen{end_value} = await $end;
        $seen{after_end} = $snapshot->();
        $seen{receive_type} = (await $receive->())->{type};
        $seen{returned} = 1;
    };
    return ($app, \%seen);
}

# =============================================================================
# 1. Peer Close with code + reason -> on_complete sees close_code/close_reason.
# =============================================================================
subtest 'peer Close(1000, "bye") -> close_code 1000, close_reason "bye"' => sub {
    my ($app, $obs, $park) = build_ws_app();
    my $server = start_server($app);
    my $sock   = connect_client($server->port);

    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'websocket upgrade succeeded');
    ok(pump_until(sub { $obs->{parked} }, 5), 'app accepted and parked');

    syswrite($sock, make_websocket_frame(8, pack('n', 1000) . 'bye'));

    ok(pump_until(sub { $obs->{complete} }, 5), 'on_complete fired');
    is($obs->{code},   1000,  'close_code is the peer code read inside on_complete');
    is($obs->{reason}, 'bye', 'close_reason is the peer text read inside on_complete');
    ok(!$obs->{disconnect}, 'on_disconnect did NOT fire (clean end)');

    $park->done unless $park->is_ready;
    pump_until(sub { $obs->{app_returned} }, 3);
    close($sock);
    shutdown_server($server);
};

# =============================================================================
# 2. Peer Close with no code (empty payload) -> 1005 / undef.
# =============================================================================
subtest 'peer Close with no code -> close_code 1005, close_reason undef' => sub {
    my ($app, $obs, $park) = build_ws_app();
    my $server = start_server($app);
    my $sock   = connect_client($server->port);

    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'websocket upgrade succeeded');
    ok(pump_until(sub { $obs->{parked} }, 5), 'app accepted and parked');

    # Close frame with an empty payload (no status code).
    syswrite($sock, make_websocket_frame(8, ''));

    ok(pump_until(sub { $obs->{complete} }, 5), 'on_complete fired');
    is($obs->{code},   1005,  'close_code is 1005 for a codeless peer Close');
    is($obs->{reason}, undef, 'close_reason is undef for a codeless peer Close');

    $park->done unless $park->is_ready;
    pump_until(sub { $obs->{app_returned} }, 3);
    close($sock);
    shutdown_server($server);
};

# =============================================================================
# 3. Abrupt transport drop, no Close frame -> 1006 / undef, in on_disconnect.
# =============================================================================
subtest 'abrupt drop, no Close -> close_code 1006, close_reason undef' => sub {
    my ($app, $obs, $park) = build_ws_app();
    my $server = start_server($app);
    my $sock   = connect_client($server->port);

    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'websocket upgrade succeeded');
    ok(pump_until(sub { $obs->{parked} }, 5), 'app accepted and parked');

    close($sock);   # TCP drop, no Close frame

    ok(pump_until(sub { $obs->{disconnect} }, 5), 'on_disconnect fired');
    is($obs->{code},   1006,  'close_code is 1006 for a transport drop with no Close');
    is($obs->{reason}, undef, 'close_reason is undef for a transport drop with no Close');
    ok(!$obs->{complete}, 'on_complete did NOT fire (abnormal end)');

    $park->done unless $park->is_ready;
    pump_until(sub { $obs->{app_returned} }, 3);
    shutdown_server($server);
};

# =============================================================================
# 4. Server-initiated close, peer never replies -> 1006, NOT the server's code.
# =============================================================================
subtest 'server-initiated close, peer silent -> close_code 1006, not the server code' => sub {
    my ($app, $obs, $park) = build_ws_app(
        pre_park => sub {
            my ($send) = @_;
            return $send->({ type => 'websocket.close', code => 1011, reason => 'server closing' });
        },
    );
    my $server = start_server($app);
    my $sock   = connect_client($server->port);

    like(ws_upgrade($sock), qr/HTTP\/1\.1 101/, 'websocket upgrade succeeded');
    ok(pump_until(sub { $obs->{parked} }, 5), 'app accepted, sent its close, and parked');

    # The peer never sends its own Close; it drops the transport instead.
    close($sock);

    ok(pump_until(sub { $obs->{disconnect} || $obs->{complete} }, 5),
        'a terminal callback fired');
    is($obs->{code},   1006,  'close_code is 1006 -- the peer never replied, never the server 1011');
    is($obs->{reason}, undef, 'close_reason is undef for a peer that never replied');

    $park->done unless $park->is_ready;
    pump_until(sub { $obs->{app_returned} }, 3);
    shutdown_server($server);
};

subtest 'a websocket refusal populates 1006 before its clean completion' => sub {
    my ($app, $seen) = build_refusing_ws_app();
    my $server = start_server($app);
    my $sock = connect_client($server->port);
    ws_refusal_request($sock);
    my ($wire, $eof) = ('', 0);
    ok(pump_until(sub {
        my $buf;
        my $n = sysread($sock, $buf, 4096);
        $wire .= $buf if defined $n && $n > 0;
        $eof = 1 if defined $n && $n == 0;
        return $eof && $seen->{returned} && $seen->{complete} && $seen->{end};
    }, 5), 'refusing application and callbacks completed');
    like($wire, qr/^HTTP\/1\.1 403 Forbidden\r\n/, 'refusal returned HTTP 403');
    my (undef, $body) = split /\r\n\r\n/, $wire, 2;
    is($body, 'Access denied', 'refusal returned the exact decoded body');
    is($seen->{before_body}, undef, 'no close metadata before refusal completion');
    is($seen->{at_complete}, [1006, undef, 1, 0, undef, undef], 'complete sees terminal facts');
    is($seen->{at_end}, [1006, undef, 1, 0, undef, undef], 'end sees terminal facts');
    is($seen->{after_end}, [1006, undef, 1, 0, undef, undef], 'end future sees terminal facts');
    is([@{$seen}{qw(complete end disconnect)}], [1, 1, 0], 'clean callback families');
    is($seen->{end_value}, undef, 'successful end future');
    is($seen->{receive_type}, 'http.disconnect', 'no synthetic WebSocket disconnect');
    close $sock;
    shutdown_server($server);
};

# =============================================================================
# 5. http and sse scopes -> close_code / close_reason undef at their clean end.
# =============================================================================
subtest 'http scope: close_code/close_reason undef at clean end' => sub {
    my %obs;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'http';
        my $conn = $scope->{'pagi.connection'};
        $conn->on_complete(sub {
            $obs{complete}++;
            $obs{code}   = $conn->close_code;
            $obs{reason} = $conn->close_reason;
        });
        await $send->({ type => 'http.response.start', status => 200, headers => [] });
        await $send->({ type => 'http.response.body', body => 'ok', more => 0 });
        return;
    };

    my $server = start_server($app);
    my $sock   = connect_client($server->port);

    syswrite($sock, "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    ok(pump_until(sub { $obs{complete} }, 5), 'on_complete fired for the http request');
    is($obs{code},   undef, 'close_code undef on an http scope');
    is($obs{reason}, undef, 'close_reason undef on an http scope');

    close($sock);
    shutdown_server($server);
};

subtest 'sse scope: close_code/close_reason undef at clean end' => sub {
    my %obs;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'sse';
        my $conn = $scope->{'pagi.connection'};
        $conn->on_complete(sub {
            $obs{complete}++;
            $obs{code}   = $conn->close_code;
            $obs{reason} = $conn->close_reason;
        });
        await $send->({ type => 'sse.start', status => 200, headers => [] });
        await $send->({ type => 'sse.send', data => 'hi' });
        await $send->({ type => 'sse.close' });
        return;
    };

    my $server = start_server($app);
    my $sock   = connect_client($server->port);

    syswrite($sock, "GET / HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n");
    ok(pump_until(sub { $obs{complete} }, 5), 'on_complete fired for the sse stream');
    is($obs{code},   undef, 'close_code undef on an sse scope');
    is($obs{reason}, undef, 'close_reason undef on an sse scope');

    close($sock);
    shutdown_server($server);
};

done_testing;
