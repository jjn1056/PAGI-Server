#!/usr/bin/env perl

# PAGI::Spec::Www 0.7, run_until_end, over HTTP/1.1: each scope type's end
# reaches a pending call. The contract itself is t/run-until-end-contract.t;
# this file checks only that the protocol delivers its end to the method:
# the client leaving (http, sse, websocket), a WebSocket client's polite
# Close, a client that stops reading while the work is parked in a held send,
# and a server shutdown. The HTTP/2 twin is t/http2/61-run-until-end.t.

use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
use MIME::Base64 ();
use Protocol::WebSocket::Frame;
use Socket qw(SO_RCVBUF);
use Future;
use Future::AsyncAwait;
use Time::HiRes qw(time);

use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

my $loop = IO::Async::Loop->new;
my @log;

my %REQUEST = (
    http      => "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n",
    sse       => "GET /events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n",
    websocket => "GET /socket HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\n"
        . "Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: "
        . MIME::Base64::encode_base64('0123456789abcdef', '') . "\r\n\r\n",
);

# Small watermarks so a body the client stops reading parks quickly.
sub start_server {
    my ($app) = @_;
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, quiet => 1, access_log => undef,
        shutdown_timeout => 1, logger => sub { push @log, $_[0] },
        write_high_watermark => 8192, write_low_watermark => 2048,
    );
    $loop->add($server);
    $server->listen->get;
    return $server;
}

sub stop_server {
    my ($server) = @_;
    $server->shutdown->get;
    $loop->remove($server);
}

sub pump_until {
    my ($cond, $secs) = @_;
    my $deadline = time + ($secs // 10);
    $loop->loop_once(0.01) until $cond->() || time > $deadline;
    return $cond->();
}

sub connect_client {
    my ($server, $request) = @_;
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $server->port, Proto => 'tcp', Timeout => 2,
    ) or die "connect: $!";
    $sock->sockopt(SO_RCVBUF, 4096);   # an unread response backs up quickly
    $sock->blocking(0);
    syswrite($sock, $request);
    return $sock;
}

# Starts the scope, then runs $work in run_until_end and records what the
# first line after the await sees.
sub app_running {
    my ($type, $obs, $work) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported scope type: $scope->{type}\n" unless $scope->{type} eq $type;
        my $conn = $scope->{'pagi.connection'};
        if    ($type eq 'websocket') { await $receive->(); await $send->({ type => 'websocket.accept' }) }
        elsif ($type eq 'sse')       { await $send->({ type => 'sse.start', status => 200 }) }
        else  { await $send->({ type => 'http.response.start', status => 200, headers => [['content-type', 'text/plain']] }) }
        $obs->{started} = 1;
        my $running;
        my @r = await $conn->run_until_end(sub { $running = $work->($send) });
        $obs->{seen} = { result => \@r, cancelled => $running->is_cancelled ? 1 : 0,
                         connected => $conn->is_connected, reason => $conn->disconnect_reason };
    };
}

sub waiting { $loop->delay_future(after => 30) }

for my $type (qw(http sse websocket)) {
    subtest "$type: the client leaves while the work waits" => sub {
        my %obs;
        my $server = start_server(app_running($type, \%obs, \&waiting));
        my $sock   = connect_client($server, $REQUEST{$type});
        ok(pump_until(sub { $obs{started} }), 'the scope started');
        close $sock;
        ok(pump_until(sub { $obs{seen} }), 'the call returned');
        # client_closed, or read_error when the close resets the connection over
        # bytes the client never read: either way an abnormal end.
        is($obs{seen}, { result => [], cancelled => 1, connected => 0, reason => match(qr/\A(?:client_closed|read_error)\z/) },
            'no value; the work cancelled on the first line after await; an abnormal end');
        stop_server($server);
    };
}

subtest 'websocket: a polite Close from the client is a clean end the call sees' => sub {
    my %obs;
    my $server = start_server(app_running('websocket', \%obs, \&waiting));
    my $sock   = connect_client($server, $REQUEST{websocket});
    ok(pump_until(sub { $obs{started} }), 'the socket was accepted');
    syswrite($sock, Protocol::WebSocket::Frame->new(
        type => 'close', buffer => pack('n', 1000), masked => 1)->to_bytes);
    ok(pump_until(sub { $obs{seen} }), 'the call returned');
    is($obs{seen}, { result => [], cancelled => 1, connected => 0, reason => undef },
        'no value; the work cancelled; a clean end');
    close $sock;
    stop_server($server);
};

subtest 'http: the work is parked in a send held by backpressure when the client leaves' => sub {
    my %obs = (sent => 0);
    my $flood = sub {
        my ($send) = @_;
        # Stops when the test says so: a server that never cancels it must fail
        # this subtest, not hang the file (once the server leaves the loop,
        # these sends no longer wait).
        return (async sub {
            until ($obs{stop}) { $obs{sent}++; await $send->({ type => 'http.response.body', body => 'x' x 1024, more => 1 }) }
        })->();
    };
    my $server = start_server(app_running('http', \%obs, $flood));
    my $sock   = connect_client($server, $REQUEST{http});   # reads nothing
    my ($last, $since) = (-1, time);
    ok(pump_until(sub {
        ($last, $since) = ($obs{sent}, time) if $obs{sent} != $last;
        return $obs{sent} > 0 && time - $since > 0.5;
    }, 15), 'the work is parked on a held send') or diag "sent=$obs{sent}";
    close $sock;
    ok(pump_until(sub { $obs{seen} }), 'the call returned');
    is($obs{seen}{result}, [], 'no value');
    is($obs{seen}{cancelled}, 1, 'the work cancelled');
    $obs{stop} = 1;
    stop_server($server);
};

subtest 'http: a server shutdown ends the scope, and the call sees it' => sub {
    my %obs;
    my $server = start_server(app_running('http', \%obs, \&waiting));
    my $sock   = connect_client($server, $REQUEST{http});
    ok(pump_until(sub { $obs{started} }), 'the response started');
    my $down = $server->shutdown;          # shutdown_timeout => 1: the stream is cut then
    ok(pump_until(sub { $obs{seen} }, 10), 'the call returned');
    is($obs{seen}, { result => [], cancelled => 1, connected => 0, reason => 'server_shutdown' },
        'no value; the work cancelled; server_shutdown');
    $down->get;
    $loop->remove($server);
    close $sock;
};

my @unexpected = grep { !/\A(?:listening|Using default max_connections|Shutdown timeout: force-closing)/ }
    map { $_->{message} // '' } @log;
is(\@unexpected, [], 'nothing unexpected logged');
done_testing;
