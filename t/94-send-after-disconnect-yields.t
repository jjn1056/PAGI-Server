#!/usr/bin/env perl

# PAGI::Spec "Send Completion Contract", over HTTP/1.1: a send issued after
# an abnormal end does no work and resolves successfully, but from the event
# loop, never within the call to $send. Its Future settles after the scope's
# terminal notifications, which were queued on the loop first.
#
# What that buys: a body whose only waits are sends, raced against
# disconnect_future, stops once the client leaves. Were each such send done
# at once, the body would never return to the loop, the news could not be
# delivered, and it would run to its end.

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

# The request opens an http scope unless one for another scope is given.
sub connect_client {
    my ($server, $request) = @_;
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $server->port, Proto => 'tcp', Timeout => 2,
    ) or die "connect: $!";
    $sock->sockopt(SO_RCVBUF, 4096);   # an unread response backs up quickly
    $sock->blocking(0);
    syswrite($sock, $request // "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n");
    return $sock;
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

subtest 'a send-only body raced against disconnect_future stops when the client leaves' => sub {
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

    my $server = start_server($app);
    my $sock   = connect_client($server);

    # The client reads nothing, so once the kernel's buffers are full the
    # body parks on backpressure: its count of sends stops moving for half a
    # second.
    my ($last, $since) = (-1, time);
    my $parked = pump_until(sub {
        ($last, $since) = ($obs{sent}, time) if $obs{sent} != $last;
        return $obs{sent} > 0 && time - $since > 0.5;
    }, 15);
    ok($parked, 'the body is parked on backpressure') or diag "sent=$obs{sent}";
    close $sock;

    ok(pump_until(sub { $obs{done} }, 30), 'the application finished');
    is($obs{export_cancelled}, 1, 'the body was cancelled, not run to its end')
        or diag "sent=$obs{sent} of 20000";
    cmp_ok($obs{sent_after_leave}, '<=', 1, 'at most one send after the client left')
        or diag "sent_after_leave=$obs{sent_after_leave}";

    stop_server($server);
};

subtest 'a body that starts after the client left stops at its first send' => sub {
    my %obs = (sent => 0, sent_after_leave => 0);
    my $gate = $loop->new_future;
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported scope type: $scope->{type}\n" unless $scope->{type} eq 'http';
        my $conn = $obs{conn} = $scope->{'pagi.connection'};
        await $send->({ type => 'http.response.start', status => 200,
            headers => [['content-type', 'text/plain']] });
        await $gate;   # a slow query, during which the client gives up
        my $export = export_body($send, $conn, \%obs);
        await Future->wait_any($export, $conn->disconnect_future);
        $obs{export_cancelled} = $export->is_cancelled ? 1 : 0;
        $obs{done} = 1;
    };

    my $server = start_server($app);
    my $sock   = connect_client($server);
    ok(pump_until(sub { $obs{conn} && $obs{conn}->response_started }), 'the response started');
    close $sock;
    ok(pump_until(sub { defined $obs{conn}->disconnect_reason }),
        'the server saw the client leave');

    $loop->later(sub { $gate->done });
    ok(pump_until(sub { $obs{done} }, 30), 'the application finished');
    is($obs{export_cancelled}, 1, 'the body was cancelled, not run to its end')
        or diag "sent=$obs{sent} of 20000";
    is($obs{sent_after_leave}, 1, 'it made one send, which did not settle at once');

    stop_server($server);
};

# The protocol scopes: each opens, its start is sent, and its body is a run
# of sends of its own kind.
my %SCOPE = (
    sse => {
        request => "GET /events HTTP/1.1\r\nHost: localhost\r\nAccept: text/event-stream\r\n\r\n",
        start   => { type => 'sse.start', status => 200 },
        chunk   => { type => 'sse.send', data => 'x' x 1024 },
    },
    websocket => {
        request => "GET /socket HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\n"
            . "Connection: Upgrade\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: "
            . MIME::Base64::encode_base64('0123456789abcdef', '') . "\r\n\r\n",
        start   => { type => 'websocket.accept' },
        chunk   => { type => 'websocket.send', text => 'x' x 1024 },
    },
);

for my $type (sort keys %SCOPE) {
    my $kind = $SCOPE{$type};
    subtest "a $type body that starts after the client left stops at its first send" => sub {
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

        my $server = start_server($app);
        my $sock   = connect_client($server, $kind->{request});
        ok(pump_until(sub { $obs{started} }), 'the stream started');
        close $sock;
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

# A clean end is no different: once a WebSocket's closing handshake, begun
# by the client, has completed and the connection has closed, a send settles
# from the loop too, and a body raced against end_future stops at its first.
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

    my $server = start_server($app);
    my $sock   = connect_client($server, $SCOPE{websocket}{request});
    ok(pump_until(sub { $obs{started} }), 'the socket was accepted');
    syswrite($sock, Protocol::WebSocket::Frame->new(
        type => 'close', buffer => pack('n', 1000), masked => 1)->to_bytes);
    ok(pump_until(sub { $obs{conn}->response_complete }), 'the socket ended cleanly');

    $loop->later(sub { $gate->done });
    ok(pump_until(sub { $obs{done} }, 30), 'the application finished');
    is($obs{export_cancelled}, 1, 'the body was cancelled, not run to its end')
        or diag "sent=$obs{sent} of 20000";
    is($obs{sent_after_end}, 1, 'it made one send, which did not settle at once');

    close $sock;
    stop_server($server);
};

# The closing window: the client's Close has arrived and the server has
# answered with its own, but the connection is not yet closed. Sends there
# do no work (RFC 6455 5.5.1 allows no data after a Close) and settle from
# the loop, so a body that is sending when the client says goodbye lets the
# server finish closing, and a race against end_future ends it.
subtest 'a websocket body sending when the client closes cleanly stops at once' => sub {
    my %obs = (sent => 0);
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        die "unsupported scope type: $scope->{type}\n" unless $scope->{type} eq 'websocket';
        my $conn = $obs{conn} = $scope->{'pagi.connection'};
        await $receive->();   # websocket.connect
        await $send->({ type => 'websocket.accept' });
        my $export = async sub {
            for my $n (1 .. 20_000) {
                $obs{sent}++;
                await $send->({ type => 'websocket.send', text => 'x' x 1024 });
            }
        }->();
        await Future->wait_any($export, $conn->end_future);
        $obs{export_cancelled} = $export->is_cancelled ? 1 : 0;
        $obs{done} = 1;
    };

    my $server = start_server($app);
    my $sock   = connect_client($server, $SCOPE{websocket}{request});
    my $reading = sub { 1 while (sysread($sock, my $buf, 65536) // 0) > 0; 0 };
    ok(pump_until(sub { $reading->() || $obs{sent} >= 200 }), 'the body is sending, the client reading');
    my $sent_at_close = $obs{sent};
    syswrite($sock, Protocol::WebSocket::Frame->new(
        type => 'close', buffer => pack('n', 1000), masked => 1)->to_bytes);

    ok(pump_until(sub { $reading->() || $obs{done} }, 30), 'the application finished');
    is($obs{export_cancelled}, 1, 'the body was cancelled, not run to its end')
        or diag "sent=$obs{sent} of 20000";
    cmp_ok($obs{sent} - $sent_at_close, '<=', 10, 'a few sends after the Close, not the rest')
        or diag "sent after the Close: " . ($obs{sent} - $sent_at_close);
    ok($obs{conn}->response_complete, 'the socket ended cleanly');

    close $sock;
    stop_server($server);
};

is(\@log, [], 'nothing logged');

done_testing;
