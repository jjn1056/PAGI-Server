#!/usr/bin/env perl
use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use Future::AsyncAwait;
use Net::Async::HTTP;

use lib 'lib';
use IO::Socket::INET;
use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# Test: Worker restarts after max_requests
subtest 'worker restarts after max_requests' => sub {
    # This test would verify worker restarts in multi-worker mode
    # Skipping for now due to complexity of testing async multi-process behavior
    # The feature is implemented and can be tested manually with:
    # pagi-server --workers 2 --max-requests 3 app.pl
    plan skip_all => 'Multi-worker restart test skipped (complex timing/async issues)';
};

# Test: max_requests=0 means unlimited
subtest 'max_requests 0 means unlimited' => sub {
    my $server = PAGI::Server->new(
        app => async sub  {
        my ($scope, $receive, $send) = @_;
            await $send->({ type => 'http.response.start', status => 200, headers => [] });
            await $send->({ type => 'http.response.body', body => 'OK' });
        },
        port => 0,
        quiet => 1,
        max_requests => 0,  # Unlimited
    );

    is($server->{max_requests}, 0, 'max_requests stored as 0');
    is($server->{_request_count}, undef, 'no request counter initialized (single-worker)');
};

# Test: max_requests ignored in single-worker mode
subtest 'max_requests ignored in single worker mode' => sub {
    my $loop = IO::Async::Loop->new;
    my $server = PAGI::Server->new(
        app => async sub  {
        my ($scope, $receive, $send) = @_;
            # Handle lifespan events
            if ($scope->{type} eq 'lifespan') {
                while (1) {
                    my $event = await $receive->();
                    if ($event->{type} eq 'lifespan.startup') {
                        await $send->({ type => 'lifespan.startup.complete' });
                    }
                    elsif ($event->{type} eq 'lifespan.shutdown') {
                        await $send->({ type => 'lifespan.shutdown.complete' });
                        last;
                    }
                }
                return;
            }
            # Handle HTTP requests
            await $send->({ type => 'http.response.start', status => 200, headers => [] });
            await $send->({ type => 'http.response.body', body => 'OK' });
        },
        port => 0,
        quiet => 1,
        workers => 0,  # Single-worker
        max_requests => 5,
    );
    $loop->add($server);
    $server->listen->get;

    my $port = $server->port;

    my $http = Net::Async::HTTP->new;
    $loop->add($http);

    # Make 10 requests - server should not restart
    for (1..10) {
        my $response = $http->GET("http://127.0.0.1:$port/")->get;
        is($response->code, 200, "Request $_ succeeded");
    }

    ok($server->is_running, 'Server still running after 10 requests');

    $server->shutdown->get;
    $loop->remove($server);
    $loop->remove($http);
};

# Test: _on_request_complete increments counter correctly
subtest '_on_request_complete increments counter' => sub {
    my $loop = IO::Async::Loop->new;
    my $server = PAGI::Server->new(
        app => async sub  {
        my ($scope, $receive, $send) = @_;
            if ($scope->{type} eq 'lifespan') {
                my $msg = await $receive->();
                if ($msg->{type} eq 'lifespan.startup') {
                    await $send->({ type => 'lifespan.startup.complete' });
                }
                return;
            }
            await $send->({ type => 'http.response.start', status => 200, headers => [] });
            await $send->({ type => 'http.response.body', body => 'OK' });
        },
        port => 0,
        quiet => 1,
        max_requests => 10,
    );

    # Simulate worker mode
    $server->{is_worker} = 1;
    $server->{_request_count} = 0;

    # Call _on_request_complete directly
    $server->_on_request_complete;
    is($server->{_request_count}, 1, 'counter incremented to 1');

    $server->_on_request_complete;
    is($server->{_request_count}, 2, 'counter incremented to 2');

    # Verify no shutdown triggered yet (count < max)
    ok(!$server->{_max_requests_shutdown_triggered}, 'shutdown not triggered at count 2');
};

# Helper: perform one SSE GET request over a fresh raw socket, forcing the
# close fate (Connection: close) so each request lands on its own
# connection, per the brief's "two sequential complete SSE requests on
# separate connections" semantics. Reads until the chunked terminator so the
# stream has actually ended before returning (i.e. the SSE "request
# completes" point, not sse.start).
sub _sse_request_over_fresh_connection {
    my ($loop, $port) = @_;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1',
        PeerPort => $port,
        Proto    => 'tcp',
        Timeout  => 5,
    );
    die "Cannot connect to server" unless $sock;

    print $sock "GET / HTTP/1.1\r\n";
    print $sock "Host: 127.0.0.1:$port\r\n";
    print $sock "Accept: text/event-stream\r\n";
    print $sock "Connection: close\r\n";
    print $sock "\r\n";

    $sock->blocking(0);
    my $response = '';
    my $deadline = time + 5;
    while (time < $deadline) {
        my $buf;
        my $n = sysread($sock, $buf, 4096);
        $response .= $buf if defined $n && $n > 0;
        last if $response =~ /0\r\n\r\n\z/;
        $loop->loop_once(0.1);
    }
    close $sock;

    return $response;
}

# Test: max_requests counts h1 SSE requests (today it does not, so a worker
# serving only SSE traffic never recycles).
subtest 'max_requests counts h1 SSE requests' => sub {
    my $loop = IO::Async::Loop->new;
    my $server = PAGI::Server->new(
        app => async sub  {
            my ($scope, $receive, $send) = @_;
            if ($scope->{type} eq 'lifespan') {
                while (1) {
                    my $event = await $receive->();
                    if ($event->{type} eq 'lifespan.startup') {
                        await $send->({ type => 'lifespan.startup.complete' });
                    }
                    elsif ($event->{type} eq 'lifespan.shutdown') {
                        await $send->({ type => 'lifespan.shutdown.complete' });
                        last;
                    }
                }
                return;
            }
            if ($scope->{type} eq 'sse') {
                await $send->({ type => 'sse.start', status => 200, headers => [] });
                await $send->({ type => 'sse.send', event => 'msg', data => 'hi' });
                return;
            }
            await $send->({ type => 'http.response.start', status => 200, headers => [] });
            await $send->({ type => 'http.response.body', body => 'OK' });
        },
        host             => '127.0.0.1',
        port             => 0,
        quiet            => 1,
        shutdown_timeout => 1,
        max_requests     => 2,
    );
    $loop->add($server);
    $server->listen->get;
    my $port = $server->port;

    # Simulate worker mode (the real thing runs this in a forked worker;
    # in-process is sufficient to exercise the counting path).
    $server->{is_worker} = 1;
    $server->{_request_count} = 0;

    my $first = _sse_request_over_fresh_connection($loop, $port);
    like($first, qr/event: msg.*data: hi/s, 'first SSE stream delivered its event');
    is($server->{_request_count}, 1, 'counter incremented after first SSE stream ends');
    ok(!$server->{_max_requests_shutdown_triggered}, 'shutdown not triggered after 1 of 2');

    my $second = _sse_request_over_fresh_connection($loop, $port);
    like($second, qr/event: msg.*data: hi/s, 'second SSE stream delivered its event');

    # Bounded wait: the shutdown trigger fires synchronously inside
    # _on_request_complete, but give the loop a chance to process pending
    # work rather than asserting a happy-path-sized sleep.
    my $deadline = time + 5;
    until ($server->{_max_requests_shutdown_triggered} || time >= $deadline) {
        $loop->loop_once(0.1);
    }

    is($server->{_request_count}, 2, 'counter incremented after second SSE stream ends');
    ok($server->{_max_requests_shutdown_triggered}, 'worker begins graceful shutdown after max_requests SSE streams');

    # Detach the server (and its listener) from the loop so this test's
    # resources don't linger into later subtests.
    $loop->remove($server);
};

# Test: max_requests counts h1 WebSocket requests (today it does not).
subtest 'max_requests counts h1 WebSocket requests' => sub {
    my $loop = IO::Async::Loop->new;
    my $server = PAGI::Server->new(
        app => async sub  {
            my ($scope, $receive, $send) = @_;
            if ($scope->{type} eq 'lifespan') {
                while (1) {
                    my $event = await $receive->();
                    if ($event->{type} eq 'lifespan.startup') {
                        await $send->({ type => 'lifespan.startup.complete' });
                    }
                    elsif ($event->{type} eq 'lifespan.shutdown') {
                        await $send->({ type => 'lifespan.shutdown.complete' });
                        last;
                    }
                }
                return;
            }
            if ($scope->{type} eq 'websocket') {
                await $send->({ type => 'websocket.accept' });
                await $send->({ type => 'websocket.close', code => 1000 });
                return;
            }
            await $send->({ type => 'http.response.start', status => 200, headers => [] });
            await $send->({ type => 'http.response.body', body => 'OK' });
        },
        host             => '127.0.0.1',
        port             => 0,
        quiet            => 1,
        shutdown_timeout => 1,
        max_requests     => 1,
    );
    $loop->add($server);
    $server->listen->get;
    my $port = $server->port;

    $server->{is_worker} = 1;
    $server->{_request_count} = 0;

    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1',
        PeerPort => $port,
        Proto    => 'tcp',
        Timeout  => 5,
    );
    die "Cannot connect to server" unless $sock;

    print $sock "GET / HTTP/1.1\r\n";
    print $sock "Host: 127.0.0.1:$port\r\n";
    print $sock "Upgrade: websocket\r\n";
    print $sock "Connection: Upgrade\r\n";
    print $sock "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n";
    print $sock "Sec-WebSocket-Version: 13\r\n";
    print $sock "\r\n";

    $sock->blocking(0);
    my $response = '';
    my $deadline = time + 5;
    while (time < $deadline) {
        my $buf;
        my $n = sysread($sock, $buf, 4096);
        $response .= $buf if defined $n && $n > 0;
        last if $response =~ /\r\n\r\n/;
        $loop->loop_once(0.1);
    }
    close $sock;

    like($response, qr/101 Switching Protocols/i, 'WebSocket handshake accepted');

    $deadline = time + 5;
    until ($server->{_max_requests_shutdown_triggered} || time >= $deadline) {
        $loop->loop_once(0.1);
    }

    is($server->{_request_count}, 1, 'counter incremented after WebSocket session ends');
    ok($server->{_max_requests_shutdown_triggered}, 'worker begins graceful shutdown after max_requests WebSocket session');

    $loop->remove($server);
};

done_testing;
