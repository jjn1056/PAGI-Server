use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";

eval { require Future::IO::Impl::IOAsync; 1 }
    or plan skip_all => 'Future::IO::Impl::IOAsync required for SSE tests';

use PAGI::Server;
use IO::Socket::INET;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# Regression for sse.http.response.* (HTTP/1.1): before sse.start, an application
# may DECLINE the SSE stream and return a normal HTTP response (404/401/204/...).
# First-send-wins: stream events after a decline, and a decline after sse.start,
# MUST raise.

my $loop = IO::Async::Loop->new;

sub create_server {
    my ($app) = @_;
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, quiet => 1, shutdown_timeout => 1,
    );
    $loop->add($server);
    $server->listen->get;
    return $server;
}

sub sse_get {
    my ($port) = @_;
    my $sock = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5,
    ) or return ('', 0);
    print $sock "GET / HTTP/1.1\r\nHost: 127.0.0.1:$port\r\nAccept: text/event-stream\r\n\r\n";
    $sock->blocking(0);
    my $wire = '';
    my $eof  = 0;
    my $deadline = time + 5;
    while (time < $deadline) {
        my $buf;
        my $n = sysread($sock, $buf, 4096);
        if (defined $n && $n > 0) { $wire .= $buf }
        elsif (defined $n && $n == 0) { $eof = 1; last }
        $loop->loop_once(0.05);
    }
    close $sock;
    $loop->loop_once(0.05) for 1 .. 20;
    return ($wire, $eof);
}

subtest 'sse.http.response.* returns a plain HTTP 404 (declines the stream)' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $send->({ type => 'sse.http.response.start', status => 404,
                        headers => [['content-type', 'text/plain']] });
        await $send->({ type => 'sse.http.response.body', body => 'No such stream', more => 0 });
    };

    my $server = create_server($app);
    my ($wire, $eof) = sse_get($server->port);

    like($wire, qr{HTTP/1\.1 404},   '404 status line');
    like($wire, qr/No such stream/,  'decline body delivered');
    unlike($wire, qr{text/event-stream}, 'NOT an event stream');
    ok($eof, 'connection closed');

    $server->shutdown->get;
};

subtest '204 decline (stop-reconnect)' => sub {
    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $send->({ type => 'sse.http.response.start', status => 204, headers => [] });
        await $send->({ type => 'sse.http.response.body', body => '', more => 0 });
    };
    my $server = create_server($app);
    my ($wire, $eof) = sse_get($server->port);
    like($wire, qr{HTTP/1\.1 204}, '204 No Content');
    ok($eof, 'connection closed');
    $server->shutdown->get;
};

subtest 'first-send-wins: stream after decline, and decline after stream, raise' => sub {
    my ($after_decline_raised, $after_start_raised) = (0, 0);

    my $app1 = async sub {
        my ($scope, $receive, $send) = @_;
        await $send->({ type => 'sse.http.response.start', status => 404, headers => [] });
        eval { await $send->({ type => 'sse.send', data => 'x' }); 1 } or $after_decline_raised = 1;
        await $send->({ type => 'sse.http.response.body', body => '', more => 0 });
    };
    my $s1 = create_server($app1);
    sse_get($s1->port);
    ok($after_decline_raised, 'sse.send after sse.http.response.start raised');
    $s1->shutdown->get;

    my $app2 = async sub {
        my ($scope, $receive, $send) = @_;
        await $send->({ type => 'sse.start', status => 200 });
        eval { await $send->({ type => 'sse.http.response.start', status => 404, headers => [] }); 1 }
            or $after_start_raised = 1;
    };
    my $s2 = create_server($app2);
    sse_get($s2->port);
    ok($after_start_raised, 'sse.http.response.start after sse.start raised');
    $s2->shutdown->get;
};

subtest 'a completed decline delivers no sse.disconnect to a later receive() call' => sub {
    # Per the PAGI spec, a decline delivers no events at all. An app that
    # calls receive() again after its decline response has fully completed
    # (before it returns) must not observe a synthesized sse.disconnect --
    # that would look exactly like an abnormal end that never happened.
    my ($settled, $event);

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        await $send->({ type => 'sse.http.response.start', status => 404, headers => [] });
        await $send->({ type => 'sse.http.response.body', body => 'nope', more => 0 });

        # Decline is now complete. Call receive() once more, WITHOUT
        # awaiting it directly (an unresolved Future would hang this
        # coroutine forever under the fix) -- just observe, after a bounded
        # wait on the same loop, whether it ever resolved and with what.
        my $future = $receive->();
        await $loop->delay_future(after => 0.3);
        $settled = $future->is_ready;
        $event   = $future->is_ready ? $future->get : undef;
    };

    my $server = create_server($app);
    my ($wire, $eof) = sse_get($server->port);

    like($wire, qr{HTTP/1\.1 404}, 'decline delivered as usual');

    # Give the app's post-decline receive() its full bounded window to settle.
    $loop->loop_once(0.05) for 1 .. 10;

    if ($settled) {
        isnt($event->{type}, 'sse.disconnect',
            'a resolved receive() did not synthesize sse.disconnect');
    }
    else {
        ok(!$settled, 'receive() after a completed decline stayed pending (no event synthesized)');
    }

    $server->shutdown->get;
};

done_testing;
