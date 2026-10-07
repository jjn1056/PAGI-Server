use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use IO::Socket::INET;
use Future;
use Future::AsyncAwait;
use Time::HiRes ();
use FindBin;
use lib "$FindBin::Bin/../lib";

use PAGI::Server;

plan skip_all => "Server integration tests not supported on Windows" if $^O eq 'MSWin32';

# A shutting-down server waits, until its shutdown_timeout deadline, for each
# connection's application to return before it sends lifespan.shutdown, so
# cleanup an application starts on its disconnect (saving state, telling
# another service) can finish. An application still running at the deadline is
# left behind with one warn line. HTTP/2: t/http2/shutdown-waits-for-apps-h2.t.

my $loop = IO::Async::Loop->new;

sub pump_until {
    my ($cond, $timeout) = @_;
    my $deadline = Time::HiRes::time() + ($timeout // 10);
    while (Time::HiRes::time() < $deadline) {
        return 1 if $cond->();
        $loop->loop_once(0.02);
    }
    return $cond->() ? 1 : 0;
}

sub start_server {
    my ($app, %opt) = @_;
    my $server = PAGI::Server->new(
        app => $app, host => '127.0.0.1', port => 0, access_log => undef,
        shutdown_timeout => $opt{shutdown_timeout} // 5,
        log_level => 'warn', logger => $opt{logger} // sub { },
    );
    $loop->add($server);
    $server->listen->get;
    return $server;
}

sub ws_connect {
    my ($port) = @_;
    my $sock = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp')
        or die "connect: $!";
    $sock->blocking(0);
    syswrite($sock, "GET / HTTP/1.1\r\nHost: localhost\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
        . "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\nSec-WebSocket-Version: 13\r\n\r\n");
    my $got = '';
    pump_until(sub { my $n = sysread($sock, my $buf, 4096); $got .= $buf if $n; $got =~ /\r\n\r\n/ }, 5);
    return $sock;
}

# Runs shutdown to completion, reading the client side so the connection can
# close; returns how long it took.
sub shut_down {
    my ($server, @socks) = @_;
    my $started = Time::HiRes::time();
    my $done = $server->shutdown;
    pump_until(sub {
        sysread($_, my $buf, 65536) for @socks;
        $done->is_ready;
    }, 15);
    my $took = Time::HiRes::time() - $started;
    eval { $loop->remove($server) };
    return ($done, $took);
}

# Records the order of what happens. Each WebSocket's cleanup after its
# disconnect waits $cleanup seconds (a database write, say) before it returns;
# undef never returns.
sub app_with_cleanup {
    my ($order, $cleanup) = @_;
    return async sub {
        my ($scope, $receive, $send) = @_;
        if ($scope->{type} eq 'lifespan') {
            while (1) {
                my $event = await $receive->();
                if ($event->{type} eq 'lifespan.startup') {
                    await $send->({ type => 'lifespan.startup.complete' });
                }
                elsif ($event->{type} eq 'lifespan.shutdown') {
                    push @$order, 'lifespan.shutdown';
                    await $send->({ type => 'lifespan.shutdown.complete' });
                    return;
                }
            }
        }
        return unless $scope->{type} eq 'websocket';
        await $receive->();
        await $send->({ type => 'websocket.accept' });
        push @$order, 'accepted';
        while (1) {
            my $event = await $receive->();
            next unless $event->{type} eq 'websocket.disconnect';
            push @$order, "disconnect:$event->{reason}";
            if (defined $cleanup) {
                await $loop->delay_future(after => $cleanup);
                push @$order, 'cleanup finished';
            }
            else {
                await $loop->new_future;        # never returns
            }
            return;
        }
    };
}

subtest 'cleanup that awaits after a shutdown disconnect finishes before lifespan.shutdown' => sub {
    my @order;
    my $server = start_server(app_with_cleanup(\@order, 0.3));
    my @socks = map { ws_connect($server->port) } 1 .. 2;
    ok(pump_until(sub { (grep { $_ eq 'accepted' } @order) == 2 }, 5), 'two WebSockets accepted');

    my ($done, $took) = shut_down($server, @socks);
    ok($done->is_ready, 'shutdown completed');
    is([grep { $_ ne 'accepted' } @order],
       ['disconnect:server_shutdown', 'disconnect:server_shutdown',
        'cleanup finished', 'cleanup finished', 'lifespan.shutdown'],
       'both cleanups finished, then lifespan.shutdown');
    ok($took < 2, sprintf('promptly once they did (%.2fs)', $took));
};

subtest 'an application that never returns is left at the shutdown_timeout deadline' => sub {
    my (@order, @logs);
    my $server = start_server(app_with_cleanup(\@order, undef),
        shutdown_timeout => 1, logger => sub { push @logs, $_[0] });
    my $sock = ws_connect($server->port);
    ok(pump_until(sub { grep { $_ eq 'accepted' } @order }, 5), 'accepted');

    my ($done, $took) = shut_down($server, $sock);
    ok($done->is_ready, 'shutdown completed');
    ok($took >= 0.9 && $took < 3, sprintf('at the deadline (%.2fs, shutdown_timeout 1)', $took));
    is([grep { $_ eq 'lifespan.shutdown' } @order], ['lifespan.shutdown'], 'lifespan.shutdown still ran');
    my @warns = grep { ($_->{level} // '') eq 'warn' && ($_->{message} // '') =~ /still running/ } @logs;
    is(scalar @warns, 1, 'one warn line');
    like($warns[0]{message} // '', qr/1 application/, 'naming how many');
};

subtest 'a server with nothing open still shuts down at once' => sub {
    my @order;
    my $server = start_server(app_with_cleanup(\@order, 0.3));
    my ($done, $took) = shut_down($server);
    ok($done->is_ready, 'shutdown completed');
    ok($took < 0.5, sprintf('promptly (%.2fs)', $took));
    is(\@order, ['lifespan.shutdown'], 'lifespan.shutdown ran');
};

done_testing;
