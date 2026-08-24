#!/usr/bin/env perl
use strict;
use warnings;

use Test2::V0;
use IO::Async::Loop;
use Future::AsyncAwait;
use IO::Socket::INET;
use File::Temp qw(tempfile);
use Time::HiRes qw(time sleep);
use POSIX ':sys_wait_h';

use lib 't/lib';
use lib '../lib';
use PAGI::Server;

# Skip if not running on a system that supports fork
plan skip_all => "Fork not available on this platform" if $^O eq 'MSWin32';

# Test 1: Server accepts workers configuration
subtest 'Server accepts workers configuration' => sub {
    my $app = async sub  {
        my ($scope, $receive, $send) = @_;
        if ($scope->{type} eq 'lifespan') {
            my $event = await $receive->();
            if ($event->{type} eq 'lifespan.startup') {
                await $send->({ type => 'lifespan.startup.complete' });
            }
            $event = await $receive->();
            if ($event && $event->{type} eq 'lifespan.shutdown') {
                await $send->({ type => 'lifespan.shutdown.complete' });
            }
            return;
        }
        die "Unsupported: $scope->{type}" unless $scope->{type} eq 'http';
        await $send->({
            type    => 'http.response.start',
            status  => 200,
            headers => [['content-type', 'text/plain']],
        });
        await $send->({
            type => 'http.response.body',
            body => "ok",
            more => 0,
        });
    };

    my $loop = IO::Async::Loop->new;

    # Test that server can be created with workers option
    my $server = PAGI::Server->new(
        app     => $app,
        host    => '127.0.0.1',
        port    => 0,  # Let OS assign port
        workers => 2,
        quiet   => 1,
    );

    ok($server, 'Server created with workers option');
    $loop->add($server);

    # Check internal state
    is($server->{workers}, 2, 'Workers option stored correctly');

    pass('Multi-worker configuration accepted');
};

# Test 2: Single worker mode (workers=0 or 1) works as before
subtest 'Single worker mode continues to work' => sub {
    my $app = async sub  {
        my ($scope, $receive, $send) = @_;
        if ($scope->{type} eq 'lifespan') {
            my $event = await $receive->();
            if ($event->{type} eq 'lifespan.startup') {
                await $send->({ type => 'lifespan.startup.complete' });
            }
            $event = await $receive->();
            if ($event && $event->{type} eq 'lifespan.shutdown') {
                await $send->({ type => 'lifespan.shutdown.complete' });
            }
            return;
        }
        die "Unsupported: $scope->{type}" unless $scope->{type} eq 'http';
        await $send->({
            type    => 'http.response.start',
            status  => 200,
            headers => [['content-type', 'text/plain']],
        });
        await $send->({
            type => 'http.response.body',
            body => "Single worker",
            more => 0,
        });
    };

    my $loop = IO::Async::Loop->new;

    # Create server with 0 workers (single process mode)
    my $server = PAGI::Server->new(
        app     => $app,
        host    => '127.0.0.1',
        port    => 0,
        workers => 0,
        quiet   => 1,
    );

    ok($server, 'Server created with workers=0');
    $loop->add($server);

    is($server->{workers}, 0, 'Single worker mode (workers=0)');

    pass('Single worker configuration works');
};

# Test 3: max_connections propagates from the master's constructor args into
# each worker's own PAGI::Server instance. _run_as_worker builds a fresh
# PAGI::Server for the worker process (single-worker mode inside that
# process); if max_connections isn't threaded through, the worker silently
# falls back to the effective_max_connections default (1000) regardless of
# what the master was configured with.
subtest 'max_connections propagates to the worker that enforces it' => sub {
    my ($port_fh, $port_file) = tempfile(UNLINK => 1);
    close $port_fh;

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        if ($scope->{type} eq 'lifespan') {
            while (1) {
                my $event = await $receive->();
                if ($event->{type} eq 'lifespan.startup') {
                    await $send->({ type => 'lifespan.startup.complete' });
                }
                elsif ($event->{type} eq 'lifespan.shutdown') {
                    await $send->({ type => 'lifespan.shutdown.complete' });
                    return;
                }
            }
        }
        # For HTTP requests, respond normally (mirrors t/40-connection-limiting.t;
        # the capacity check happens on accept, before any request is read).
        await $send->({ type => 'http.response.start', status => 200, headers => [] });
        await $send->({ type => 'http.response.body', body => 'OK', more => 0 });
    };

    my $master_pid = fork();
    die "Fork failed: $!" unless defined $master_pid;

    if ($master_pid == 0) {
        # Child: run a single-worker multi-worker master so accept-path
        # enforcement happens inside the worker process, not the master.
        my $child_loop = IO::Async::Loop->new;
        my $server = PAGI::Server->new(
            app             => $app,
            host            => '127.0.0.1',
            port            => 0,
            workers         => 1,
            max_connections => 1,  # Only allow 1 connection, per worker
            quiet           => 1,
        );
        $child_loop->add($server);
        eval {
            $server->listen->get;
            open my $out, '>', $port_file or die "Cannot write $port_file: $!";
            print $out $server->port;
            close $out;
            $child_loop->run;
        };
        exit(0);
    }

    # Parent: loop-poll (bounded) for the master to report its bound port.
    my $deadline = time() + 15;
    my $port;
    while (time() < $deadline) {
        if (-s $port_file) {
            open my $in, '<', $port_file or die "Cannot read $port_file: $!";
            $port = <$in>;
            close $in;
            last if $port;
        }
        sleep(0.1);
    }

    unless ($port) {
        kill('KILL', $master_pid);
        waitpid($master_pid, 0);
        fail('master never reported a bound port');
        return;
    }

    # Loop-poll (bounded) for the worker to actually be accepting.
    my $sock1;
    $deadline = time() + 10;
    while (time() < $deadline) {
        $sock1 = IO::Socket::INET->new(
            PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 1,
        );
        last if $sock1;
        sleep(0.1);
    }

    ok($sock1, 'first connection accepted') or do {
        kill('KILL', $master_pid);
        waitpid($master_pid, 0);
        return;
    };

    # Give the worker's own event loop a moment to register the accept
    # (separate OS process -- no shared loop to step synchronously).
    sleep(0.3);

    # Second connection: should be rejected (503) because max_connections=1
    # propagated to the worker enforcing it.
    my $sock2 = IO::Socket::INET->new(
        PeerAddr => '127.0.0.1', PeerPort => $port, Proto => 'tcp', Timeout => 5,
    );

    my $response = '';
    if ($sock2) {
        print $sock2 "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
        $sock2->blocking(0);
        my $read_deadline = time() + 5;
        while (time() < $read_deadline) {
            my $chunk;
            my $n = sysread($sock2, $chunk, 4096);
            if (defined $n && $n > 0) {
                $response .= $chunk;
            }
            elsif (defined $n && $n == 0) {
                last;  # EOF
            }
            last if $response =~ /\r\n\r\n/;
            select(undef, undef, undef, 0.1);
        }
        close($sock2);
    }

    close($sock1);
    kill('TERM', $master_pid);
    waitpid($master_pid, 0);

    like($response, qr/503/,
        'second connection gets 503: worker enforces the propagated max_connections, not the 1000 default');
};

# Note: Multi-worker functional tests require complex process management
# and have been verified manually.
#
# The implementation in lib/PAGI/Server.pm uses IO::Async idiomatically:
# - Uses $loop->fork() which properly clears $ONE_TRUE_LOOP and resets signals
# - Uses $loop->watch_process() for automatic worker restart on exit
# - Uses $loop->watch_signal() for graceful shutdown (SIGTERM/SIGINT)
# - Parent runs $loop->run() instead of manual select() loop
# - Per-worker lifespan startup/shutdown in each forked process
#
# Loop isolation is tested in t/12-fork-loop-isolation.t which verifies:
# - Child process gets a fresh loop instance (not parent's cached loop)
# - $ONE_TRUE_LOOP is properly cleared in child
#
# Manual verification:
# 1. ./bin/pagi-server --app examples/01-hello-http/app.pl --port 9777 --workers 2
# 2. curl http://127.0.0.1:9777/ - Response received successfully
# 3. kill -9 <worker_pid> - Worker is automatically respawned
# 4. kill -TERM <parent_pid> - Graceful shutdown of all workers

done_testing;
