#!/usr/bin/env perl

# =============================================================================
# Test: Lifespan scope includes worker fields (PAGI spec compliance)
#
# Per lifespan.mkdn:
# - pagi["is_worker"] (Int, optional) - 1 if running as a worker, 0 otherwise
# - pagi["worker_num"] (Int, optional) - Worker identifier (1, 2, 3, ...)
# =============================================================================

use strict;
use warnings;
use Test2::V0;
use IO::Async::Loop;
use Future::AsyncAwait;
use FindBin;
use lib "$FindBin::Bin/../lib";
use PAGI::Server;
use File::Temp qw(tempfile);
use POSIX qw(WNOHANG);
use Time::HiRes qw(sleep time);

subtest 'lifespan scope includes worker fields' => sub {
    # Read the Server.pm source
    my $source = do {
        open my $fh, '<', 'lib/PAGI/Server.pm' or die "Cannot read: $!";
        local $/;
        <$fh>;
    };

    # Find the lifespan scope creation
    like(
        $source,
        qr/type\s*=>\s*'lifespan'/,
        'lifespan scope creation exists'
    );

    # Verify is_worker is included in pagi hashref
    like(
        $source,
        qr/is_worker\s*=>\s*\$self->\{is_worker\}/,
        'is_worker field is included in pagi hashref'
    );

    # Verify worker_num is included in pagi hashref
    like(
        $source,
        qr/worker_num\s*=>\s*\$self->\{worker_num\}/,
        'worker_num field is included in pagi hashref'
    );
};

subtest 'worker fields are set during worker process creation' => sub {
    my $source = do {
        open my $fh, '<', 'lib/PAGI/Server.pm' or die "Cannot read: $!";
        local $/;
        <$fh>;
    };

    # Verify is_worker is set to 1 in worker process
    like(
        $source,
        qr/\{is_worker\}\s*=\s*1/,
        'is_worker is set to 1 in worker process'
    );

    # Verify worker_num is assigned from the worker number parameter
    like(
        $source,
        qr/\{worker_num\}\s*=\s*\$worker_num/,
        'worker_num is assigned from worker number'
    );
};

# Behavioral conformance: heartbeat_timeout is read by the worker process
# itself (it drives how often the worker writes a liveness ping -- see
# t/47-worker-heartbeat.t and the heartbeat_timeout POD). _run_as_worker
# constructs a fresh PAGI::Server for the worker process; if
# heartbeat_timeout isn't threaded into that constructor call, the worker's
# own server object silently carries the wrong (default) value even though
# the running process happens to still behave correctly today via a
# closure over the master's (forked) $self. That's a representational trap
# for anyone who later reads heartbeat_timeout off the worker object
# instead of the closure. Assert the constructor call propagates it, and
# that the interval calculation reads it from the worker's own object.
subtest 'heartbeat_timeout propagates into the worker constructor' => sub {
    my $source = do {
        open my $fh, '<', 'lib/PAGI/Server.pm' or die "Cannot read: $!";
        local $/;
        <$fh>;
    };

    like(
        $source,
        qr/heartbeat_timeout\s*=>\s*\$self->\{heartbeat_timeout\},\s*\n\s*lifespan_mode/,
        'heartbeat_timeout is passed into the worker PAGI::Server constructor call'
    );

    like(
        $source,
        qr/my \$interval = \(\$worker_server->\{heartbeat_timeout\} \|\| 50\) \/ 5;/,
        'the worker heartbeat-writer interval reads heartbeat_timeout from the worker object, not a closure over the master'
    );
};

# Behavioral conformance: per PAGI::Spec::Lifespan the lifespan scope's `state`
# is a HashRef (or omitted if unsupported) -- never undef. PAGI::Server supports
# it, so it must always be a defined, writable HashRef the app populates.
subtest 'lifespan scope state is a HashRef the app can populate' => sub {
    my ($state_defined, $state_ref, $write_ok);

    my $app = async sub {
        my ($scope, $receive, $send) = @_;
        return unless $scope->{type} eq 'lifespan';

        $state_defined = defined($scope->{state}) ? 1 : 0;
        $state_ref     = ref($scope->{state});
        # The spec calls state "a namespace where the application can persist
        # values" -- so it must be a live, writable HashRef.
        $write_ok = eval { $scope->{state}{ready} = 1; 1 } ? 1 : 0;

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
    };

    my $loop   = IO::Async::Loop->new;
    my $server = PAGI::Server->new(app => $app, port => 0, quiet => 1);
    $loop->add($server);
    $server->listen->get;

    ok($state_defined, 'lifespan scope state is defined (never undef)');
    is($state_ref, 'HASH', 'lifespan scope state is a HashRef');
    ok($write_ok, 'app can persist values into state');

    $server->shutdown->get;
    $loop->remove($server);
};

# Behavioral conformance: workers must inherit lifespan_mode (and
# lifespan_startup_timeout) from the master, not silently fall back to the
# 'auto' default. With lifespan_mode => 'on', an app that declines the
# lifespan protocol (dies on the lifespan scope without ever signalling
# startup) makes startup fatal per-worker (see t/17-worker-respawn-loop.t
# idiom: worker exits with "startup failed" logged, exit code 2, no
# respawn). Once every worker has failed and none were respawned, the
# master gives up and exits (see PAGI::Server::_spawn_worker's
# "All workers have exited and none were respawned" handling).
subtest 'workers inherit lifespan_mode strict enforcement from the master' => sub {
    skip_all('Fork tests not supported on Windows') if $^O eq 'MSWin32';

    my ($stderr_fh, $stderr_file) = tempfile(UNLINK => 1);

    my $declining_app = async sub {
        my ($scope, $receive, $send) = @_;
        if ($scope->{type} eq 'lifespan') {
            die "app declines lifespan support\n";
        }
        return;
    };

    my $master_pid = fork();
    die "Fork failed: $!" unless defined $master_pid;

    # Put the master in its own process group so that, if it must be
    # force-killed below (the safety net for a master that fails to honor
    # strict lifespan enforcement and never exits on its own), the whole
    # group -- master and any worker children it forked -- can be reaped in
    # one shot. Otherwise an orphaned worker keeps this test script's
    # inherited STDOUT pipe open and `prove` hangs waiting for EOF that
    # never comes. Set from both sides to avoid the fork/setpgid race.
    if ($master_pid) {
        eval { POSIX::setpgid($master_pid, $master_pid) };
    }

    if ($master_pid == 0) {
        eval { POSIX::setpgid(0, 0) };

        # Child: run the multi-worker master with STDOUT/STDERR redirected
        # to a file so the parent (this test) can inspect what the workers
        # and master logged, and so this fork doesn't keep the test
        # harness's TAP pipe open.
        open STDOUT, '>&', $stderr_fh or die "Can't redirect STDOUT: $!";
        open STDERR, '>&', $stderr_fh or die "Can't redirect STDERR: $!";
        STDOUT->autoflush(1);
        STDERR->autoflush(1);

        my $child_loop = IO::Async::Loop->new;
        my $server = PAGI::Server->new(
            app                      => $declining_app,
            host                     => '127.0.0.1',
            port                     => 0,
            workers                  => 2,
            lifespan_mode            => 'on',
            lifespan_startup_timeout => 5,
            quiet                    => 0,
        );
        $child_loop->add($server);
        eval {
            $server->listen->get;
            $child_loop->run;
        };
        exit(0);
    }

    # Parent: loop-poll (bounded, generous) for the master to exit on its
    # own once every worker has failed lifespan startup. This host can be
    # slow, so the bound is generous rather than tuned to the happy path.
    my $deadline = time() + 20;
    my $reaped   = 0;
    while (time() < $deadline) {
        my $seen = waitpid($master_pid, WNOHANG);
        if ($seen == $master_pid) { $reaped = 1; last; }
        sleep(0.1);
    }

    unless ($reaped) {
        kill('KILL', -$master_pid);  # whole process group: master + workers
        waitpid($master_pid, 0);
    }

    ok($reaped, 'master process exits on its own once all workers fail lifespan startup');

    seek($stderr_fh, 0, 0);
    local $/;
    my $stderr_output = readline($stderr_fh) // '';

    like(
        $stderr_output,
        qr/startup failed/,
        'worker(s) logged a lifespan startup failure (strict mode propagated)'
    );
    like(
        $stderr_output,
        qr/All workers have exited and none were respawned/,
        'master gave up after every worker failed startup'
    );
};

done_testing;
