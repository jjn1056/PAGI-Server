#!/usr/bin/env perl
use strict;
use warnings;
use Test2::V0;
use POSIX ();
use IO::Socket::INET;
use File::Spec;
use Cwd qw(abs_path);
use Test::TempDir::Tiny;

BEGIN { require FindBin; }

plan skip_all => 'Not supported on Windows' if $^O eq 'MSWin32';

# The --pid file names the running master for as long as it runs, so an
# operator (or systemd's PIDFile=) can signal it: workers ending, being
# replaced or recycled must leave it alone; the master removes it when it
# stops; after a hot restart it names the new master.

# A re-executed master (hot restart) runs $^X $0 without -Ilib.
my $lib_dir = abs_path(File::Spec->catdir($FindBin::Bin, '..', 'lib'));
$ENV{PERL5LIB} = join(':', grep { defined } $lib_dir, $ENV{PERL5LIB});
my $bin = abs_path(File::Spec->catfile($FindBin::Bin, '..', 'bin', 'pagi-server'));

# Server shutdown signals its process group; each server gets its own.
$SIG{TERM} = 'IGNORE';

my @groups;
END {
    kill('TERM', -$_) for grep { kill(0, $_) } @groups;
    select(undef, undef, undef, 0.5) if @groups;
    kill('KILL', -$_) for grep { kill(0, $_) } @groups;
    $? = 0;
}

sub write_app {
    my ($dir) = @_;
    my $file = File::Spec->catfile($dir, 'app.pl');
    open my $fh, '>', $file or die "Cannot write $file: $!";
    print $fh <<'APP';
use strict;
use warnings;
use Future::AsyncAwait;

my $app = async sub {
    my ($scope, $receive, $send) = @_;
    if ($scope->{type} eq 'lifespan') {
        while (1) {
            my $event = await $receive->();
            if ($event->{type} eq 'lifespan.startup') {
                await $send->({ type => 'lifespan.startup.complete' });
            } elsif ($event->{type} eq 'lifespan.shutdown') {
                await $send->({ type => 'lifespan.shutdown.complete' });
                last;
            }
        }
        return;
    }
    my $body = "pid=$$";
    await $send->({ type => 'http.response.start', status => 200,
                    headers => [['content-type', 'text/plain'], ['content-length', length $body]] });
    await $send->({ type => 'http.response.body', body => $body, more => 0 });
};

$app;
APP
    close $fh;
    return $file;
}

sub free_port {
    my $probe = IO::Socket::INET->new(LocalAddr => '127.0.0.1', LocalPort => 0, Listen => 1, ReuseAddr => 1)
        or die "Cannot find a free port: $!";
    my $port = $probe->sockport;
    close $probe;
    return $port;
}

sub get {
    my ($port) = @_;
    my $client = IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $port, Timeout => 5)
        or return undef;
    print $client "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    my $response = do { local $/; <$client> };
    close $client;
    return undef unless defined $response && $response =~ /\r\n\r\n(.*)\z/s;
    return $1;
}

# Starts pagi-server with --workers 2 and --pid in its own process group;
# returns the master's pid, the pid file and the port once it's serving.
sub start_server {
    my ($dir, @extra) = @_;
    my $port = free_port();
    my $pid_file = File::Spec->catfile($dir, 'pagi.pid');
    my $log = File::Spec->catfile($dir, 'server.log');
    my $app = write_app($dir);
    my $pid = fork() // die "fork failed: $!";
    if ($pid == 0) {
        setpgrp(0, 0);
        open STDOUT, '>>', $log or POSIX::_exit(1);
        open STDERR, '>&STDOUT' or POSIX::_exit(1);
        exec($^X, '-Ilib', $bin, '--host', '127.0.0.1', '--port', $port, '--workers', 2,
            '--pid', $pid_file, '--heartbeat-timeout', 5, @extra,
            '--log-level', 'info', '--no-default-middleware', '--no-access-log', $app)
            or POSIX::_exit(1);
    }
    push @groups, $pid;
    my $serving = wait_for(sub { (get($port) // '') =~ /^pid=\d+/ }, 15);
    return ($pid, $pid_file, $port, $serving);
}

sub wait_for {
    my ($check, $timeout) = @_;
    my $deadline = time + $timeout;
    while (time < $deadline) {
        return 1 if $check->();
        select(undef, undef, undef, 0.25);
    }
    return 0;
}

sub workers_of { my ($master) = @_; return { map { $_ => 1 } split ' ', `pgrep -P $master` } }

sub pid_file_holds {
    my ($pid_file) = @_;
    open my $fh, '<', $pid_file or return undef;
    my $content = <$fh>;
    return undef unless defined $content;
    chomp $content;
    return $content;
}

sub stop_server {
    my ($pid) = @_;
    kill('TERM', $pid);
    my $exited = wait_for(sub { waitpid($pid, POSIX::WNOHANG()); !kill(0, $pid) }, 15);
    kill('KILL', -$pid) if kill(0, -$pid);
    return $exited;
}

sub server_log {
    my ($dir) = @_;
    open my $fh, '<', File::Spec->catfile($dir, 'server.log') or return '(no log)';
    local $/;
    return <$fh>;
}

subtest 'the master removes its pid file when it stops' => sub {
    my $dir = tempdir('stop');
    my ($master, $pid_file, $port, $serving) = start_server($dir);
    ok($serving, 'serving') or return diag(server_log($dir));
    is(pid_file_holds($pid_file), $master, 'the pid file names the master');
    ok(stop_server($master), 'the master stopped');
    ok(!-e $pid_file, 'and its pid file is gone');
};

subtest 'workers recycled by max_requests leave it' => sub {
    my $dir = tempdir('recycle');
    my ($master, $pid_file, $port, $serving) = start_server($dir, '--max-requests', 2);
    ok($serving, 'serving') or return diag(server_log($dir));
    my $before = workers_of($master);
    get($port) for 1 .. 10;
    ok(wait_for(sub { grep { !$before->{$_} } keys %{ workers_of($master) } }, 10),
        'a worker was recycled') or diag(server_log($dir));
    is(pid_file_holds($pid_file), $master, 'the pid file still names the master');
    stop_server($master);
};

subtest 'a SIGHUP rolling restart leaves it' => sub {
    my $dir = tempdir('hup');
    my ($master, $pid_file, $port, $serving) = start_server($dir);
    ok($serving, 'serving') or return diag(server_log($dir));
    my $before = workers_of($master);
    kill('HUP', $master);
    ok(wait_for(sub { my $now = workers_of($master); keys %$now == 2 && !grep { $before->{$_} } keys %$now }, 20),
        'every worker was replaced') or diag(server_log($dir));
    is(pid_file_holds($pid_file), $master, 'the pid file still names the master');
    stop_server($master);
};

subtest 'a SIGTTOU (one fewer worker) leaves it' => sub {
    my $dir = tempdir('ttou');
    my ($master, $pid_file, $port, $serving) = start_server($dir);
    ok($serving, 'serving') or return diag(server_log($dir));
    kill('TTOU', $master);
    ok(wait_for(sub { keys %{ workers_of($master) } == 1 }, 10), 'a worker ended') or diag(server_log($dir));
    is(pid_file_holds($pid_file), $master, 'the pid file still names the master');
    stop_server($master);
};

subtest 'after a hot restart it names the new master' => sub {
    skip_all 'hot restart requires RELEASE_TESTING' unless $ENV{RELEASE_TESTING};
    my $dir = tempdir('hot restart');
    my ($master, $pid_file, $port, $serving) = start_server($dir);
    ok($serving, 'serving') or return diag(server_log($dir));
    kill('USR2', $master);
    ok(wait_for(sub { waitpid($master, POSIX::WNOHANG()); !kill(0, $master) }, 30),
        'the old master handed over and exited') or return diag(server_log($dir));
    my $new = pid_file_holds($pid_file);
    ok($new && $new != $master && kill(0, $new), 'the pid file names a new, running master')
        or diag('pid file: ' . ($new // 'missing') . "\n" . server_log($dir));
    ok($new && keys %{ workers_of($new) }, 'whose workers are serving') if $new;
    kill('TERM', $new) if $new;            # the new master shares the old one's process group
};

done_testing;
