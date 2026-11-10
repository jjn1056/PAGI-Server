#!/usr/bin/env perl
use strict;
use warnings;
use Test2::V0;
use POSIX ();
use IO::Socket::INET;
use File::Spec;
use File::Copy qw(copy);
use Cwd qw(abs_path);
use Test::TempDir::Tiny;

BEGIN { require FindBin; }

plan skip_all => 'hot restart requires RELEASE_TESTING' unless $ENV{RELEASE_TESTING};
plan skip_all => 'Not supported on Windows' if $^O eq 'MSWin32';

# A hot restart whose new master fails to start (the redeployed application
# doesn't compile, or its startup fails) leaves the old master serving. The
# old master must notice and say so, keep its pid file, and accept the next
# SIGUSR2, so the operator can fix the deploy and try again.

# A re-executed master runs $^X $0 without -Ilib.
my $lib_dir = abs_path(File::Spec->catdir($FindBin::Bin, '..', 'lib'));
$ENV{PERL5LIB} = join(':', grep { defined } $lib_dir, $ENV{PERL5LIB});
my $bin = abs_path(File::Spec->catfile($FindBin::Bin, '..', 'bin', 'pagi-server'));

# Server shutdown signals its process group; each server gets its own.
$SIG{TERM} = 'IGNORE';

# By process group: after a hot restart the first master is gone and the
# new one, in the same group, may still be running.
my @groups;
END {
    kill('TERM', -$_) for grep { kill(0, -$_) } @groups;
    select(undef, undef, undef, 0.5) if @groups;
    kill('KILL', -$_) for grep { kill(0, -$_) } @groups;
    $? = 0;
}

my $GOOD = <<'APP';
use strict;
use warnings;
use Future::AsyncAwait;

my $app = async sub {
    my ($scope, $receive, $send) = @_;
    die "unsupported scope type: $scope->{type}\n" unless $scope->{type} eq 'http';
    my $body = "pid=$$";
    await $send->({ type => 'http.response.start', status => 200,
                    headers => [['content-type', 'text/plain'], ['content-length', length $body]] });
    await $send->({ type => 'http.response.body', body => $body, more => 0 });
};

$app;
APP

my %BROKEN = (
    'a redeploy that does not compile' => "use strict;\nthis is not perl (\n",
    'a redeploy whose startup fails'    => <<'APP',
use strict;
use warnings;
use Future::AsyncAwait;

my $app = async sub {
    my ($scope, $receive, $send) = @_;
    die "unsupported scope type: $scope->{type}\n" unless $scope->{type} eq 'lifespan';
    await $receive->();
    await $send->({ type => 'lifespan.startup.failed', message => 'database unreachable' });
};

$app;
APP
);

sub write_file {
    my ($file, $content) = @_;
    open my $fh, '>', $file or die "Cannot write $file: $!";
    print $fh $content;
    close $fh;
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

sub wait_for {
    my ($check, $timeout) = @_;
    my $deadline = time + $timeout;
    while (time < $deadline) {
        return 1 if $check->();
        select(undef, undef, undef, 0.25);
    }
    return 0;
}

sub gone { my ($pid) = @_; waitpid($pid, POSIX::WNOHANG()); return !kill(0, $pid) }

sub read_file {
    my ($file) = @_;
    open my $fh, '<', $file or return undef;
    local $/;
    my $content = <$fh>;
    return $content;
}

sub pid_file_holds { my $content = read_file($_[0]) // return undef; chomp $content; return $content }

for my $case (sort keys %BROKEN) {
    subtest $case => sub {
        my $dir = tempdir($case);
        my ($app, $pid_file, $log) = map { File::Spec->catfile($dir, $_) } qw(app.pl pagi.pid server.log);
        write_file($app, $GOOD);
        my $port = free_port();

        my $master = fork() // die "fork failed: $!";
        if ($master == 0) {
            setpgrp(0, 0);
            open STDOUT, '>>', $log or POSIX::_exit(1);
            open STDERR, '>&STDOUT' or POSIX::_exit(1);
            exec($^X, '-Ilib', $bin, '--host', '127.0.0.1', '--port', $port, '--workers', 2,
                '--pid', $pid_file, '--heartbeat-timeout', 5, '--log-level', 'info',
                '--no-default-middleware', '--no-access-log', $app)
                or POSIX::_exit(1);
        }
        push @groups, $master;
        ok(wait_for(sub { (get($port) // '') =~ /^pid=\d+/ }, 15), 'serving') or return diag(read_file($log));
        my %old_workers = map { $_ => 1 } split ' ', `pgrep -P $master`;

        write_file($app, $BROKEN{$case});
        kill('USR2', $master);
        ok(wait_for(sub { (read_file($log) // '') =~ /Hot restart failed/ }, 30),
            'the old master says the hot restart failed') or diag(read_file($log));
        ok(!gone($master), 'and keeps running');
        like(get($port), qr/^pid=\d+$/, 'and serving');
        is(pid_file_holds($pid_file), $master, 'its pid file still names it');

        write_file($app, $GOOD);
        kill('USR2', $master);
        ok(wait_for(sub { gone($master) }, 30), 'after the fix, another SIGUSR2 hot-restarts: the old master hands over')
            or return diag(read_file($log));
        my $new = pid_file_holds($pid_file);
        ok($new && $new != $master && kill(0, $new), 'the pid file names the new master') or diag(read_file($log));
        my ($pid) = (get($port) // '') =~ /^pid=(\d+)/;
        ok($pid && !$old_workers{$pid}, 'which serves from new workers');
    };
}

done_testing;
