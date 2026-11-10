#!/usr/bin/env perl
use strict;
use warnings;
use Test2::V0;
use POSIX ();
use IO::Socket::INET;
use IO::Socket::UNIX;
use File::Spec;
use Cwd qw(abs_path);
use Test::TempDir::Tiny;

BEGIN { require FindBin; }

plan skip_all => 'Not supported on Windows' if $^O eq 'MSWin32';

# A process an application starts (a report, a converter, a slow command)
# must not inherit the server's listening socket. If it does, it keeps the
# address bound after the server stops: clients hang instead of being
# refused, and the server can't be started again on that address until the
# child exits. Each case starts the real pagi-server, has the application
# start a child the way an application would, and asks the child what it
# inherited.

# A re-executed master (hot restart) runs $^X $0 without -Ilib.
my $lib_dir = abs_path(File::Spec->catdir($FindBin::Bin, '..', 'lib'));
$ENV{PERL5LIB} = join(':', grep { defined } $lib_dir, $ENV{PERL5LIB});
my $bin = abs_path(File::Spec->catfile($FindBin::Bin, '..', 'bin', 'pagi-server'));

# Server shutdown signals its process group; each server gets its own.
$SIG{TERM} = 'IGNORE';

my @groups;
# By process group: after a hot restart the first master is gone and the
# new one, in the same group, may still be running.
END {
    kill('TERM', -$_) for grep { kill(0, -$_) } @groups;
    select(undef, undef, undef, 0.5) if @groups;
    kill('KILL', -$_) for grep { kill(0, -$_) } @groups;
    $? = 0;
}

# The application: /child starts a child that reports which of its
# descriptors are the server's socket, a listening one or an accepted
# connection (one with a peer), matched by port (TCP) or path (Unix). Every response names
# the process that answered.
my $CHILD = <<'CHILD';
use strict;
use warnings;
use Socket qw(AF_INET AF_UNIX sockaddr_family unpack_sockaddr_in unpack_sockaddr_un);
my ($port, $path) = @ARGV;
my (@listening, @connected);
for my $fd (3 .. 1023) {
    open(my $h, '+<&=', $fd) or next;
    my $addr = getsockname($h) or next;
    my $family = sockaddr_family($addr);
    my $ours = $family == AF_INET ? (length $port && (unpack_sockaddr_in($addr))[0] == $port)
             : $family == AF_UNIX ? (length $path && unpack_sockaddr_un($addr) eq $path)
             : 0;
    next unless $ours;
    if (getpeername($h)) { push @connected, $fd } else { push @listening, $fd }
}
print 'listening=' . join(',', @listening) . ' connected=' . join(',', @connected);
CHILD

sub write_app {
    my ($dir) = @_;
    my $file = File::Spec->catfile($dir, 'app.pl');
    open my $fh, '>', $file or die "Cannot write $file: $!";
    printf $fh <<'APP', $CHILD;
use strict;
use warnings;
use Future::AsyncAwait;

my $CHILD = <<'SOURCE';
%sSOURCE

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
    if ($scope->{path} eq '/child') {
        open(my $fh, '-|', $^X, '-e', $CHILD, $ENV{PROBE_PORT} // '', $ENV{PROBE_PATH} // '')
            or die "cannot start the child: $!\n";
        my $report = do { local $/; <$fh> };
        close $fh;
        $body .= " $report";
    }
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

# Starts pagi-server in its own process group, with %env set and the code
# in $before run in the forked child just before exec. Returns its pid.
sub start_server {
    my (%args) = @_;
    my $log = File::Spec->catfile($args{dir}, 'server.log');
    my $pid = fork() // die "fork failed: $!";
    if ($pid == 0) {
        setpgrp(0, 0);
        open STDOUT, '>>', $log or POSIX::_exit(1);
        open STDERR, '>&STDOUT' or POSIX::_exit(1);
        $ENV{$_} = $args{env}{$_} for keys %{ $args{env} || {} };
        $args{before}->() if $args{before};
        exec($^X, '-Ilib', $bin, @{ $args{argv} },
            '--log-level', 'info', '--no-default-middleware', '--no-access-log', $args{app})
            or POSIX::_exit(1);
    }
    push @groups, $pid;
    return $pid;
}

sub connect_to {
    my ($where) = @_;
    return $where->{path}
        ? IO::Socket::UNIX->new(Peer => $where->{path}, Type => Socket::SOCK_STREAM())
        : IO::Socket::INET->new(PeerAddr => '127.0.0.1', PeerPort => $where->{port}, Timeout => 5);
}

sub get {
    my ($where, $path) = @_;
    my $client = connect_to($where) or return undef;
    print $client "GET $path HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    my $response = do { local $/; <$client> };
    close $client;
    return undef unless defined $response && $response =~ /\r\n\r\n(.*)\z/s;
    return $1;
}

sub wait_until_serving {
    my ($where, $timeout) = @_;
    my $deadline = time + ($timeout // 15);
    while (time < $deadline) {
        my $body = get($where, '/');
        return 1 if defined $body && $body =~ /^pid=\d+/;
        select(undef, undef, undef, 0.25);
    }
    return 0;
}

sub stop_server {
    my ($pid) = @_;
    kill('TERM', -$pid);
    my $deadline = time + 15;
    while (time < $deadline) {
        last unless kill(0, -$pid);
        waitpid($pid, POSIX::WNOHANG());
        select(undef, undef, undef, 0.25);
    }
    kill('KILL', -$pid) if kill(0, -$pid);
    waitpid($pid, 0);
}

sub server_log {
    my ($dir) = @_;
    open my $fh, '<', File::Spec->catfile($dir, 'server.log') or return '(no log)';
    local $/;
    return <$fh>;
}

# Asks a running server's application to start a child; checks that the
# child holds neither the listening socket nor a connection.
sub child_inherits_nothing {
    my ($where, $dir, $label) = @_;
    my $body = get($where, '/child');
    like($body, qr/^pid=\d+ listening=\S* connected=\S*$/, "$label: the child reported")
        or return diag(server_log($dir));
    like($body, qr/ listening= /, "$label: the child doesn't hold the listening socket")
        or diag($body);
    like($body, qr/ connected=$/, "$label: nor the client's connection")
        or diag($body);
    return $body;
}

my %MODE = (
    'single process'   => [],
    'workers (shared)' => ['--workers', 2, '--heartbeat-timeout', 5],
);

for my $mode (sort keys %MODE) {
    subtest "$mode, TCP" => sub {
        my $dir = tempdir("tcp $mode");
        my $port = free_port();
        my $server = start_server(dir => $dir, app => write_app($dir), env => { PROBE_PORT => $port },
            argv => ['--host', '127.0.0.1', '--port', $port, @{ $MODE{$mode} }]);
        ok(wait_until_serving({ port => $port }), 'serving') or return diag(server_log($dir));
        child_inherits_nothing({ port => $port }, $dir, $mode);
        stop_server($server);
    };

    subtest "$mode, Unix socket" => sub {
        my $dir = tempdir("unix $mode");
        # Relative: a Unix socket path is limited to about 104 bytes.
        my $path = File::Spec->abs2rel(File::Spec->catfile($dir, 's.sock'));
        my $server = start_server(dir => $dir, app => write_app($dir), env => { PROBE_PATH => $path },
            argv => ['--socket', $path, @{ $MODE{$mode} }]);
        ok(wait_until_serving({ path => $path }), 'serving') or return diag(server_log($dir));
        child_inherits_nothing({ path => $path }, $dir, $mode);
        stop_server($server);
    };
}

# A socket handed to the server arrives without close-on-exec: the server
# must set it before an application's child can inherit it.
subtest 'a socket adopted from PAGI_REUSE' => sub {
    my $dir = tempdir('adopted reuse');
    my $socket = do {
        local $^F = 1023;                  # as a previous master leaves it: inheritable
        IO::Socket::INET->new(LocalAddr => '127.0.0.1', LocalPort => 0, Listen => 128, ReuseAddr => 1)
            or die "Cannot listen: $!";
    };
    my $port = $socket->sockport;
    my $server = start_server(dir => $dir, app => write_app($dir),
        env  => { PROBE_PORT => $port, PAGI_REUSE => "127.0.0.1:$port:" . fileno($socket) },
        argv => ['--host', '127.0.0.1', '--port', $port]);
    close $socket;                         # the server holds it now
    ok(wait_until_serving({ port => $port }), 'serving on the adopted socket') or return diag(server_log($dir));
    like(server_log($dir), qr/Reusing inherited fd/, 'it adopted the socket rather than binding a new one')
        or diag(server_log($dir));
    child_inherits_nothing({ port => $port }, $dir, 'adopted from PAGI_REUSE');
    stop_server($server);
};

subtest 'a socket adopted from systemd (LISTEN_FDS)' => sub {
    my $dir = tempdir('adopted systemd');
    my $socket = IO::Socket::INET->new(LocalAddr => '127.0.0.1', LocalPort => 0, Listen => 128, ReuseAddr => 1)
        or die "Cannot listen: $!";
    my $port = $socket->sockport;
    my $server = start_server(dir => $dir, app => write_app($dir),
        env    => { PROBE_PORT => $port, LISTEN_FDS => 1 },
        before => sub {                    # as systemd hands it over: fd 3, inheritable
            POSIX::dup2(fileno($socket), 3) // POSIX::_exit(1);
            $ENV{LISTEN_PID} = $$;
        },
        argv => ['--host', '127.0.0.1', '--port', $port]);
    close $socket;
    ok(wait_until_serving({ port => $port }), 'serving on the adopted socket') or return diag(server_log($dir));
    like(server_log($dir), qr/Reusing inherited fd 3/, 'it adopted fd 3 rather than binding a new one')
        or diag(server_log($dir));
    child_inherits_nothing({ port => $port }, $dir, 'adopted from systemd');
    stop_server($server);
};

# Hot restart hands the sockets to a new master across exec; once it has
# them, its application's children still mustn't inherit them.
subtest 'after a hot restart' => sub {
    skip_all 'hot restart requires RELEASE_TESTING' unless $ENV{RELEASE_TESTING};
    my $dir = tempdir('hot restart');
    my $port = free_port();
    my $master = start_server(dir => $dir, app => write_app($dir), env => { PROBE_PORT => $port },
        argv => ['--host', '127.0.0.1', '--port', $port, '--workers', 2, '--heartbeat-timeout', 5]);
    ok(wait_until_serving({ port => $port }), 'serving') or return diag(server_log($dir));
    my %old_workers = map { $_ => 1 } split ' ', `pgrep -P $master`;
    child_inherits_nothing({ port => $port }, $dir, 'before the restart');

    kill('USR2', $master);
    my $deadline = time + 30;
    while (time < $deadline && kill(0, $master)) {
        waitpid($master, POSIX::WNOHANG());
        select(undef, undef, undef, 0.25);
    }
    ok(!kill(0, $master), 'the old master handed over and exited') or return diag(server_log($dir));

    my $body = child_inherits_nothing({ port => $port }, $dir, 'the new master');
    my ($pid) = ($body // '') =~ /^pid=(\d+)/;
    ok($pid && !$old_workers{$pid}, 'answered by a new worker, on the handed-over socket') or diag($body);
    stop_server($master);                  # the new master shares the old one's process group
};

# A master that adopted its socket (one a hot restart started, or one given
# PAGI_REUSE by whatever launched it) holds it close-on-exec, since Perl sets
# that when it adopts a descriptor. Its own hot restart must still hand the
# socket over, or the new master takes over with nothing to listen on.
subtest 'a hot restart of a master that adopted its socket' => sub {
    skip_all 'hot restart requires RELEASE_TESTING' unless $ENV{RELEASE_TESTING};
    my $dir = tempdir('adopted hot restart');
    my $port = free_port();
    my $master = start_server(dir => $dir, app => write_app($dir), env => { PROBE_PORT => $port },
        before => sub {                    # as a previous master leaves it: inheritable, in PAGI_REUSE
            my $socket = do {
                local $^F = 1023;
                IO::Socket::INET->new(LocalAddr => '127.0.0.1', LocalPort => $port, Listen => 128, ReuseAddr => 1)
                    or POSIX::_exit(1);
            };
            $ENV{PAGI_REUSE} = "127.0.0.1:$port:" . fileno($socket);
            our $KEEP = $socket;           # open until exec
        },
        argv => ['--host', '127.0.0.1', '--port', $port, '--workers', 2, '--heartbeat-timeout', 5]);
    ok(wait_until_serving({ port => $port }), 'serving') or return diag(server_log($dir));
    like(server_log($dir), qr/Reusing inherited fd/, 'it adopted the socket') or diag(server_log($dir));
    my %old_workers = map { $_ => 1 } split ' ', `pgrep -P $master`;

    kill('USR2', $master);
    my $deadline = time + 30;
    while (time < $deadline && kill(0, $master)) {
        waitpid($master, POSIX::WNOHANG());
        select(undef, undef, undef, 0.25);
    }
    ok(!kill(0, $master), 'the old master handed over and exited') or return diag(server_log($dir));

    my $body = get({ port => $port }, '/');
    my ($pid) = ($body // '') =~ /^pid=(\d+)/;
    ok($pid && !$old_workers{$pid}, 'still serving, from a new worker') or diag(server_log($dir));
    child_inherits_nothing({ port => $port }, $dir, 'the new master') if $pid;
    stop_server($master);
};

done_testing;
