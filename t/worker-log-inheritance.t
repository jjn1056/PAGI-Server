use strict;
use warnings;
use Test2::V0;

# First, so Future stays pure-perl in the forked server (see PAGI::Server's
# Future::XS notice).
use PAGI::Server;

use Future::AsyncAwait;
use IO::Async::Loop;
use IO::Socket::UNIX;
use File::Temp qw(tempdir);
use POSIX qw(WNOHANG);
use Time::HiRes qw(sleep time);

# Each worker is a fresh PAGI::Server built from the master's settings. A sink
# or an access-log format that is not handed over silently stops applying in
# every worker, which is where all requests are served.

plan skip_all => 'Unix sockets and fork are not supported on Windows'
    if $^O eq 'MSWin32';
plan skip_all => 'Multi-worker tests require RELEASE_TESTING'
    unless $ENV{RELEASE_TESTING};

my $dir         = tempdir(CLEANUP => 1);
my $socket_path = "$dir/pagi.sock";
my $events_file = "$dir/events.log";
my $access_file = "$dir/access.log";
my $stderr_file = "$dir/stderr.log";

sub slurp {
    my ($path) = @_;
    open my $fh, '<', $path or return '';
    local $/;
    return scalar <$fh>;
}

# Declines lifespan, so each worker reports "lifespan not supported" through
# its own sink with the Worker N prefix -- a line only a worker emits.
my $app = async sub {
    my ($scope, $receive, $send) = @_;
    die "no lifespan here\n" if $scope->{type} eq 'lifespan';
    await $send->({
        type    => 'http.response.start',
        status  => 200,
        headers => [['content-type', 'text/plain']],
    });
    await $send->({ type => 'http.response.body', body => 'ok', more => 0 });
};

my $master = fork();
die "fork failed: $!" unless defined $master;
eval { POSIX::setpgid($master, $master) } if $master;

if ($master == 0) {
    eval { POSIX::setpgid(0, 0) };
    open STDERR, '>>', $stderr_file or die "stderr: $!";
    open STDOUT, '>&', \*STDERR     or die "stdout: $!";
    STDERR->autoflush(1);
    open my $access, '>>', $access_file or die "access: $!";
    $access->autoflush(1);

    my $loop   = IO::Async::Loop->new;
    my $server = PAGI::Server->new(
        app               => $app,
        socket            => $socket_path,
        workers           => 2,
        access_log        => $access,
        access_log_format => 'tiny',
        logger            => sub {
            my ($event) = @_;
            open my $fh, '>>', $events_file or return;
            print {$fh} "$event->{message}\n";
            close $fh;
        },
    );
    $loop->add($server);
    eval { $server->listen->get; $loop->run };
    exit 0;
}

# Parent: one request once a worker is accepting.
my $response = '';
my $deadline = time + 20;
while (time < $deadline) {
    if (my $client = IO::Socket::UNIX->new(Peer => $socket_path)) {
        print $client "GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
        $response .= $_ while <$client>;
        close $client;
        last;
    }
    sleep 0.1;
}
like($response, qr/\AHTTP\/1\.1 200/, 'a worker served the request');

# The access line is written after the response; wait for it.
$deadline = time + 10;
sleep 0.1 while time < $deadline && slurp($access_file) !~ /\n/;

kill 'TERM', $master;
my $reaped = 0;
$deadline = time + 20;
while (time < $deadline) {
    if (waitpid($master, WNOHANG) == $master) { $reaped = 1; last }
    sleep 0.1;
}
unless ($reaped) {
    kill 'KILL', -$master;
    waitpid($master, 0);
}
ok($reaped, 'the master shut down on SIGTERM');

my $events = slurp($events_file);
my $access = slurp($access_file);
my $stderr = slurp($stderr_file);

like($events, qr/^Worker \d+ \(\d+\): /m,
    "a worker's diagnostics reach the configured logger");
unlike($stderr, qr/^Worker \d+ \(\d+\): /m,
    'and none of them fall back to STDERR');
like($access, qr{^GET / 200 \d+ms$}m,
    'workers write the configured access-log format');
unlike($access, qr{\[\d\d/\w{3}/\d{4}:}, 'not the clf default');

done_testing;
