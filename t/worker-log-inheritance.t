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
use JSON::MaybeXS ();
use POSIX qw(WNOHANG);
use Time::HiRes qw(sleep time);

# Each worker is a fresh PAGI::Server built from the master's settings. A sink,
# an access-log format or a log format that is not handed over silently stops
# applying in every worker, which is where all requests are served.

plan skip_all => 'Unix sockets and fork are not supported on Windows'
    if $^O eq 'MSWin32';
plan skip_all => 'Multi-worker tests require RELEASE_TESTING'
    unless $ENV{RELEASE_TESTING};

sub slurp {
    my ($path) = @_;
    open my $fh, '<', $path or return '';
    local $/;
    return scalar <$fh>;
}

# Declines lifespan, so each worker reports "lifespan not supported" through
# its own sink -- a message only a worker emits.
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

# Runs a two-worker master with %server_args, makes one request, shuts it
# down, and returns what it wrote: the response, STDERR, the access log, and
# the events file a test's logger may append to.
sub run_workers {
    my (%server_args) = @_;
    my $dir         = tempdir(CLEANUP => 1);
    my $socket_path = "$dir/pagi.sock";
    my %file = map { $_ => "$dir/$_.log" } qw(events access stderr);

    my $master = fork();
    die "fork failed: $!" unless defined $master;
    eval { POSIX::setpgid($master, $master) } if $master;

    if ($master == 0) {
        eval { POSIX::setpgid(0, 0) };
        open STDERR, '>>', $file{stderr} or die "stderr: $!";
        open STDOUT, '>&', \*STDERR     or die "stdout: $!";
        STDERR->autoflush(1);
        open my $access, '>>', $file{access} or die "access: $!";
        $access->autoflush(1);

        my $loop   = IO::Async::Loop->new;
        my $server = PAGI::Server->new(
            app        => $app,
            socket     => $socket_path,
            workers    => 2,
            access_log => $access,
            map { ref $server_args{$_} eq 'CODE' && $_ eq 'logger'
                      ? ($_ => $server_args{$_}->($file{events}))
                      : ($_ => $server_args{$_}) } keys %server_args,
        );
        $loop->add($server);
        eval { $server->listen->get; $loop->run };
        exit 0;
    }

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

    # The access line is written after the response; wait for it.
    $deadline = time + 10;
    sleep 0.1 while time < $deadline && slurp($file{access}) !~ /\n/;

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

    return {
        response => $response,
        map { $_ => slurp($file{$_}) } qw(events access stderr),
    };
}

subtest 'workers use the configured logger and access-log format' => sub {
    my $out = run_workers(
        access_log_format => 'tiny',
        # The coderef is inherited through fork, so a worker's events land in
        # the same file as the master's.
        logger => sub {
            my ($events_file) = @_;
            return sub {
                my ($event) = @_;
                open my $fh, '>>', $events_file or return;
                print {$fh} "$event->{message}\n";
                close $fh;
            };
        },
    );

    like($out->{response}, qr/\AHTTP\/1\.1 200/, 'a worker served the request');
    like($out->{events}, qr/^Worker \d+ \(\d+\): /m,
        "a worker's diagnostics reach the configured logger");
    unlike($out->{stderr}, qr/^Worker \d+ \(\d+\): /m,
        'and none of them fall back to STDERR');
    like($out->{access}, qr{^GET / 200 \d+ms$}m,
        'workers write the configured access-log format');
    unlike($out->{access}, qr{\[\d\d/\w{3}/\d{4}:}, 'not the clf default');
};

subtest 'workers write JSON when the master is configured for it' => sub {
    my $decoder = JSON::MaybeXS->new(utf8 => 1);
    my $out = run_workers(log_format => 'json');

    like($out->{response}, qr/\AHTTP\/1\.1 200/, 'a worker served the request');

    my @lines  = grep { length } split /\n/, $out->{stderr};
    my @events = map { my $e = eval { $decoder->decode($_) }; $e ? $e : () } @lines;
    is(scalar @events, scalar @lines, 'every STDERR line is JSON, workers included')
        or diag(join "\n", @lines);

    my @from_workers = grep { defined $_->{worker} } @events;
    ok(scalar @from_workers, 'workers log with a worker field');
    is([grep { $_->{message} =~ /\AWorker \d+ \(/ } @from_workers], [],
        'and no Worker prefix in the message');

    my @banner = grep { $_->{message} =~ /listening on/ } @events;
    is(scalar @banner, 1, "the master's banner is one event");
    ok($banner[0]{notes}, 'with its notes as fields');

    my ($access) = grep { length } split /\n/, $out->{access};
    my $record = eval { $decoder->decode($access // '') };
    ok($record, 'the access log defaults to JSON in the worker') or diag($access);
    is($record->{status}, 200, 'for the real request');
    like($record->{worker}, qr/\A\d+\z/, 'naming the worker that served it');
};

done_testing;
